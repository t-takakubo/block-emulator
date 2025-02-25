package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

// This module used in the blockChain using transaction relaying mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
type CLPAContractOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
	cfcpm    *dataSupport.CrossFunctionCallPoolManager
}

func (ccom *CLPAContractOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		ccom.handleRelay(content)
	case message.CInject:
		ccom.handleInjectTx(content)

	// messages about CLPA
	case message.CPartitionMsg:
		ccom.handlePartitionMsg(content)
	case message.AccountState_and_TX:
		ccom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		ccom.handlePartitionReady(content)

	// for smart contract
	case message.CContractInject:
		ccom.handleContractInject(content)
	case message.CContractRequest:
		ccom.cfcpm.HandleContractRequest(content)
	case message.CContactResponse:
		ccom.cfcpm.HandleContractResponse(content)
	default:
	}
	return true
}

// receive relay transaction, which is for cross shard txs
func (ccom *CLPAContractOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	ccom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay %d txs from shard %d, the senderSeq is %d\n", ccom.pbftNode.ShardID, ccom.pbftNode.NodeID, len(relay.Txs), relay.SenderShardID, relay.SenderSeq)
	ccom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	ccom.pbftNode.seqMapLock.Lock()
	ccom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	ccom.pbftNode.seqMapLock.Unlock()
	ccom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", ccom.pbftNode.ShardID, ccom.pbftNode.NodeID)
}

func (ccom *CLPAContractOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	ccom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	ccom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", ccom.pbftNode.ShardID, ccom.pbftNode.NodeID, len(it.Txs))
}

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (ccom *CLPAContractOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}

	ccom.cdm.ModifiedMap = append(ccom.cdm.ModifiedMap, pm.PartitionModified)
	ccom.cdm.MergedContracts = append(ccom.cdm.MergedContracts, pm.MergedContracts)
	ccom.cdm.ReversedMergedContracts = append(ccom.cdm.ReversedMergedContracts, ReverseMap(pm.MergedContracts))
	ccom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message\n", ccom.pbftNode.ShardID, ccom.pbftNode.NodeID)
	ccom.cdm.PartitionOn = true
}

// wait for other shards' last rounds are over
func (ccom *CLPAContractOutsideModule) handlePartitionReady(content []byte) {
	pr := new(message.PartitionReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	ccom.cdm.P_ReadyLock.Lock()
	ccom.cdm.PartitionReady[pr.FromShard] = true
	ccom.cdm.P_ReadyLock.Unlock()

	ccom.pbftNode.seqMapLock.Lock()
	ccom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	ccom.pbftNode.seqMapLock.Unlock()

	ccom.pbftNode.pl.Plog.Printf("ready message from shard %d, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

// when the message from other shard arriving, it should be added into the message pool
func (ccom *CLPAContractOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic(err)
	}

	ccom.cdm.AccountStateTxLock.Lock()
	ccom.cdm.AccountStateTx[at.FromShard] = at
	ccom.cdm.AccountStateTxLock.Unlock()

	ccom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", ccom.pbftNode.ShardID, ccom.pbftNode.NodeID, at.FromShard)

	if len(ccom.cdm.AccountStateTx) == int(ccom.pbftNode.pbftChainConfig.ShardNums)-1 {
		ccom.cdm.CollectLock.Lock()
		ccom.cdm.CollectOver = true
		ccom.cdm.CollectLock.Unlock()
		ccom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", ccom.pbftNode.ShardID, ccom.pbftNode.NodeID)
	}
}

func (ccom *CLPAContractOutsideModule) handleContractInject(content []byte) {
	ci := new(message.ContractInjectTxs)
	err := json.Unmarshal(content, ci)
	if err != nil {
		log.Panic("unmarshal error", err)
	}

	requestsByShard, innerTxList := ccom.processContractInject(ci.Txs)

	ccom.sendRequests(requestsByShard)

	if len(innerTxList) > 0 {
		ccom.pbftNode.CurChain.Txpool.AddTxs2Front(innerTxList)
	}
}

func (ccom *CLPAContractOutsideModule) processContractInject(txs []*core.Transaction) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	innerTxList := make([]*core.Transaction, 0)

	for _, tx := range txs {
		root := core.BuildExecutionCallTree(tx, make(map[string]bool))

		differentShardNode, hasDiffShard, processedMap := ccom.DFS(root, true)

		if hasDiffShard {
			destShardID := ccom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)

			request := &message.CrossShardFunctionRequest{
				RequestID:          string(tx.TxHash),
				OriginalSender:     tx.Sender,
				SourceShardID:      ccom.pbftNode.ShardID,
				DestinationShardID: destShardID,
				Sender:             differentShardNode.Sender,
				Recepient:          differentShardNode.Recipient,
				Value:              differentShardNode.Value,
				Arguments:          []byte(""),
				Timestamp:          time.Now().Unix(),
				TypeTraceAddress:   differentShardNode.TypeTraceAddress,
				Tx:                 tx,
				ProcessedMap:       processedMap,
			}
			requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
		} else {
			tx.IsAllInner = true
			tx.DivisionCount = 1
			tx.StateChangeAccounts[tx.Sender] = &core.Account{AcAddress: tx.Sender, IsContract: false}
			tx.StateChangeAccounts[tx.Recipient] = &core.Account{AcAddress: tx.Recipient, IsContract: true}
			for _, itx := range tx.InternalTxs {
				switch itx.CallType {
				case "call", "create":
					tx.StateChangeAccounts[itx.Recipient] = &core.Account{AcAddress: itx.Recipient, IsContract: itx.RecipientIsContract}
				case "delegatecall":
					tx.StateChangeAccounts[itx.Sender] = &core.Account{AcAddress: itx.Sender, IsContract: itx.SenderIsContract}
				}
			}
			innerTxList = append(innerTxList, tx)
		}
	}
	return requestsByShard, innerTxList
}

func (ccom *CLPAContractOutsideModule) processBatchRequests(requests []*message.CrossShardFunctionRequest) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	responseByShard := make(map[uint64][]*message.CrossShardFunctionResponse)
	skipCount := 0

	for _, req := range requests {
		root := core.BuildExecutionCallTree(req.Tx, req.ProcessedMap)
		currentCallNode := root.FindNodeByTTA(req.TypeTraceAddress)

		if currentCallNode == nil {
			fmt.Println(req.TypeTraceAddress)
			log.Panic("currentCallNode is nil")
		}

		if currentCallNode.IsLeaf {
			randomAccountInSC := GenerateRandomString(params.AccountNumInContract)
			isAccountLocked := ccom.cfcpm.SClock.IsAccountLocked(currentCallNode.Recipient, randomAccountInSC, req.RequestID)
			if isAccountLocked {
				skipCount++
				ccom.cfcpm.AddRequest(req)
				continue
			}

			switch currentCallNode.CallType {
			case "call", "create":
				err := ccom.cfcpm.SClock.LockAccount(currentCallNode.Recipient, randomAccountInSC, req.RequestID)
				if err != nil {
					fmt.Println(err)
				}
			case "delegatecall":
				err := ccom.cfcpm.SClock.LockAccount(currentCallNode.Sender, randomAccountInSC, req.RequestID)
				if err != nil {
					fmt.Println(err)
				}
			}

			destShardID := req.SourceShardID
			req.ProcessedMap[currentCallNode.TypeTraceAddress] = true

			response := &message.CrossShardFunctionResponse{
				ResponseID:         req.RequestID,
				OriginalSender:     req.OriginalSender,
				SourceShardID:      ccom.pbftNode.ShardID,
				DestinationShardID: destShardID,
				Sender:             currentCallNode.Sender,
				Recipient:          currentCallNode.Recipient,
				Value:              currentCallNode.Value,
				ResultData:         []byte(""),
				Timestamp:          time.Now().Unix(),
				TypeTraceAddress:   currentCallNode.TypeTraceAddress,
				Tx:                 req.Tx,
				ProcessedMap:       req.ProcessedMap,
			}
			responseByShard[destShardID] = append(responseByShard[destShardID], response)
		} else {
			differentShardNode, hasDiffShard, processedMap := ccom.DFS(currentCallNode, false)
			for k, v := range processedMap {
				req.ProcessedMap[k] = v
			}

			if hasDiffShard {
				destShardID := ccom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)

				request := &message.CrossShardFunctionRequest{
					RequestID:          req.RequestID,
					OriginalSender:     req.OriginalSender,
					SourceShardID:      ccom.pbftNode.ShardID,
					DestinationShardID: destShardID,
					Sender:             differentShardNode.Sender,
					Recepient:          differentShardNode.Recipient,
					Value:              differentShardNode.Value,
					Arguments:          []byte(""),
					Timestamp:          time.Now().Unix(),
					TypeTraceAddress:   differentShardNode.TypeTraceAddress,
					Tx:                 req.Tx,
					ProcessedMap:       req.ProcessedMap,
				}
				requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
			} else {
				req.ProcessedMap[currentCallNode.TypeTraceAddress] = true
				destShardID := req.SourceShardID

				response := &message.CrossShardFunctionResponse{
					ResponseID:         req.RequestID,
					OriginalSender:     req.OriginalSender,
					SourceShardID:      ccom.pbftNode.ShardID,
					DestinationShardID: destShardID,
					Sender:             currentCallNode.Sender,
					Recipient:          currentCallNode.Recipient,
					Value:              currentCallNode.Value,
					ResultData:         []byte(""),
					Timestamp:          time.Now().Unix(),
					TypeTraceAddress:   currentCallNode.TypeTraceAddress,
					Tx:                 req.Tx,
					ProcessedMap:       req.ProcessedMap,
				}
				responseByShard[destShardID] = append(responseByShard[destShardID], response)
			}
		}
	}

	ccom.sendRequests(requestsByShard)
	ccom.sendResponses(responseByShard)
}

func (ccom *CLPAContractOutsideModule) processBatchResponses(responses []*message.CrossShardFunctionResponse) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	responseByShard := make(map[uint64][]*message.CrossShardFunctionResponse)
	injectTxByShard := make(map[uint64][]*core.Transaction)

	for _, res := range responses {
		root := core.BuildExecutionCallTree(res.Tx, res.ProcessedMap)
		currentCallNode := root.FindParentNodeByTTA(res.TypeTraceAddress)

		if currentCallNode == nil {
			fmt.Println(res.TypeTraceAddress)
			log.Panic("currentCallNode is nil")
		}

		differentShardNode, hasDiffShard, processedMap := ccom.DFS(currentCallNode, false)
		for k, v := range processedMap {
			res.ProcessedMap[k] = v
		}

		if !hasDiffShard {
			destShardID := ccom.pbftNode.CurChain.Get_PartitionMap(currentCallNode.Sender)
			res.ProcessedMap[currentCallNode.TypeTraceAddress] = true

			if currentCallNode.CallType == "root" {
				if destShardID != ccom.pbftNode.ShardID {
					response := &message.CrossShardFunctionResponse{
						ResponseID:         res.ResponseID,
						OriginalSender:     res.OriginalSender,
						SourceShardID:      ccom.pbftNode.ShardID,
						DestinationShardID: destShardID,
						Sender:             currentCallNode.Sender,
						Recipient:          currentCallNode.Recipient,
						Value:              currentCallNode.Value,
						ResultData:         []byte(""),
						Timestamp:          time.Now().Unix(),
						TypeTraceAddress:   currentCallNode.TypeTraceAddress,
						Tx:                 res.Tx,
						ProcessedMap:       res.ProcessedMap,
					}
					responseByShard[destShardID] = append(responseByShard[destShardID], response)
				} else {
					stateChangeAccountByShard := make(map[uint64][]*core.Account)
					ssid := ccom.pbftNode.CurChain.Get_PartitionMap(res.Tx.Sender)
					rsid := ccom.pbftNode.CurChain.Get_PartitionMap(res.Tx.Recipient)

					if stateChangeAccountByShard[ssid] == nil {
						stateChangeAccountByShard[ssid] = make([]*core.Account, 0)
					}
					stateChangeAccountByShard[ssid] = append(stateChangeAccountByShard[ssid], &core.Account{AcAddress: res.Tx.Sender, IsContract: false})

					if stateChangeAccountByShard[rsid] == nil {
						stateChangeAccountByShard[rsid] = make([]*core.Account, 0)
					}
					stateChangeAccountByShard[rsid] = append(stateChangeAccountByShard[rsid], &core.Account{AcAddress: res.Tx.Recipient, IsContract: true})

					for _, itx := range res.Tx.InternalTxs {
						switch itx.CallType {
						case "call", "create":
							rsid = ccom.pbftNode.CurChain.Get_PartitionMap(itx.Recipient)
							if stateChangeAccountByShard[rsid] == nil {
								stateChangeAccountByShard[rsid] = make([]*core.Account, 0)
							}
							stateChangeAccountByShard[rsid] = append(stateChangeAccountByShard[rsid], &core.Account{AcAddress: itx.Recipient, IsContract: itx.RecipientIsContract})
						case "delegatecall":
							ssid = ccom.pbftNode.CurChain.Get_PartitionMap(itx.Sender)
							if stateChangeAccountByShard[ssid] == nil {
								stateChangeAccountByShard[ssid] = make([]*core.Account, 0)
							}
							stateChangeAccountByShard[ssid] = append(stateChangeAccountByShard[ssid], &core.Account{AcAddress: itx.Sender, IsContract: itx.SenderIsContract})
						}
					}

					for shardID, accounts := range stateChangeAccountByShard {
						tx := core.NewTransaction(res.OriginalSender, res.Tx.Recipient, res.Tx.Value, 0, res.Tx.Time)
						tx.TxHash = res.Tx.TxHash
						tx.HasContract = true
						tx.RequestID = res.ResponseID
						tx.DivisionCount = len(stateChangeAccountByShard)

						if len(stateChangeAccountByShard) == 1 {
							ccom.cfcpm.SClock.UnlockAllByRequestID(tx.RequestID)
							tx.InternalTxs = res.Tx.InternalTxs
							tx.IsAllInner = true
						} else {
							tx.InternalTxs = res.Tx.InternalTxs
							tx.IsExecuteCLPA = true

							tx.IsCrossShardFuncCall = true
						}

						scAddrCount := 0
						acAddrCount := 0
						for _, account := range accounts {
							tx.StateChangeAccounts[account.AcAddress] = account
							if account.IsContract {
								scAddrCount++
							} else {
								acAddrCount++
							}
						}
						injectTxByShard[shardID] = append(injectTxByShard[shardID], tx)
					}
				}
			} else {
				response := &message.CrossShardFunctionResponse{
					ResponseID:         res.ResponseID,
					OriginalSender:     res.OriginalSender,
					SourceShardID:      ccom.pbftNode.ShardID,
					DestinationShardID: destShardID,
					Sender:             currentCallNode.Sender,
					Recipient:          currentCallNode.Recipient,
					Value:              currentCallNode.Value,
					ResultData:         []byte(""),
					Timestamp:          time.Now().Unix(),
					TypeTraceAddress:   currentCallNode.TypeTraceAddress,
					Tx:                 res.Tx,
					ProcessedMap:       res.ProcessedMap,
				}
				responseByShard[destShardID] = append(responseByShard[destShardID], response)
			}
		} else {
			destShardID := ccom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
			request := &message.CrossShardFunctionRequest{
				RequestID:          res.ResponseID,
				OriginalSender:     res.OriginalSender,
				SourceShardID:      ccom.pbftNode.ShardID,
				DestinationShardID: destShardID,
				Sender:             differentShardNode.Sender,
				Recepient:          differentShardNode.Recipient,
				Value:              differentShardNode.Value,
				Arguments:          []byte(""),
				Timestamp:          time.Now().Unix(),
				TypeTraceAddress:   differentShardNode.TypeTraceAddress,
				Tx:                 res.Tx,
				ProcessedMap:       res.ProcessedMap,
			}

			requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
		}
	}
	ccom.sendRequests(requestsByShard)
	ccom.sendResponses(responseByShard)
	ccom.sendInjectTransactions(injectTxByShard)
}

func (ccom *CLPAContractOutsideModule) sendRequests(requestsByShard map[uint64][]*message.CrossShardFunctionRequest) {
	for sid, requests := range requestsByShard {
		rByte, err := json.Marshal(requests)
		if err != nil {
			log.Panic("unmarshal error", err)
		}
		msg_send := message.MergeMessage(message.CContractRequest, rByte)
		go networks.TcpDial(msg_send, ccom.pbftNode.ip_nodeTable[sid][0])
	}
}

func (ccom *CLPAContractOutsideModule) sendResponses(responsesByShard map[uint64][]*message.CrossShardFunctionResponse) {
	for sid, responses := range responsesByShard {
		rByte, err := json.Marshal(responses)
		if err != nil {
			log.Panic("unmarshal error", err)
		}
		msg_send := message.MergeMessage(message.CContactResponse, rByte)
		go networks.TcpDial(msg_send, ccom.pbftNode.ip_nodeTable[sid][0])
	}
}

func (ccom *CLPAContractOutsideModule) sendInjectTransactions(sendToShard map[uint64][]*core.Transaction) {
	for sid, txs := range sendToShard {
		it := message.InjectTxs{
			Txs:       txs,
			ToShardID: sid,
		}
		itByte, err := json.Marshal(it)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CInject, itByte)
		go networks.TcpDial(sendMsg, ccom.pbftNode.ip_nodeTable[sid][0])
	}
}

func (ccom *CLPAContractOutsideModule) StartBatchProcessing(batchSize int, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	exportTicker := time.NewTicker(time.Duration(params.Block_Interval) * time.Millisecond)
	defer exportTicker.Stop()

	partitionLogTicker := time.NewTicker(10 * time.Second)
	defer partitionLogTicker.Stop()

	for {
		select {
		case <-ticker.C:
			if ccom.cdm.PartitionOn {
				continue
			}

			requestBatch := ccom.cfcpm.GetAndClearRequests(batchSize)
			if len(requestBatch) > 0 {
				ccom.processBatchRequests(requestBatch)
			}

			responseBatch := ccom.cfcpm.GetAndClearResponses(batchSize)
			if len(responseBatch) > 0 {
				ccom.processBatchResponses(responseBatch)
			}

		case <-exportTicker.C:
			filePath := "expTest/contractPoolSize/S" + strconv.Itoa(int(ccom.pbftNode.ShardID)) + ".csv"
			ccom.cfcpm.ExportPoolSizesToCSV(filePath)

		case <-partitionLogTicker.C:
			if ccom.cdm.PartitionOn {
				ccom.pbftNode.pl.Plog.Println("waiting for finishing partition...")
			}
		}
	}
}

func (ccom *CLPAContractOutsideModule) DFS(root *core.CallNode, checkSelf bool) (*core.CallNode, bool, map[string]bool) {
	processedMap := make(map[string]bool)

	var dfsHelper func(node *core.CallNode) (*core.CallNode, bool)
	dfsHelper = func(node *core.CallNode) (*core.CallNode, bool) {
		if node == nil || node.IsProcessed {
			return nil, false
		}

		ssid := ccom.pbftNode.CurChain.Get_PartitionMap(node.Sender)
		rsid := ccom.pbftNode.CurChain.Get_PartitionMap(node.Recipient)

		if ssid != rsid {
			return node, true
		}

		allChildrenProcessed := true
		for _, child := range node.Children {
			result, found := dfsHelper(child)
			if found {
				return result, true
			}
			if !child.IsProcessed {
				allChildrenProcessed = false
			}
		}

		if allChildrenProcessed {
			node.IsProcessed = true
			processedMap[node.TypeTraceAddress] = true
		}

		return nil, false
	}

	if checkSelf {
		if result, found := dfsHelper(root); found {
			return result, true, processedMap
		}
		return root, false, processedMap
	}

	for _, child := range root.Children {
		result, found := dfsHelper(child)
		if found {
			return result, true, processedMap
		}
	}

	return root, false, processedMap
}
