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

type UnionChainOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
	cfcpm    *dataSupport.CrossFunctionCallPoolManager
}

func (ucom *UnionChainOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CSeqIDinfo:
		ucom.handleSeqIDinfos(content)
	case message.CInject:
		ucom.handleInjectTx(content)

	// messages about CLPA
	case message.StartCLPA:
		ucom.handleStartCLPA()
	case message.CPartitionMsg:
		ucom.handlePartitionMsg(content)
	case message.CAccountTransferMsg_broker:
		ucom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		ucom.handlePartitionReady(content)

	// for smart contract
	case message.CContractInject:
		ucom.handleContractInject(content)
	case message.CContractRequest:
		ucom.cfcpm.HandleContractRequest(content)
	case message.CContactResponse:
		ucom.cfcpm.HandleContractResponse(content)

	default:
	}
	return true
}

// receive SeqIDinfo
func (ucom *UnionChainOutsideModule) handleSeqIDinfos(content []byte) {
	sii := new(message.SeqIDinfo)
	err := json.Unmarshal(content, sii)
	if err != nil {
		log.Panic(err)
	}
	ucom.pbftNode.pl.Plog.Printf("S%dN%d : has received SeqIDinfo from shard %d, the senderSeq is %d\n", ucom.pbftNode.ShardID, ucom.pbftNode.NodeID, sii.SenderShardID, sii.SenderSeq)
	ucom.pbftNode.seqMapLock.Lock()
	ucom.pbftNode.seqIDMap[sii.SenderShardID] = sii.SenderSeq
	ucom.pbftNode.seqMapLock.Unlock()
	ucom.pbftNode.pl.Plog.Printf("S%dN%d : has handled SeqIDinfo msg\n", ucom.pbftNode.ShardID, ucom.pbftNode.NodeID)
}

func (ucom *UnionChainOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	ucom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	ucom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", ucom.pbftNode.ShardID, ucom.pbftNode.NodeID, len(it.Txs))
}

func (ucom *UnionChainOutsideModule) handleStartCLPA() {
	ucom.pbftNode.IsStartCLPA = true
}

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (ucom *UnionChainOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	ucom.cdm.ModifiedMap = append(ucom.cdm.ModifiedMap, pm.PartitionModified)
	ucom.cdm.MergedContracts = append(ucom.cdm.MergedContracts, pm.MergedContracts)
	ucom.cdm.ReversedMergedContracts = append(ucom.cdm.ReversedMergedContracts, ReverseMap(pm.MergedContracts))
	ucom.pbftNode.IsStartCLPA = false
	ucom.cdm.PartitionOn = true
}

// wait for other shards' last rounds are over
func (ucom *UnionChainOutsideModule) handlePartitionReady(content []byte) {
	pr := new(message.PartitionReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	ucom.cdm.P_ReadyLock.Lock()
	ucom.cdm.PartitionReady[pr.FromShard] = true
	ucom.cdm.P_ReadyLock.Unlock()

	ucom.pbftNode.seqMapLock.Lock()
	ucom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	ucom.pbftNode.seqMapLock.Unlock()
}

// when the message from other shard arriving, it should be added into the message pool
func (ucom *UnionChainOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}

	ucom.cdm.AccountStateTxLock.Lock()
	ucom.cdm.AccountStateTx[at.FromShard] = at
	ucom.cdm.AccountStateTxLock.Unlock()

	ucom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", ucom.pbftNode.ShardID, ucom.pbftNode.NodeID, at.FromShard)

	if len(ucom.cdm.AccountStateTx) == int(ucom.pbftNode.pbftChainConfig.ShardNums)-1 {
		ucom.cdm.CollectLock.Lock()
		ucom.cdm.CollectOver = true
		ucom.cdm.CollectLock.Unlock()
		ucom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", ucom.pbftNode.ShardID, ucom.pbftNode.NodeID)
	}
}

func (ucom *UnionChainOutsideModule) handleContractInject(content []byte) {
	ci := new(message.ContractInjectTxs)
	err := json.Unmarshal(content, ci)
	if err != nil {
		log.Panic("handleContractInject unmarshal error", err)
	}

	requestsByShard, innerTxList := ucom.processContractInject(ci.Txs)

	ucom.sendRequests(requestsByShard)

	if len(innerTxList) > 0 {
		ucom.pbftNode.CurChain.Txpool.AddTxs2Front(innerTxList)
	}
}

func (ucom *UnionChainOutsideModule) processContractInject(txs []*core.Transaction) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	innerTxList := make([]*core.Transaction, 0)

	for _, tx := range txs {
		root := core.BuildExecutionCallTree(tx, make(map[string]bool))

		differentShardNode, hasDiffShard, processedMap := ucom.DFS(root, true)

		if hasDiffShard {
			destShardID := ucom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)

			request := &message.CrossShardFunctionRequest{
				RequestID:          string(tx.TxHash),
				OriginalSender:     tx.Sender,
				SourceShardID:      ucom.pbftNode.ShardID,
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

func (ucom *UnionChainOutsideModule) processBatchRequests(requests []*message.CrossShardFunctionRequest) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	responseByShard := make(map[uint64][]*message.CrossShardFunctionResponse)

	for _, req := range requests {
		root := core.BuildExecutionCallTree(req.Tx, req.ProcessedMap)
		currentCallNode := root.FindNodeByTTA(req.TypeTraceAddress)

		if currentCallNode == nil {
			fmt.Println(req.TypeTraceAddress)
			log.Panic("currentCallNode is nil.")
		}

		if currentCallNode.IsLeaf {
			randomAccountInSC := GenerateRandomString(params.AccountNumInContract)
			isAccountLocked := ucom.cfcpm.SClock.IsAccountLocked(currentCallNode.Recipient, randomAccountInSC, req.RequestID)
			if isAccountLocked {
				ucom.cfcpm.AddRequest(req)
				continue
			}

			switch currentCallNode.CallType {
			case "call", "create":
				err := ucom.cfcpm.SClock.LockAccount(currentCallNode.Recipient, randomAccountInSC, req.RequestID)
				if err != nil {
					fmt.Println(err)
				}
			case "delegatecall":
				err := ucom.cfcpm.SClock.LockAccount(currentCallNode.Sender, randomAccountInSC, req.RequestID)
				if err != nil {
					fmt.Println(err)
				}
			}

			destShardID := req.SourceShardID
			req.ProcessedMap[currentCallNode.TypeTraceAddress] = true

			response := &message.CrossShardFunctionResponse{
				ResponseID:         req.RequestID,
				OriginalSender:     req.OriginalSender,
				SourceShardID:      ucom.pbftNode.ShardID,
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
			differentShardNode, hasDiffShard, processedMap := ucom.DFS(currentCallNode, false)
			for k, v := range processedMap {
				req.ProcessedMap[k] = v
			}

			if hasDiffShard {
				destShardID := ucom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)

				request := &message.CrossShardFunctionRequest{
					RequestID:          string(req.Tx.TxHash),
					OriginalSender:     req.OriginalSender,
					SourceShardID:      ucom.pbftNode.ShardID,
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
					ResponseID:         string(req.Tx.TxHash),
					OriginalSender:     req.OriginalSender,
					SourceShardID:      ucom.pbftNode.ShardID,
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

	// send request and response
	ucom.sendRequests(requestsByShard)
	ucom.sendResponses(responseByShard)
}

func (ucom *UnionChainOutsideModule) processBatchResponses(responses []*message.CrossShardFunctionResponse) {
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

		differentShardNode, hasDiffShard, processedMap := ucom.DFS(currentCallNode, false)
		for k, v := range processedMap {
			res.ProcessedMap[k] = v
		}

		if !hasDiffShard {
			destShardID := ucom.pbftNode.CurChain.Get_PartitionMap(currentCallNode.Sender)
			res.ProcessedMap[currentCallNode.TypeTraceAddress] = true

			if currentCallNode.CallType == "root" {
				if destShardID != ucom.pbftNode.ShardID {
					response := &message.CrossShardFunctionResponse{
						ResponseID:         res.ResponseID,
						OriginalSender:     res.OriginalSender,
						SourceShardID:      ucom.pbftNode.ShardID,
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
					ssid := ucom.pbftNode.CurChain.Get_PartitionMap(res.Tx.Sender)
					rsid := ucom.pbftNode.CurChain.Get_PartitionMap(res.Tx.Recipient)

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
							rsid = ucom.pbftNode.CurChain.Get_PartitionMap(itx.Recipient)
							if stateChangeAccountByShard[rsid] == nil {
								stateChangeAccountByShard[rsid] = make([]*core.Account, 0)
							}
							stateChangeAccountByShard[rsid] = append(stateChangeAccountByShard[rsid], &core.Account{AcAddress: itx.Recipient, IsContract: itx.RecipientIsContract})
						case "delegatecall":
							ssid = ucom.pbftNode.CurChain.Get_PartitionMap(itx.Sender)
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
							ucom.cfcpm.SClock.UnlockAllByRequestID(tx.RequestID)
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
					SourceShardID:      ucom.pbftNode.ShardID,
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
			destShardID := ucom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
			request := &message.CrossShardFunctionRequest{
				RequestID:          res.ResponseID,
				OriginalSender:     res.OriginalSender,
				SourceShardID:      ucom.pbftNode.ShardID,
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

	ucom.sendRequests(requestsByShard)
	ucom.sendResponses(responseByShard)
	ucom.sendInjectTransactions(injectTxByShard)
}

func (ucom *UnionChainOutsideModule) sendRequests(requestsByShard map[uint64][]*message.CrossShardFunctionRequest) {
	for sid, requests := range requestsByShard {
		rByte, err := json.Marshal(requests)
		if err != nil {
			log.Panic("unmarshal error", err)
		}
		msg_send := message.MergeMessage(message.CContractRequest, rByte)
		go networks.TcpDial(msg_send, ucom.pbftNode.ip_nodeTable[sid][0])
	}
}

func (ucom *UnionChainOutsideModule) sendResponses(responsesByShard map[uint64][]*message.CrossShardFunctionResponse) {
	for sid, responses := range responsesByShard {
		rByte, err := json.Marshal(responses)
		if err != nil {
			log.Panic("unmarshal error", err)
		}
		msg_send := message.MergeMessage(message.CContactResponse, rByte)
		go networks.TcpDial(msg_send, ucom.pbftNode.ip_nodeTable[sid][0])
	}
}

func (ucom *UnionChainOutsideModule) sendInjectTransactions(sendToShard map[uint64][]*core.Transaction) {
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
		go networks.TcpDial(sendMsg, ucom.pbftNode.ip_nodeTable[sid][0])
	}
}

func (ucom *UnionChainOutsideModule) StartBatchProcessing(batchSize int, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	exportTicker := time.NewTicker(time.Duration(params.Block_Interval) * time.Millisecond)
	defer exportTicker.Stop()

	partitionLogTicker := time.NewTicker(10 * time.Second)
	defer partitionLogTicker.Stop()

	for {
		select {
		case <-ticker.C:
			if ucom.cdm.PartitionOn {
				continue
			}

			requestBatch := ucom.cfcpm.GetAndClearRequests(batchSize)
			if len(requestBatch) > 0 {
				ucom.processBatchRequests(requestBatch)
			}

			responseBatch := ucom.cfcpm.GetAndClearResponses(batchSize)
			if len(responseBatch) > 0 {
				ucom.processBatchResponses(responseBatch)
			}

		case <-exportTicker.C:
			filePath := "expTest/contractPoolSize/S" + strconv.Itoa(int(ucom.pbftNode.ShardID)) + ".csv"
			ucom.cfcpm.ExportPoolSizesToCSV(filePath)

		case <-partitionLogTicker.C:
			if ucom.cdm.PartitionOn {
				ucom.pbftNode.pl.Plog.Println("waiting for finishing partition...")
			}
		}
	}
}

func (ucom *UnionChainOutsideModule) DFS(root *core.CallNode, checkSelf bool) (*core.CallNode, bool, map[string]bool) {
	processedMap := make(map[string]bool)

	var dfsHelper func(node *core.CallNode) (*core.CallNode, bool)
	dfsHelper = func(node *core.CallNode) (*core.CallNode, bool) {
		if node == nil || node.IsProcessed {
			return nil, false
		}

		ssid := ucom.pbftNode.CurChain.Get_PartitionMap(node.Sender)
		rsid := ucom.pbftNode.CurChain.Get_PartitionMap(node.Recipient)

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
