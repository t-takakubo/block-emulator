// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// this message used in propose stage, so it will be invoked by InsidePBFT_Module
func (uchm *UnionChainPbftInsideExtraHandleMod) sendPartitionReady() {
	uchm.cdm.P_ReadyLock.Lock()
	uchm.cdm.PartitionReady[uchm.pbftNode.ShardID] = true
	uchm.cdm.P_ReadyLock.Unlock()

	pr := message.PartitionReady{
		FromShard: uchm.pbftNode.ShardID,
		NowSeqID:  uchm.pbftNode.sequenceID,
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := 0; sid < int(uchm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			go networks.TcpDial(send_msg, uchm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	uchm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (uchm *UnionChainPbftInsideExtraHandleMod) getPartitionReady() bool {
	uchm.cdm.P_ReadyLock.Lock()
	defer uchm.cdm.P_ReadyLock.Unlock()
	uchm.pbftNode.seqMapLock.Lock()
	defer uchm.pbftNode.seqMapLock.Unlock()
	uchm.cdm.ReadySeqLock.Lock()
	defer uchm.cdm.ReadySeqLock.Unlock()

	flag := true
	for sid, val := range uchm.pbftNode.seqIDMap {
		if rval, ok := uchm.cdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(uchm.cdm.PartitionReady) == int(uchm.pbftNode.pbftChainConfig.ShardNums) && flag
}

// send the transactions and the accountState to other leaders
func (uchm *UnionChainPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)        // Do not include mergedVertex
	lastMapid := len(uchm.cdm.ModifiedMap) - 1 // initial value is 0
	txsBeCross := make([]*core.Transaction, 0) // the transactions which will be cross-shard tx because of re-partition
	for addr, newShardID := range uchm.cdm.ModifiedMap[lastMapid] {
		if originalAddrs, ok := uchm.cdm.ReversedMergedContracts[lastMapid][partition.Vertex{Addr: addr}]; ok {
			for _, originalAddr := range originalAddrs {
				currentShard := uchm.pbftNode.CurChain.Get_PartitionMap(originalAddr)
				isDifferentShard := currentShard != newShardID
				isNewShardNotLocal := newShardID != uchm.pbftNode.ShardID
				isCurrentShardLocal := currentShard == uchm.pbftNode.ShardID

				if isDifferentShard && isNewShardNotLocal && isCurrentShardLocal {
					accountToFetch = append(accountToFetch, originalAddr)
				}
			}
			continue
		}

		if newShardID != uchm.pbftNode.ShardID && uchm.pbftNode.CurChain.Get_PartitionMap(addr) == uchm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, addr) // Extract only the accounts to be moved
		}
	}

	asFetched := uchm.pbftNode.CurChain.FetchAccounts(accountToFetch)

	// send the accounts to other shards
	uchm.pbftNode.CurChain.Txpool.GetLocked()
	uchm.pbftNode.pl.Plog.Println("sendAccounts_and_Txs(): The size of tx pool is: ", len(uchm.pbftNode.CurChain.Txpool.TxQueue))
	for destShardID := uint64(0); destShardID < uchm.pbftNode.pbftChainConfig.ShardNums; destShardID++ {
		if destShardID == uchm.pbftNode.ShardID {
			continue
		}

		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)

		for idx, originalAddr := range accountToFetch {
			if mergedVertex, ok := uchm.cdm.MergedContracts[lastMapid][originalAddr]; ok {
				if uchm.cdm.ModifiedMap[lastMapid][mergedVertex.Addr] == destShardID {
					addrSend = append(addrSend, originalAddr)
					addrSet[originalAddr] = true
					asSend = append(asSend, asFetched[idx])
				}
				continue
			}
			if uchm.cdm.ModifiedMap[lastMapid][originalAddr] == destShardID {
				addrSend = append(addrSend, originalAddr)
				addrSet[originalAddr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}

		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(uchm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := uchm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			if !ptx.HasContract {
				// whether should be transfer or not
				beSend := false
				beRemoved := false
				_, ok1 := addrSet[ptx.Sender]
				_, ok2 := addrSet[ptx.Recipient]
				if ptx.RawTxHash == nil { // if this tx is an inner-shard tx...
					if ptx.HasBroker {
						if ptx.SenderIsBroker {
							beSend = ok2
							beRemoved = ok2
						} else {
							beRemoved = ok1
							beSend = ok1
						}
					} else if ok1 || ok2 { // if the inner-shard tx should be transferred.
						txsBeCross = append(txsBeCross, ptx)
						beRemoved = true
					}
					// all inner-shard tx should not be added into the account transfer message
				} else if ptx.FinalRecipient == ptx.Recipient {
					beSend = ok2
					beRemoved = ok2
				} else if ptx.OriginalSender == ptx.Sender {
					beRemoved = ok1
					beSend = ok1
				}

				if beSend {
					txSend = append(txSend, ptx)
				}
				if !beRemoved {
					uchm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = uchm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
					firstPtr++
				}
			}

			if ptx.HasContract {
				remainingAccount := make(map[string]*core.Account)
				sendAccount := make(map[string]*core.Account)
				isTxSend := false

				if ptx.IsCrossShardFuncCall {
					if len(ptx.StateChangeAccounts) > 0 {
						for _, account := range ptx.StateChangeAccounts {
							_, ok := addrSet[account.AcAddress]
							if ok {
								sendAccount[account.AcAddress] = account
								isTxSend = true
							} else {
								remainingAccount[account.AcAddress] = account
							}

						}

						ptx.StateChangeAccounts = remainingAccount

						if len(remainingAccount) > 0 {
							uchm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
							firstPtr++
						} else {
							ptx.IsDeleted = true
						}

						if isTxSend {
							ptx.StateChangeAccounts = sendAccount
							txSend = append(txSend, ptx)
						}
					}
				}

				if ptx.IsAllInner {
					if len(ptx.StateChangeAccounts) > 0 {
						remainingAccount := make(map[string]*core.Account)
						sendAccount := make(map[string]*core.Account)
						isTxSend := false

						for _, account := range ptx.StateChangeAccounts {
							if _, ok := addrSet[account.AcAddress]; ok {
								sendAccount[account.AcAddress] = account
								isTxSend = true
							} else {
								remainingAccount[account.AcAddress] = account
							}
						}

						if isTxSend && len(ptx.StateChangeAccounts) != len(sendAccount) && len(remainingAccount) > 0 {
							ptx.IsAllInner = false
							ptx.IsCrossShardFuncCall = true
							ptx.DivisionCount++
						}

						if len(remainingAccount) > 0 {
							ptx.StateChangeAccounts = remainingAccount
							uchm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
							firstPtr++
						} else {
							ptx.IsDeleted = true
						}

						if isTxSend {
							ptxSend := *ptx
							ptxSend.StateChangeAccounts = sendAccount
							ptxSend.IsDeleted = false
							txSend = append(txSend, &ptxSend)
						}

					}
				}
			}
		}

		uchm.pbftNode.CurChain.Txpool.TxQueue = uchm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]

		// for cross shard function call in request pool
		requestSend := make([]*message.CrossShardFunctionRequest, 0)
		firstPtr = 0
		for secondPtr := 0; secondPtr < len(uchm.cfcpm.RequestPool); secondPtr++ {
			request := uchm.cfcpm.RequestPool[secondPtr]

			_, ok := addrSet[request.Recepient]
			if ok {
				requestSend = append(requestSend, request)
			} else {
				uchm.cfcpm.RequestPool[firstPtr] = request
				firstPtr++
			}
		}
		uchm.cfcpm.RequestPool = uchm.cfcpm.RequestPool[:firstPtr]

		// for cross shard function call in response pool
		responseSend := make([]*message.CrossShardFunctionResponse, 0)
		firstPtr = 0
		for secondPtr := 0; secondPtr < len(uchm.cfcpm.ResponsePool); secondPtr++ {
			response := uchm.cfcpm.ResponsePool[secondPtr]

			_, ok := addrSet[response.Recipient]
			if ok {
				responseSend = append(responseSend, response)
			} else {
				uchm.cfcpm.ResponsePool[firstPtr] = response
				firstPtr++
			}
		}
		uchm.cfcpm.ResponsePool = uchm.cfcpm.ResponsePool[:firstPtr]

		uchm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", destShardID)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    uchm.pbftNode.ShardID,
			Txs:          txSend,
			Requests:     requestSend,
			Responses:    responseSend,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.CAccountTransferMsg_broker, aByte)
		go networks.TcpDial(send_msg, uchm.pbftNode.ip_nodeTable[destShardID][0])
		uchm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", destShardID)
	}
	uchm.pbftNode.CurChain.Txpool.GetUnlocked()

	// send these txs to supervisor
	i2ctx := message.InnerTx2CrossTx{
		Txs: txsBeCross,
	}
	icByte, err := json.Marshal(i2ctx)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CInner2CrossTx, icByte)
	go networks.TcpDial(send_msg, uchm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
}

// fetch collect infos
func (uchm *UnionChainPbftInsideExtraHandleMod) getCollectOver() bool {
	uchm.cdm.CollectLock.Lock()
	defer uchm.cdm.CollectLock.Unlock()
	return uchm.cdm.CollectOver
}

// propose a partition message
func (uchm *UnionChainPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	uchm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID)
	// add all data in pool into the set
	for _, at := range uchm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			uchm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		uchm.cdm.ReceivedNewTx = append(uchm.cdm.ReceivedNewTx, at.Txs...)
		uchm.cfcpm.AddRequests(at.Requests)
		uchm.cfcpm.AddResponses(at.Responses)
	}
	// propose, send all txs to other nodes in shard
	uchm.pbftNode.pl.Plog.Println("The number of ReceivedNewTx: ", len(uchm.cdm.ReceivedNewTx))

	filteredTxs := []*core.Transaction{}
	txHashMap := make(map[string]*core.Transaction)
	for _, rtx := range uchm.cdm.ReceivedNewTx {
		if rtx.HasContract {
			for addr, account := range rtx.StateChangeAccounts {
				if mergedVertex, ok := uchm.cdm.MergedContracts[uchm.cdm.AccountTransferRound][addr]; ok {
					addr = mergedVertex.Addr
				}

				shardID := uchm.cdm.ModifiedMap[uchm.cdm.AccountTransferRound][addr]

				if shardID != uchm.pbftNode.ShardID {
					fmt.Printf("[ERROR] Addr: %s, modifiedMapShardID: %d, currentShardID: %d, IsContract: %t\n", addr, shardID, uchm.pbftNode.ShardID, account.IsContract)
					log.Panic("error tx: StateChangeAccount is not in the shard ", addr, shardID, uchm.pbftNode.ShardID, account.IsContract)
				}

			}
		}

		if existingTx, exists := txHashMap[string(rtx.TxHash)]; exists {
			for addr, account := range rtx.StateChangeAccounts {
				existingTx.StateChangeAccounts[addr] = account
			}
			existingTx.MergeCount++

			if rtx.InternalTxs != nil {
				existingTx.InternalTxs = rtx.InternalTxs
				existingTx.IsExecuteCLPA = true
			}

			if rtx.DivisionCount == existingTx.MergeCount+1 {
				existingTx.IsAllInner = true
				existingTx.IsCrossShardFuncCall = false
			}
		} else {
			txHashMap[string(rtx.TxHash)] = rtx
			filteredTxs = append(filteredTxs, rtx)
		}
	}

	uchm.cdm.ReceivedNewTx = filteredTxs

	txHashToIdxMap := make(map[string]int) // key: txHash, value: TxQueue index

	for idx, tx := range uchm.pbftNode.CurChain.Txpool.TxQueue {
		txHashToIdxMap[string(tx.TxHash)] = idx
	}

	filteredNewTx := make([]*core.Transaction, 0)

	for _, rtx := range uchm.cdm.ReceivedNewTx {
		if poolIdx, ok := txHashToIdxMap[string(rtx.TxHash)]; ok {
			for addr, account := range rtx.StateChangeAccounts {
				uchm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].StateChangeAccounts[addr] = account
			}

			if rtx.IsDeleted {
				if rtx.IsCrossShardFuncCall && uchm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].DivisionCount == rtx.MergeCount+1 {
					uchm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsCrossShardFuncCall = false
					uchm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsAllInner = true
				}
			}
		} else {
			filteredNewTx = append(filteredNewTx, rtx)
		}
	}

	uchm.cdm.ReceivedNewTx = filteredNewTx

	uchm.pbftNode.CurChain.Txpool.AddTxs2Front(uchm.cdm.ReceivedNewTx)

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range uchm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{
		ModifiedMap:     uchm.cdm.ModifiedMap[uchm.cdm.AccountTransferRound],
		MergedContracts: uchm.cdm.MergedContracts[uchm.cdm.AccountTransferRound],
		Addrs:           atmaddr,
		AccountState:    atmAs,
		ATid:            uint64(len(uchm.cdm.ModifiedMap)),
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.PartitionReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}

	return true, r
}

// all nodes in a shard will do accout Transfer, to sync the state trie
func (uchm *UnionChainPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	// change the partition Map
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		uchm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}

	for key, val := range atm.MergedContracts {
		uchm.pbftNode.CurChain.Update_MergedContracts(key, val)
	}

	uchm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	uchm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	uchm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	uchm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, uchm.pbftNode.view.Load())

	if uint64(len(uchm.cdm.ModifiedMap)) != atm.ATid {
		uchm.cdm.ModifiedMap = append(uchm.cdm.ModifiedMap, atm.ModifiedMap)
		uchm.cdm.MergedContracts = append(uchm.cdm.MergedContracts, atm.MergedContracts)
		uchm.cdm.ReversedMergedContracts = append(uchm.cdm.ReversedMergedContracts, ReverseMap(atm.MergedContracts))
	}
	uchm.cdm.AccountTransferRound = atm.ATid
	uchm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	uchm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	uchm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	uchm.cdm.PartitionOn = false

	uchm.cdm.CollectLock.Lock()
	uchm.cdm.CollectOver = false
	uchm.cdm.CollectLock.Unlock()

	uchm.cdm.P_ReadyLock.Lock()
	uchm.cdm.PartitionReady = make(map[uint64]bool)
	uchm.cdm.P_ReadyLock.Unlock()

	uchm.pbftNode.CurChain.PrintBlockChain()
}
