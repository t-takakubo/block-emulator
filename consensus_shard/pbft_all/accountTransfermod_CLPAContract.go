// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/partition"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// this message used in propose stage, so it will be invoked by InsidePBFT_Module
func (cchm *CLPAContractInsideExtraHandleMod) sendPartitionReady() {
	cchm.cdm.P_ReadyLock.Lock()
	cchm.cdm.PartitionReady[cchm.pbftNode.ShardID] = true
	cchm.cdm.P_ReadyLock.Unlock()

	pr := message.PartitionReady{
		FromShard: cchm.pbftNode.ShardID,
		NowSeqID:  cchm.pbftNode.sequenceID,
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := 0; sid < int(cchm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			go networks.TcpDial(send_msg, cchm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	cchm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (cchm *CLPAContractInsideExtraHandleMod) getPartitionReady() bool {
	cchm.cdm.P_ReadyLock.Lock()
	defer cchm.cdm.P_ReadyLock.Unlock()
	cchm.pbftNode.seqMapLock.Lock()
	defer cchm.pbftNode.seqMapLock.Unlock()
	cchm.cdm.ReadySeqLock.Lock()
	defer cchm.cdm.ReadySeqLock.Unlock()

	flag := true
	for sid, val := range cchm.pbftNode.seqIDMap {
		if rval, ok := cchm.cdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(cchm.cdm.PartitionReady) == int(cchm.pbftNode.pbftChainConfig.ShardNums) && flag
}

// send the transactions and the accountState to other leaders
func (cchm *CLPAContractInsideExtraHandleMod) sendAccounts_and_Txs() {
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)        // Do not include mergedVertex
	lastMapid := len(cchm.cdm.ModifiedMap) - 1 // initial value is 0
	for addr, newShardID := range cchm.cdm.ModifiedMap[lastMapid] {
		if originalAddrs, ok := cchm.cdm.ReversedMergedContracts[lastMapid][partition.Vertex{Addr: addr}]; ok {
			for _, originalAddr := range originalAddrs {
				currentShard := cchm.pbftNode.CurChain.Get_PartitionMap(originalAddr)
				isDifferentShard := currentShard != newShardID
				isNewShardNotLocal := newShardID != cchm.pbftNode.ShardID
				isCurrentShardLocal := currentShard == cchm.pbftNode.ShardID

				if isDifferentShard && isNewShardNotLocal && isCurrentShardLocal {
					accountToFetch = append(accountToFetch, originalAddr)
				}
			}
			continue
		}

		if newShardID != cchm.pbftNode.ShardID && cchm.pbftNode.CurChain.Get_PartitionMap(addr) == cchm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, addr) // Extract only the accounts to be moved
		}
	}

	asFetched := cchm.pbftNode.CurChain.FetchAccounts(accountToFetch)

	// send the accounts to other shards
	cchm.pbftNode.CurChain.Txpool.GetLocked()
	cchm.pbftNode.pl.Plog.Println("sendAccounts_and_Txs(): The size of tx pool is: ", len(cchm.pbftNode.CurChain.Txpool.TxQueue))
	for destShardID := uint64(0); destShardID < cchm.pbftNode.pbftChainConfig.ShardNums; destShardID++ {
		if destShardID == cchm.pbftNode.ShardID {
			continue
		}

		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)

		for idx, originalAddr := range accountToFetch {
			if mergedVertex, ok := cchm.cdm.MergedContracts[lastMapid][originalAddr]; ok {
				if cchm.cdm.ModifiedMap[lastMapid][mergedVertex.Addr] == destShardID {
					addrSend = append(addrSend, originalAddr)
					addrSet[originalAddr] = true
					asSend = append(asSend, asFetched[idx])
				}
				continue
			}
			if cchm.cdm.ModifiedMap[lastMapid][originalAddr] == destShardID {
				addrSend = append(addrSend, originalAddr)
				addrSet[originalAddr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}

		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(cchm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := cchm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			if !ptx.HasContract {
				_, ok1 := addrSet[ptx.Sender]
				condition1 := ok1 && !ptx.Relayed
				_, ok2 := addrSet[ptx.Recipient]
				condition2 := ok2 && ptx.Relayed
				if condition1 || condition2 {
					txSend = append(txSend, ptx)
				} else {
					cchm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
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
							cchm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
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
							cchm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
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

		cchm.pbftNode.CurChain.Txpool.TxQueue = cchm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]

		// for cross shard function call in request pool
		requestSend := make([]*message.CrossShardFunctionRequest, 0)
		firstPtr = 0
		for secondPtr := 0; secondPtr < len(cchm.cfcpm.RequestPool); secondPtr++ {
			request := cchm.cfcpm.RequestPool[secondPtr]

			_, ok := addrSet[request.Recepient]
			if ok {
				requestSend = append(requestSend, request)
			} else {
				cchm.cfcpm.RequestPool[firstPtr] = request
				firstPtr++
			}
		}
		cchm.cfcpm.RequestPool = cchm.cfcpm.RequestPool[:firstPtr]

		// for cross shard function call in response pool
		responseSend := make([]*message.CrossShardFunctionResponse, 0)
		firstPtr = 0
		for secondPtr := 0; secondPtr < len(cchm.cfcpm.ResponsePool); secondPtr++ {
			response := cchm.cfcpm.ResponsePool[secondPtr]

			_, ok := addrSet[response.Recipient]
			if ok {
				responseSend = append(responseSend, response)
			} else {
				cchm.cfcpm.ResponsePool[firstPtr] = response
				firstPtr++
			}
		}
		cchm.cfcpm.ResponsePool = cchm.cfcpm.ResponsePool[:firstPtr]

		cchm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", destShardID)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    cchm.pbftNode.ShardID,
			Txs:          txSend,
			Requests:     requestSend,
			Responses:    responseSend,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.AccountState_and_TX, aByte)
		go networks.TcpDial(send_msg, cchm.pbftNode.ip_nodeTable[destShardID][0])
		cchm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", destShardID)
	}
	cchm.pbftNode.CurChain.Txpool.GetUnlocked()
}

// fetch collect infos
func (cchm *CLPAContractInsideExtraHandleMod) getCollectOver() bool {
	cchm.cdm.CollectLock.Lock()
	defer cchm.cdm.CollectLock.Unlock()
	return cchm.cdm.CollectOver
}

// propose a partition message
func (cchm *CLPAContractInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	cchm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID)
	// add all data in pool into the set
	for _, at := range cchm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			cchm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		cchm.cdm.ReceivedNewTx = append(cchm.cdm.ReceivedNewTx, at.Txs...)
		cchm.cfcpm.AddRequests(at.Requests)
		cchm.cfcpm.AddResponses(at.Responses)
	}
	// propose, send all txs to other nodes in shard
	cchm.pbftNode.pl.Plog.Println("The number of ReceivedNewTx: ", len(cchm.cdm.ReceivedNewTx))

	filteredTxs := []*core.Transaction{}
	txHashMap := make(map[string]*core.Transaction)
	for _, rtx := range cchm.cdm.ReceivedNewTx {
		if !rtx.HasContract {
			sender := rtx.Sender
			recipient := rtx.Recipient
			if mergedVertex, ok := cchm.cdm.MergedContracts[cchm.cdm.AccountTransferRound][rtx.Sender]; ok {
				sender = mergedVertex.Addr
			}
			if mergedVertex, ok := cchm.cdm.MergedContracts[cchm.cdm.AccountTransferRound][rtx.Recipient]; ok {
				recipient = mergedVertex.Addr
			}

			if !rtx.Relayed && cchm.cdm.ModifiedMap[cchm.cdm.AccountTransferRound][sender] != cchm.pbftNode.ShardID {
				fmt.Printf("[ERROR] Sender: %s, Recipient: %s modifiedMapShardID: %d, currentShardID: %d\n", rtx.Sender, rtx.Recipient, cchm.cdm.ModifiedMap[cchm.cdm.AccountTransferRound][rtx.Sender], cchm.pbftNode.ShardID)
			}
			if rtx.Relayed && cchm.cdm.ModifiedMap[cchm.cdm.AccountTransferRound][recipient] != cchm.pbftNode.ShardID {
				fmt.Printf("[ERROR] Sender: %s, Recipient: %s, modifiedMapShardID: %d, currentShardID: %d\n", rtx.Sender, rtx.Recipient, cchm.cdm.ModifiedMap[cchm.cdm.AccountTransferRound][rtx.Recipient], cchm.pbftNode.ShardID)
			}
		}

		if rtx.HasContract {
			for addr, account := range rtx.StateChangeAccounts {
				if mergedVertex, ok := cchm.cdm.MergedContracts[cchm.cdm.AccountTransferRound][addr]; ok {
					addr = mergedVertex.Addr
				}

				shardID := cchm.cdm.ModifiedMap[cchm.cdm.AccountTransferRound][addr]

				if shardID != cchm.pbftNode.ShardID {
					rtx.PrintTx()
					fmt.Printf("[ERROR] Addr: %s, modifiedMapShardID: %d, currentShardID: %d, IsContract: %t\n", addr, shardID, cchm.pbftNode.ShardID, account.IsContract)
					log.Panic("error tx: StateChangeAccount is not in the shard ", addr, shardID, cchm.pbftNode.ShardID, account.IsContract)
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

	cchm.cdm.ReceivedNewTx = filteredTxs
	txHashToIdxMap := make(map[string]int) // key: txHash, value: TxQueue index

	for idx, tx := range cchm.pbftNode.CurChain.Txpool.TxQueue {
		txHashToIdxMap[string(tx.TxHash)] = idx
	}

	filteredNewTx := make([]*core.Transaction, 0)

	for _, rtx := range cchm.cdm.ReceivedNewTx {
		if poolIdx, ok := txHashToIdxMap[string(rtx.TxHash)]; ok {
			for addr, account := range rtx.StateChangeAccounts {
				cchm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].StateChangeAccounts[addr] = account
			}

			if rtx.IsDeleted {
				if rtx.IsCrossShardFuncCall && cchm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].DivisionCount == rtx.MergeCount+1 {
					cchm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsCrossShardFuncCall = false
					cchm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsAllInner = true
				}
			}
		} else {
			filteredNewTx = append(filteredNewTx, rtx)
		}
	}

	cchm.cdm.ReceivedNewTx = filteredNewTx
	cchm.pbftNode.CurChain.Txpool.AddTxs2Front(cchm.cdm.ReceivedNewTx)

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range cchm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{
		ModifiedMap:     cchm.cdm.ModifiedMap[cchm.cdm.AccountTransferRound],
		MergedContracts: cchm.cdm.MergedContracts[cchm.cdm.AccountTransferRound],
		Addrs:           atmaddr,
		AccountState:    atmAs,
		ATid:            uint64(len(cchm.cdm.ModifiedMap)),
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
// Called when a partition block is committed.
func (cchm *CLPAContractInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	// change the partition Map
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		cchm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}

	for key, val := range atm.MergedContracts {
		cchm.pbftNode.CurChain.Update_MergedContracts(key, val)
	}
	cchm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	cchm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	cchm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	cchm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, cchm.pbftNode.view.Load())

	if uint64(len(cchm.cdm.ModifiedMap)) != atm.ATid {
		cchm.cdm.ModifiedMap = append(cchm.cdm.ModifiedMap, atm.ModifiedMap)
		cchm.cdm.MergedContracts = append(cchm.cdm.MergedContracts, atm.MergedContracts)
		cchm.cdm.ReversedMergedContracts = append(cchm.cdm.ReversedMergedContracts, ReverseMap(atm.MergedContracts))
	}
	cchm.cdm.AccountTransferRound = atm.ATid
	cchm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	cchm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	cchm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	cchm.cdm.PartitionOn = false

	cchm.cdm.CollectLock.Lock()
	cchm.cdm.CollectOver = false
	cchm.cdm.CollectLock.Unlock()

	cchm.cdm.P_ReadyLock.Lock()
	cchm.cdm.PartitionReady = make(map[uint64]bool)
	cchm.cdm.P_ReadyLock.Unlock()

	cchm.pbftNode.CurChain.PrintBlockChain()
}
