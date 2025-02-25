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

type UnionChainPbftInsideExtraHandleMod struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
	cfcpm    *dataSupport.CrossFunctionCallPoolManager
}

// propose request with different types
func (uchm *UnionChainPbftInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	if uchm.cdm.PartitionOn {
		uchm.pbftNode.pl.Plog.Println("Propose the partition block...")
		uchm.sendPartitionReady()
		for !uchm.getPartitionReady() {
			time.Sleep(time.Second)
		}
		// send accounts and txs
		uchm.sendAccounts_and_Txs()
		// propose a partition
		for !uchm.getCollectOver() {
			time.Sleep(time.Second)
		}
		return uchm.proposePartition()
	}

	// ELSE: propose a block
	block := uchm.pbftNode.CurChain.GenerateBlock(int32(uchm.pbftNode.NodeID))
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()
	return true, r

}

// the diy operation in preprepare
func (uchm *UnionChainPbftInsideExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// judge whether it is a partitionRequest or not
	isPartitionReq := ppmsg.RequestMsg.RequestType == message.PartitionReq

	if isPartitionReq {
		// after some checking
		uchm.pbftNode.pl.Plog.Printf("S%dN%d : a partition block\n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID)
	} else {
		// the request is a block
		if uchm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
			uchm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID)
			return false
		}
	}
	uchm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID)
	uchm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (uchm *UnionChainPbftInsideExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (uchm *UnionChainPbftInsideExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r := uchm.pbftNode.requestPool[string(cmsg.Digest)]
	// requestType ...
	if r.RequestType == message.PartitionReq {
		// if a partition Requst ...
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)
		uchm.accountTransfer_do(atm)
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	uchm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID, block.Header.Number, uchm.pbftNode.CurChain.CurrentBlock.Header.Number)
	uchm.pbftNode.CurChain.AddBlock(block)
	uchm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID, block.Header.Number)
	uchm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if uchm.pbftNode.NodeID == uint64(uchm.pbftNode.view.Load()) {
		uchm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send broker confirm txs at height = %d \n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID, block.Header.Number)
		// generate brokertxs and collect txs excuted
		innerShardTxs := make([]*core.Transaction, 0)
		broker1Txs := make([]*core.Transaction, 0)
		broker2Txs := make([]*core.Transaction, 0)

		crossShardFunctionCall := make([]*core.Transaction, 0)
		innerSCTxs := make([]*core.Transaction, 0)

		for _, tx := range block.Body {
			if !tx.HasContract {
				isBroker1Tx := tx.Sender == tx.OriginalSender
				isBroker2Tx := tx.Recipient == tx.FinalRecipient

				senderIsInshard := uchm.pbftNode.CurChain.Get_PartitionMap(tx.Sender) == uchm.pbftNode.ShardID
				recipientIsInshard := uchm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient) == uchm.pbftNode.ShardID
				if isBroker1Tx && !senderIsInshard {
					tx.PrintTx()
					fmt.Println("[ERROR] Err tx1")
				}
				if isBroker2Tx && !recipientIsInshard {
					tx.PrintTx()
					fmt.Println("[ERROR] Err tx2")
				}
				if tx.RawTxHash == nil {
					if tx.HasBroker {
						if tx.SenderIsBroker && !recipientIsInshard {
							tx.PrintTx()
							fmt.Println("[ERROR] err tx 1 - recipient")
						}
						if !tx.SenderIsBroker && !senderIsInshard {
							tx.PrintTx()
							fmt.Println("[ERROR] err tx 1 - sender")
						}
					} else {
						if !senderIsInshard || !recipientIsInshard {
							tx.PrintTx()
							fmt.Println("[ERROR] err tx - without broker")
						}
					}
				}

				if isBroker2Tx {
					broker2Txs = append(broker2Txs, tx)
				} else if isBroker1Tx {
					broker1Txs = append(broker1Txs, tx)
				} else {
					innerShardTxs = append(innerShardTxs, tx)
				}
			}

			if tx.HasContract {
				if tx.IsCrossShardFuncCall {
					crossShardFunctionCall = append(crossShardFunctionCall, tx)
					uchm.cfcpm.SClock.UnlockAllByRequestID(tx.RequestID)
				} else if tx.IsAllInner {
					innerSCTxs = append(innerSCTxs, tx)
				}
			}

		}

		for sid := uint64(0); sid < uchm.pbftNode.pbftChainConfig.ShardNums; sid++ {
			if sid == uchm.pbftNode.ShardID {
				continue
			}
			sii := message.SeqIDinfo{
				SenderShardID: uchm.pbftNode.ShardID,
				SenderSeq:     uchm.pbftNode.sequenceID,
			}
			sByte, err := json.Marshal(sii)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CSeqIDinfo, sByte)
			go networks.TcpDial(msg_send, uchm.pbftNode.ip_nodeTable[sid][0])
			uchm.pbftNode.pl.Plog.Printf("S%dN%d : sended sequence ids to %d\n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID, sid)
		}
		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   innerShardTxs,
			Epoch:           int(uchm.cdm.AccountTransferRound),

			Broker1Txs: broker1Txs,
			Broker2Txs: broker2Txs,

			CrossShardFunctionCall: crossShardFunctionCall,
			InnerSCTxs:             innerSCTxs,

			SenderShardID: uchm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, uchm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		uchm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID)

		uchm.pbftNode.CurChain.Txpool.GetLocked()

		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",

			"# of Inner Account Txs in this block",
			"# of Broker1 (ctx) Txs in this block",
			"# of Broker2 (ctx) Txs in this block",

			"# of Cross Shard Function Call Txs in this block",
			"# of Inner SC Txs in this block",

			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Broker1 Txs) (Duration: Broker1 proposed -> Broker1 Commit)",
			"SUM of confirm latency (ms, Broker2 Txs) (Duration: Broker2 proposed -> Broker2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(uchm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),

			strconv.Itoa(len(innerShardTxs)),
			strconv.Itoa(len(broker1Txs)),
			strconv.Itoa(len(broker2Txs)),

			strconv.Itoa(len(crossShardFunctionCall)),
			strconv.Itoa(len(innerSCTxs)),

			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker2Txs, bim.CommitTime), 10),
		}
		uchm.pbftNode.writeCSVline(metricName, metricVal)
		uchm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	return true
}

func (uchm *UnionChainPbftInsideExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (uchm *UnionChainPbftInsideExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		uchm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", uchm.pbftNode.ShardID, uchm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				uchm.pbftNode.CurChain.AddBlock(b)
			} else {
				atm := message.DecodeAccountTransferMsg(r.Msg.Content)
				uchm.accountTransfer_do(atm)
			}
		}
		uchm.pbftNode.sequenceID = som.SeqEndHeight + 1
		uchm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
