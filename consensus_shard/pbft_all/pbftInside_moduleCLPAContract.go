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

type CLPAContractInsideExtraHandleMod struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
	cfcpm    *dataSupport.CrossFunctionCallPoolManager
}

// propose request with different types
func (cchm *CLPAContractInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	if cchm.cdm.PartitionOn {
		cchm.pbftNode.pl.Plog.Println("Propose the partition block...")
		cchm.sendPartitionReady()
		for !cchm.getPartitionReady() {
			time.Sleep(time.Second)
		}
		// send accounts and txs
		cchm.sendAccounts_and_Txs()
		// propose a partition
		for !cchm.getCollectOver() {
			time.Sleep(time.Second)
		}
		return cchm.proposePartition()
	}

	// ELSE: propose a block
	block := cchm.pbftNode.CurChain.GenerateBlock(int32(cchm.pbftNode.NodeID))
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()
	return true, r

}

// the diy operation in preprepare
func (cchm *CLPAContractInsideExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// judge whether it is a partitionRequest or not
	isPartitionReq := ppmsg.RequestMsg.RequestType == message.PartitionReq

	if isPartitionReq {
		// after some checking
		cchm.pbftNode.pl.Plog.Printf("S%dN%d : a partition block\n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID)
	} else {
		// the request is a block
		if cchm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
			cchm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID)
			return false
		}
	}
	cchm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID)
	cchm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (cchm *CLPAContractInsideExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (cchm *CLPAContractInsideExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r := cchm.pbftNode.requestPool[string(cmsg.Digest)]
	// requestType ...
	if r.RequestType == message.PartitionReq {
		// if a partition Requst ...
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)
		cchm.accountTransfer_do(atm)
		cchm.pbftNode.pl.Plog.Println("complete accountTransfer_do")
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	cchm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID, block.Header.Number, cchm.pbftNode.CurChain.CurrentBlock.Header.Number)
	cchm.pbftNode.CurChain.AddBlock(block)
	cchm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID, block.Header.Number)
	cchm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if cchm.pbftNode.NodeID == uint64(cchm.pbftNode.view.Load()) {
		cchm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send relay txs at height = %d \n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID, block.Header.Number)
		// generate relay pool and collect txs excuted
		cchm.pbftNode.CurChain.Txpool.RelayPool = make(map[uint64][]*core.Transaction)
		innerShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)

		crossShardFunctionCall := make([]*core.Transaction, 0)
		innerSCTxs := make([]*core.Transaction, 0)

		for _, tx := range block.Body {
			if !tx.HasContract {
				ssid := cchm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
				rsid := cchm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
				if !tx.Relayed && ssid != cchm.pbftNode.ShardID {
					cchm.pbftNode.pl.Plog.Printf(
						"[ERROR] Transaction relay status mismatch: expected ShardID=%d, got ssid=%d, rsid=%d. Sender=%s, Recipient=%s, Relayed=%t",
						cchm.pbftNode.ShardID, ssid, rsid, tx.Sender, tx.Recipient, tx.Relayed,
					)
				}
				if tx.Relayed && rsid != cchm.pbftNode.ShardID {
					cchm.pbftNode.pl.Plog.Printf(
						"[ERROR] Transaction relay shard ID mismatch: expected ShardID=%d, got ssid=%d, rsid=%d. Sender=%s, Recipient=%s, Relayed=%t",
						cchm.pbftNode.ShardID, ssid, rsid, tx.Sender, tx.Recipient, tx.Relayed,
					)
				}
				if rsid != cchm.pbftNode.ShardID {
					relay1Txs = append(relay1Txs, tx)
					tx.Relayed = true
					cchm.pbftNode.CurChain.Txpool.AddRelayTx(tx, rsid) // add relay pool
				} else {
					if tx.Relayed {
						relay2Txs = append(relay2Txs, tx)
					} else {
						innerShardTxs = append(innerShardTxs, tx)
					}
				}
			}

			if tx.HasContract {
				if tx.IsCrossShardFuncCall {
					crossShardFunctionCall = append(crossShardFunctionCall, tx)
					cchm.cfcpm.SClock.UnlockAllByRequestID(tx.RequestID)
				} else if tx.IsAllInner {
					innerSCTxs = append(innerSCTxs, tx)
				}
			}
		}

		// send relay txs
		if params.RelayWithMerkleProof == 1 {
			cchm.pbftNode.RelayWithProofSend(block)
		} else {
			cchm.pbftNode.RelayMsgSend()
		}

		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   innerShardTxs,
			Epoch:           int(cchm.cdm.AccountTransferRound),

			Relay1Txs: relay1Txs,
			Relay2Txs: relay2Txs,

			CrossShardFunctionCall: crossShardFunctionCall,
			InnerSCTxs:             innerSCTxs,

			SenderShardID: cchm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, cchm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		cchm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID)

		cchm.pbftNode.CurChain.Txpool.GetLocked()

		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",

			"# of Inner Account Txs in this block",
			"# of Relay1 Txs in this block",
			"# of Relay2 Txs in this block",

			"# of Cross Shard Function Call Txs in this block",
			"# of Inner SC Txs in this block",

			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs) (Duration: Relay1 proposed -> Relay1 Commit)",
			"SUM of confirm latency (ms, Relay2 Txs) (Duration: Relay1 proposed -> Relay2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(cchm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),

			strconv.Itoa(len(innerShardTxs)),
			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),

			strconv.Itoa(len(crossShardFunctionCall)),
			strconv.Itoa(len(innerSCTxs)),

			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		cchm.pbftNode.writeCSVline(metricName, metricVal)
		cchm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	return true
}

func (cchm *CLPAContractInsideExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (cchm *CLPAContractInsideExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		cchm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", cchm.pbftNode.ShardID, cchm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				cchm.pbftNode.CurChain.AddBlock(b)
			} else {
				atm := message.DecodeAccountTransferMsg(r.Msg.Content)
				cchm.accountTransfer_do(atm)
			}
		}
		cchm.pbftNode.sequenceID = som.SeqEndHeight + 1
		cchm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
