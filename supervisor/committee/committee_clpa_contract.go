package committee

import (
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type CLPAContractCommitteeModule struct {
	csvPath           string
	internalTxCsvPath string
	dataTotalNum      int
	nowDataNum        int
	batchDataNum      int

	// additional variants
	curEpoch                int32
	clpaLock                sync.Mutex
	ClpaGraph               *partition.CLPAState
	ClpaGraphHistory        []*partition.CLPAState
	modifiedMap             map[string]uint64 // key: address, value: shardID
	clpaLastRunningTime     time.Time
	clpaFreq                int
	IsCLPAExecuted          map[string]bool // txhash -> bool
	MergedContracts         map[string]partition.Vertex
	ReversedMergedContracts map[partition.Vertex][]string
	UnionFind               *partition.UnionFind // Union-Find

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// smart contract internal transaction
	internalTxMap map[string][]*core.InternalTransaction
}

func NewCLPAContractCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath, internalTxCsvPath string, dataNum, batchNum, clpaFrequency int) *CLPAContractCommitteeModule {
	cg := new(partition.CLPAState)
	cg.Init_CLPAState(0.5, params.CLPAIterationNum, params.ShardNum) // argument (WeightPenalty, MaxIterations, ShardNum)

	cccm := &CLPAContractCommitteeModule{
		csvPath:             csvFilePath,
		internalTxCsvPath:   internalTxCsvPath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		ClpaGraph:           cg,
		ClpaGraphHistory:    make([]*partition.CLPAState, 0),
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		IsCLPAExecuted:      make(map[string]bool),
		clpaLastRunningTime: time.Time{},
		MergedContracts:     make(map[string]partition.Vertex),
		UnionFind:           partition.NewUnionFind(),
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
		internalTxMap:       make(map[string][]*core.InternalTransaction),
	}

	cccm.internalTxMap = LoadInternalTxsFromCSV(cccm.internalTxCsvPath)
	cccm.sl.Slog.Println("Complete Loading Internal transactions")

	return cccm
}

func (cccm *CLPAContractCommitteeModule) HandleOtherMessage([]byte) {}

func (cccm *CLPAContractCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := cccm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (cccm *CLPAContractCommitteeModule) txSending(txlist []*core.Transaction) {
	sendToShard := make(map[uint64][]*core.Transaction)         // transfer txs
	contractSendToShard := make(map[uint64][]*core.Transaction) // smart contract txs

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			cccm.sendInjectTransactions(sendToShard)
			cccm.sendContractInjectTransactions(contractSendToShard)

			sendToShard = make(map[uint64][]*core.Transaction)
			contractSendToShard = make(map[uint64][]*core.Transaction)

			time.Sleep(time.Second)
		}

		if idx == len(txlist) {
			break
		}

		tx := txlist[idx]
		cccm.clpaLock.Lock()
		sendersid := cccm.fetchModifiedMap(tx.Sender)
		cccm.clpaLock.Unlock()

		if tx.HasContract {
			contractSendToShard[sendersid] = append(contractSendToShard[sendersid], tx)
		} else {
			sendToShard[sendersid] = append(sendToShard[sendersid], tx)
		}
	}

	cccm.sl.Slog.Println(len(txlist), "txs have been sent.")
}

func (cccm *CLPAContractCommitteeModule) sendInjectTransactions(sendToShard map[uint64][]*core.Transaction) {
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
		go networks.TcpDial(sendMsg, cccm.IpNodeTable[sid][0])
	}
}

// `CContractInject` 用のトランザクション送信
func (cccm *CLPAContractCommitteeModule) sendContractInjectTransactions(contractSendToShard map[uint64][]*core.Transaction) {
	for sid, txs := range contractSendToShard {
		cit := message.ContractInjectTxs{
			Txs:       txs,
			ToShardID: sid,
		}
		citByte, err := json.Marshal(cit)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CContractInject, citByte)
		go networks.TcpDial(sendMsg, cccm.IpNodeTable[sid][0])
	}
}

func (cccm *CLPAContractCommitteeModule) data2txWithContract(data []string, nonce uint64) (*core.Transaction, bool) {
	// data[2]: txHash
	// data[3]: from e.g. 0x1234567890abcdef1234567890abcdef12345678
	// data[4]: to e.g. 0x1234567890abcdef1234567890abcdef12345678
	// data[6]: fromIsContract (0: not contract, 1: contract)
	// data[7]: toIsContract (0: not contract, 1: contract)
	// data[8]: value e.g. 1000000000000000000
	txHash := data[2]
	from := data[3]
	to := data[4]
	fromIsContract := data[6] == "1"
	toIsContract := data[7] == "1"
	valueStr := data[8]

	// TX for money transfer between two parties
	if !fromIsContract && !toIsContract && len(from) > 16 && len(to) > 16 && from != to {
		val, ok := new(big.Int).SetString(valueStr, 10)
		if !ok {
			log.Panic("Failed to parse value")
		}
		tx := core.NewTransaction(from[2:], to[2:], val, nonce, time.Now())
		return tx, true
	}

	// TX for smart contract
	if toIsContract && len(from) > 16 && len(to) > 16 && from != to {
		val, ok := new(big.Int).SetString(valueStr, 10)
		if !ok {
			log.Panic("Failed to parse value")
		}
		tx := core.NewTransaction(from[2:], to[2:], val, nonce, time.Now())
		// add internal transactions
		tx.HasContract = true
		if internalTxs, ok := cccm.internalTxMap[txHash]; ok {
			tx.InternalTxs = internalTxs
			delete(cccm.internalTxMap, txHash)
		}

		if len(tx.InternalTxs) > params.SkipThresholdInternalTx {
			if params.IsSkipLongInternalTx == 1 {
				return &core.Transaction{}, false
			}
		}
		return tx, true
	}

	return &core.Transaction{}, false
}

func (cccm *CLPAContractCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(cccm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	clpaCnt := 0
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := cccm.data2txWithContract(data, uint64(cccm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			cccm.nowDataNum++
		} else {
			continue
		}

		// batch sending condition
		if len(txlist) == int(cccm.batchDataNum) || cccm.nowDataNum == cccm.dataTotalNum {
			// set the algorithm timer begins
			if cccm.clpaLastRunningTime.IsZero() {
				cccm.clpaLastRunningTime = time.Now()
			}
			cccm.txSending(txlist)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			cccm.Ss.StopGap_Reset()
		}

		if params.ShardNum > 1 && !cccm.clpaLastRunningTime.IsZero() && time.Since(cccm.clpaLastRunningTime) >= time.Duration(cccm.clpaFreq)*time.Second {
			cccm.clpaLock.Lock()
			clpaCnt++

			cccm.sl.Slog.Println("Start CLPA Partitioning")
			mmap, _ := cccm.ClpaGraph.CLPA_Partition()

			cccm.MergedContracts = cccm.ClpaGraph.UnionFind.GetParentMap()
			cccm.ReversedMergedContracts = pbft_all.ReverseMap(cccm.MergedContracts)
			cccm.UnionFind = cccm.ClpaGraph.UnionFind

			cccm.clpaMapSend(mmap)
			for key, val := range mmap {
				cccm.modifiedMap[key] = val
			}

			cccm.clpaReset()
			cccm.clpaLock.Unlock()

			for atomic.LoadInt32(&cccm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			cccm.clpaLastRunningTime = time.Now()
			cccm.sl.Slog.Println("Next CLPA epoch begins.")
		}

		if cccm.nowDataNum == cccm.dataTotalNum {
			cccm.sl.Slog.Println("All txs have been sent!!!")
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !cccm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if time.Since(cccm.clpaLastRunningTime) >= time.Duration(cccm.clpaFreq)*time.Second {
			cccm.clpaLock.Lock()
			clpaCnt++

			cccm.sl.Slog.Println("Start CLPA Partitioning")
			mmap, _ := cccm.ClpaGraph.CLPA_Partition()

			cccm.MergedContracts = cccm.ClpaGraph.UnionFind.GetParentMap()
			cccm.ReversedMergedContracts = pbft_all.ReverseMap(cccm.MergedContracts)
			cccm.UnionFind = cccm.ClpaGraph.UnionFind

			cccm.clpaMapSend(mmap)
			for key, val := range mmap {
				cccm.modifiedMap[key] = val
			}

			cccm.clpaReset()
			cccm.clpaLock.Unlock()

			for atomic.LoadInt32(&cccm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			cccm.clpaLastRunningTime = time.Now()
			cccm.sl.Slog.Println("Next CLPA epoch begins.")
		}
	}
}

func (cccm *CLPAContractCommitteeModule) clpaMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
		MergedContracts:   cccm.MergedContracts,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, cccm.IpNodeTable[i][0])
	}
	cccm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (cccm *CLPAContractCommitteeModule) clpaReset() {
	cccm.ClpaGraphHistory = append(cccm.ClpaGraphHistory, cccm.ClpaGraph)
	cccm.ClpaGraph = new(partition.CLPAState)
	cccm.ClpaGraph.Init_CLPAState(0.5, params.CLPAIterationNum, params.ShardNum)
	for key, val := range cccm.modifiedMap {
		cccm.ClpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
	cccm.ClpaGraph.UnionFind = cccm.UnionFind
}

func (cccm *CLPAContractCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	cccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	IsChangeEpoch := false
	if atomic.CompareAndSwapInt32(&cccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		cccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
		IsChangeEpoch = true
	}

	if IsChangeEpoch {
		cccm.updateCLPAResult(b)
	}

	if b.BlockBodyLength == 0 {
		return
	}

	cccm.clpaLock.Lock()
	defer cccm.clpaLock.Unlock()

	cccm.processTransactions(b.InnerShardTxs)
	cccm.processTransactions(b.Relay2Txs)
	cccm.processTransactions(b.CrossShardFunctionCall)
	cccm.processTransactions(b.InnerSCTxs)
}

func (cccm *CLPAContractCommitteeModule) updateCLPAResult(b *message.BlockInfoMsg) {
	if b.CLPAResult == nil {
		b.CLPAResult = &partition.CLPAState{}
	}
	b.CLPAResult = cccm.ClpaGraphHistory[b.Epoch-1]
}

func (cccm *CLPAContractCommitteeModule) processTransactions(txs []*core.Transaction) {
	skipCount := 0
	for _, tx := range txs {
		if _, ok := cccm.IsCLPAExecuted[string(tx.TxHash)]; !ok {
			cccm.IsCLPAExecuted[string(tx.TxHash)] = true
		} else {
			skipCount++
			continue
		}

		recepient := partition.Vertex{Addr: cccm.ClpaGraph.UnionFind.Find(tx.Recipient)}
		sender := partition.Vertex{Addr: cccm.ClpaGraph.UnionFind.Find(tx.Sender)}

		cccm.ClpaGraph.AddEdge(sender, recepient)

		for _, itx := range tx.InternalTxs {
			if cccm.UnionFind.IsConnected(itx.Sender, itx.Recipient) {
				continue
			}
			cccm.processInternalTx(itx)
		}
	}
}

func (cccm *CLPAContractCommitteeModule) processInternalTx(itx *core.InternalTransaction) {
	itxSender := partition.Vertex{Addr: cccm.ClpaGraph.UnionFind.Find(itx.Sender)}
	itxRecipient := partition.Vertex{Addr: cccm.ClpaGraph.UnionFind.Find(itx.Recipient)}

	cccm.ClpaGraph.AddEdge(itxSender, itxRecipient)

	if params.IsMerge == 1 && itx.SenderIsContract && itx.RecipientIsContract {
		cccm.ClpaGraph.MergeContracts(partition.Vertex{Addr: itx.Sender}, partition.Vertex{Addr: itx.Recipient})
	}
}
