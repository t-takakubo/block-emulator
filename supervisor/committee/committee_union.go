package committee

import (
	"blockEmulator/broker"
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// CLPA committee operations
type UnionChainCommitteeModule struct {
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
	modifiedMap             map[string]uint64
	clpaLastRunningTime     time.Time
	clpaFreq                int
	IsCLPAExecuted          map[string]bool // txhash -> bool
	MergedContracts         map[string]partition.Vertex
	ReversedMergedContracts map[partition.Vertex][]string
	UnionFind               *partition.UnionFind

	//broker related  attributes avatar
	broker             *broker.Broker
	brokerConfirm1Pool map[string]*message.Mag1Confirm
	brokerConfirm2Pool map[string]*message.Mag2Confirm
	brokerTxPool       []*core.Transaction
	brokerModuleLock   sync.Mutex

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// smart contract internal transaction
	internalTxMap map[string][]*core.InternalTransaction
}

func NewUnionChainCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath, internalTxCsvPath string, dataNum, batchNum, clpaFrequency int) *UnionChainCommitteeModule {
	cg := new(partition.CLPAState)
	cg.Init_CLPAState(0.5, params.CLPAIterationNum, params.ShardNum) // argument (WeightPenalty, MaxIterations, ShardNum)

	broker := new(broker.Broker)
	broker.NewBroker(nil)

	uccm := &UnionChainCommitteeModule{
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

		brokerConfirm1Pool: make(map[string]*message.Mag1Confirm),
		brokerConfirm2Pool: make(map[string]*message.Mag2Confirm),
		brokerTxPool:       make([]*core.Transaction, 0),
		broker:             broker,
	}

	uccm.internalTxMap = LoadInternalTxsFromCSV(uccm.internalTxCsvPath)
	uccm.sl.Slog.Println("Complete Loading Internal transactions")

	return uccm
}

// for CLPA_Broker committee, it only handle the extra CInner2CrossTx message.
func (uccm *UnionChainCommitteeModule) HandleOtherMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	if msgType != message.CInner2CrossTx {
		return
	}
	itct := new(message.InnerTx2CrossTx)
	err := json.Unmarshal(content, itct)
	if err != nil {
		log.Panic()
	}
	itxs := uccm.dealTxByBroker(itct.Txs)
	uccm.txSending(itxs)
}

// get shard id by address
func (uccm *UnionChainCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := uccm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (uccm *UnionChainCommitteeModule) txSending(txlist []*core.Transaction) {
	sendToShard := make(map[uint64][]*core.Transaction)         // transfer txs
	contractSendToShard := make(map[uint64][]*core.Transaction) // smart contract txs

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			uccm.sendInjectTransactions(sendToShard)
			uccm.sendContractInjectTransactions(contractSendToShard)

			sendToShard = make(map[uint64][]*core.Transaction)
			contractSendToShard = make(map[uint64][]*core.Transaction)

			time.Sleep(time.Second)
		}

		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		uccm.clpaLock.Lock()
		sendersid := uccm.fetchModifiedMap(tx.Sender)

		if tx.HasContract {
			contractSendToShard[sendersid] = append(contractSendToShard[sendersid], tx)
		} else {
			if uccm.broker.IsBroker(tx.Sender) {
				sendersid = uccm.fetchModifiedMap(tx.Recipient)
			}
			sendToShard[sendersid] = append(sendToShard[sendersid], tx)
		}
		uccm.clpaLock.Unlock()
	}

	uccm.sl.Slog.Println(len(txlist), "txs have been sent.")
}

func (uccm *UnionChainCommitteeModule) sendInjectTransactions(sendToShard map[uint64][]*core.Transaction) {
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
		go networks.TcpDial(sendMsg, uccm.IpNodeTable[sid][0])
	}
}

func (uccm *UnionChainCommitteeModule) sendContractInjectTransactions(contractSendToShard map[uint64][]*core.Transaction) {
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
		go networks.TcpDial(sendMsg, uccm.IpNodeTable[sid][0])
	}
}

func (uccm *UnionChainCommitteeModule) data2txWithContract(data []string, nonce uint64) (*core.Transaction, bool) {
	// data[2]: txHash
	// data[3]: from address e.g. 0x1234567890abcdef1234567890abcdef12345678
	// data[4]: to address e.g. 0x1234567890abcdef1234567890abcdef12345678
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
		if internalTxs, ok := uccm.internalTxMap[txHash]; ok {
			tx.InternalTxs = internalTxs
			delete(uccm.internalTxMap, txHash)
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

func (uccm *UnionChainCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(uccm.csvPath)
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
		if tx, ok := uccm.data2txWithContract(data, uint64(uccm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			uccm.nowDataNum++
		} else {
			continue
		}

		// batch sending condition
		if len(txlist) == int(uccm.batchDataNum) || uccm.nowDataNum == uccm.dataTotalNum {
			// set the algorithm timer begins
			if uccm.clpaLastRunningTime.IsZero() {
				uccm.clpaLastRunningTime = time.Now()
			}

			innerTxs := uccm.dealTxByBroker(txlist)

			uccm.txSending(innerTxs)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			uccm.Ss.StopGap_Reset()
		}

		if params.ShardNum > 1 && !uccm.clpaLastRunningTime.IsZero() && time.Since(uccm.clpaLastRunningTime) >= time.Duration(uccm.clpaFreq)*time.Second {
			uccm.clpaLock.Lock()
			clpaCnt++

			emptyData := []byte{}
			if err != nil {
				log.Panic()
			}
			startCLPA_msg := message.MergeMessage(message.StartCLPA, emptyData)
			// send to worker shards
			for i := uint64(0); i < uint64(params.ShardNum); i++ {
				go networks.TcpDial(startCLPA_msg, uccm.IpNodeTable[i][0])
			}

			uccm.sl.Slog.Println("Start CLPA Partitioning")
			mmap, _ := uccm.ClpaGraph.CLPA_Partition()

			uccm.MergedContracts = uccm.ClpaGraph.UnionFind.GetParentMap()
			uccm.ReversedMergedContracts = pbft_all.ReverseMap(uccm.MergedContracts)
			uccm.UnionFind = uccm.ClpaGraph.UnionFind

			uccm.clpaMapSend(mmap)
			for key, val := range mmap {
				uccm.modifiedMap[key] = val
			}

			uccm.clpaReset()
			uccm.clpaLock.Unlock()

			for atomic.LoadInt32(&uccm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}

			uccm.clpaLastRunningTime = time.Now()
			uccm.sl.Slog.Println("Next CLPA epoch begins. ")
		}

		if uccm.nowDataNum == uccm.dataTotalNum {
			uccm.sl.Slog.Println("All txs have been sent!!!")
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !uccm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if time.Since(uccm.clpaLastRunningTime) >= time.Duration(uccm.clpaFreq)*time.Second {
			uccm.clpaLock.Lock()
			clpaCnt++

			emptyData := []byte{}
			if err != nil {
				log.Panic()
			}
			startCLPA_msg := message.MergeMessage(message.StartCLPA, emptyData)
			// send to worker shards
			for i := uint64(0); i < uint64(params.ShardNum); i++ {
				go networks.TcpDial(startCLPA_msg, uccm.IpNodeTable[i][0])
			}

			uccm.sl.Slog.Println("Start CLPA Partitioning")

			mmap, _ := uccm.ClpaGraph.CLPA_Partition()

			uccm.MergedContracts = uccm.ClpaGraph.UnionFind.GetParentMap()
			uccm.ReversedMergedContracts = pbft_all.ReverseMap(uccm.MergedContracts)
			uccm.UnionFind = uccm.ClpaGraph.UnionFind

			uccm.clpaMapSend(mmap)
			for key, val := range mmap {
				uccm.modifiedMap[key] = val
			}

			uccm.clpaReset()
			uccm.clpaLock.Unlock()

			for atomic.LoadInt32(&uccm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}

			uccm.clpaLastRunningTime = time.Now()
			uccm.sl.Slog.Println("Next CLPA epoch begins. ")
		}
	}
}

func (uccm *UnionChainCommitteeModule) clpaMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
		MergedContracts:   uccm.MergedContracts,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, uccm.IpNodeTable[i][0])
	}
	uccm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (uccm *UnionChainCommitteeModule) clpaReset() {
	uccm.ClpaGraphHistory = append(uccm.ClpaGraphHistory, uccm.ClpaGraph)
	uccm.ClpaGraph = new(partition.CLPAState)
	uccm.ClpaGraph.Init_CLPAState(0.5, params.CLPAIterationNum, params.ShardNum)
	for key, val := range uccm.modifiedMap {
		uccm.ClpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
	uccm.ClpaGraph.UnionFind = uccm.UnionFind
}

// handle block information when received CBlockInfo message (pbft node commited)
func (uccm *UnionChainCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	uccm.sl.Slog.Printf("received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	IsChangeEpoch := false

	if atomic.CompareAndSwapInt32(&uccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		uccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
		IsChangeEpoch = true
	}

	if IsChangeEpoch {
		uccm.updateCLPAResult(b)
	}

	if b.BlockBodyLength == 0 {
		return
	}

	// add createConfirm
	txs := make([]*core.Transaction, 0)
	txs = append(txs, b.Broker1Txs...)
	txs = append(txs, b.Broker2Txs...)
	go uccm.createConfirm(txs)

	uccm.clpaLock.Lock()
	defer uccm.clpaLock.Unlock()

	uccm.processTransactions(b.InnerShardTxs)
	uccm.processTransactions(b.Broker1Txs)
	uccm.processTransactions(b.CrossShardFunctionCall)
	uccm.processTransactions(b.InnerSCTxs)
}

func (uccm *UnionChainCommitteeModule) updateCLPAResult(b *message.BlockInfoMsg) {
	if b.CLPAResult == nil {
		b.CLPAResult = &partition.CLPAState{}
	}
	b.CLPAResult = uccm.ClpaGraphHistory[b.Epoch-1]
}

func (uccm *UnionChainCommitteeModule) processTransactions(txs []*core.Transaction) {
	skipCount := 0
	for _, tx := range txs {
		if _, ok := uccm.IsCLPAExecuted[string(tx.TxHash)]; !ok {
			uccm.IsCLPAExecuted[string(tx.TxHash)] = true
		} else {
			skipCount++
			continue
		}

		recepient := partition.Vertex{Addr: uccm.ClpaGraph.UnionFind.Find(tx.Recipient)}
		sender := partition.Vertex{Addr: uccm.ClpaGraph.UnionFind.Find(tx.Sender)}

		uccm.ClpaGraph.AddEdge(sender, recepient)
		for _, itx := range tx.InternalTxs {
			if uccm.UnionFind.IsConnected(itx.Sender, itx.Recipient) {
				continue
			}
			uccm.processInternalTx(itx)
		}
	}
}

func (uccm *UnionChainCommitteeModule) processInternalTx(itx *core.InternalTransaction) {
	itxSender := partition.Vertex{Addr: uccm.ClpaGraph.UnionFind.Find(itx.Sender)}
	itxRecipient := partition.Vertex{Addr: uccm.ClpaGraph.UnionFind.Find(itx.Recipient)}

	uccm.ClpaGraph.AddEdge(itxSender, itxRecipient)

	if params.IsMerge == 1 && itx.SenderIsContract && itx.RecipientIsContract {
		uccm.ClpaGraph.MergeContracts(partition.Vertex{Addr: itx.Sender}, partition.Vertex{Addr: itx.Recipient})
	}
}

func (uccm *UnionChainCommitteeModule) createConfirm(txs []*core.Transaction) {
	confirm1s := make([]*message.Mag1Confirm, 0)
	confirm2s := make([]*message.Mag2Confirm, 0)
	uccm.brokerModuleLock.Lock()
	for _, tx := range txs {
		if confirm1, ok := uccm.brokerConfirm1Pool[string(tx.TxHash)]; ok {
			confirm1s = append(confirm1s, confirm1)
		}
		if confirm2, ok := uccm.brokerConfirm2Pool[string(tx.TxHash)]; ok {
			confirm2s = append(confirm2s, confirm2)
		}
	}
	uccm.brokerModuleLock.Unlock()

	if len(confirm1s) != 0 {
		uccm.handleTx1ConfirmMag(confirm1s)
	}

	if len(confirm2s) != 0 {
		uccm.handleTx2ConfirmMag(confirm2s)
	}
}

func (uccm *UnionChainCommitteeModule) dealTxByBroker(txs []*core.Transaction) (innerTxs []*core.Transaction) {
	innerTxs = make([]*core.Transaction, 0)
	brokerRawMegs := make([]*message.BrokerRawMeg, 0)
	for _, tx := range txs {
		if !tx.HasContract {
			uccm.clpaLock.Lock()
			rSid := uccm.fetchModifiedMap(tx.Recipient)
			sSid := uccm.fetchModifiedMap(tx.Sender)
			uccm.clpaLock.Unlock()
			if rSid != sSid && !uccm.broker.IsBroker(tx.Recipient) && !uccm.broker.IsBroker(tx.Sender) {
				brokerRawMeg := &message.BrokerRawMeg{
					Tx:     tx,
					Broker: uccm.broker.BrokerAddress[0],
				}
				brokerRawMegs = append(brokerRawMegs, brokerRawMeg)
			} else {
				if uccm.broker.IsBroker(tx.Recipient) || uccm.broker.IsBroker(tx.Sender) {
					tx.HasBroker = true
					tx.SenderIsBroker = uccm.broker.IsBroker(tx.Sender)
				}
				innerTxs = append(innerTxs, tx)
			}
		}
		if tx.HasContract {
			// Smart contract transaction
			innerTxs = append(innerTxs, tx)
		}
	}
	if len(brokerRawMegs) != 0 {
		// Cross shard transaction
		uccm.handleBrokerRawMag(brokerRawMegs)
	}
	return innerTxs
}

func (uccm *UnionChainCommitteeModule) handleBrokerType1Mes(brokerType1Megs []*message.BrokerType1Meg) {
	tx1s := make([]*core.Transaction, 0)
	for _, brokerType1Meg := range brokerType1Megs {
		ctx := brokerType1Meg.RawMeg.Tx
		tx1 := core.NewTransaction(ctx.Sender, brokerType1Meg.Broker, ctx.Value, ctx.Nonce, time.Now())
		tx1.OriginalSender = ctx.Sender
		tx1.FinalRecipient = ctx.Recipient
		tx1.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx1.RawTxHash, ctx.TxHash)
		tx1s = append(tx1s, tx1)
		confirm1 := &message.Mag1Confirm{
			RawMeg:  brokerType1Meg.RawMeg,
			Tx1Hash: tx1.TxHash,
		}
		uccm.brokerModuleLock.Lock()
		uccm.brokerConfirm1Pool[string(tx1.TxHash)] = confirm1
		uccm.brokerModuleLock.Unlock()
	}
	uccm.txSending(tx1s)
	fmt.Println("BrokerType1Mes received by shard,  add brokerTx1 len ", len(tx1s))
}

func (uccm *UnionChainCommitteeModule) handleBrokerType2Mes(brokerType2Megs []*message.BrokerType2Meg) {
	tx2s := make([]*core.Transaction, 0)
	for _, mes := range brokerType2Megs {
		ctx := mes.RawMeg.Tx
		tx2 := core.NewTransaction(mes.Broker, ctx.Recipient, ctx.Value, ctx.Nonce, time.Now())
		tx2.OriginalSender = ctx.Sender
		tx2.FinalRecipient = ctx.Recipient
		tx2.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx2.RawTxHash, ctx.TxHash)
		tx2s = append(tx2s, tx2)

		confirm2 := &message.Mag2Confirm{
			RawMeg:  mes.RawMeg,
			Tx2Hash: tx2.TxHash,
		}
		uccm.brokerModuleLock.Lock()
		uccm.brokerConfirm2Pool[string(tx2.TxHash)] = confirm2
		uccm.brokerModuleLock.Unlock()
	}
	uccm.txSending(tx2s)
	fmt.Println("broker tx2 add to pool len ", len(tx2s))
}

// get the digest of rawMeg
func (uccm *UnionChainCommitteeModule) getBrokerRawMagDigest(r *message.BrokerRawMeg) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

// handle broker raw message(Cross shard transaction)
func (uccm *UnionChainCommitteeModule) handleBrokerRawMag(brokerRawMags []*message.BrokerRawMeg) {
	b := uccm.broker
	brokerType1Mags := make([]*message.BrokerType1Meg, 0)
	fmt.Println("broker receive ctx ", len(brokerRawMags))
	uccm.brokerModuleLock.Lock()
	for _, meg := range brokerRawMags {
		b.BrokerRawMegs[string(uccm.getBrokerRawMagDigest(meg))] = meg

		brokerType1Mag := &message.BrokerType1Meg{
			RawMeg:   meg,
			Hcurrent: 0,
			Broker:   meg.Broker,
		}
		brokerType1Mags = append(brokerType1Mags, brokerType1Mag)
	}
	uccm.brokerModuleLock.Unlock()
	uccm.handleBrokerType1Mes(brokerType1Mags)
}

func (uccm *UnionChainCommitteeModule) handleTx1ConfirmMag(mag1confirms []*message.Mag1Confirm) {
	brokerType2Mags := make([]*message.BrokerType2Meg, 0)
	b := uccm.broker

	fmt.Println("receive confirm  brokerTx1 len ", len(mag1confirms))
	uccm.brokerModuleLock.Lock()
	for _, mag1confirm := range mag1confirms {
		RawMeg := mag1confirm.RawMeg
		_, ok := b.BrokerRawMegs[string(uccm.getBrokerRawMagDigest(RawMeg))]
		if !ok {
			fmt.Println("raw message is not exited,tx1 confirms failure !")
			continue
		}
		b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)], string(mag1confirm.Tx1Hash))

		brokerType2Mag := &message.BrokerType2Meg{
			Broker: uccm.broker.BrokerAddress[0],
			RawMeg: RawMeg,
		}
		brokerType2Mags = append(brokerType2Mags, brokerType2Mag)
	}
	uccm.brokerModuleLock.Unlock()
	uccm.handleBrokerType2Mes(brokerType2Mags)
}

func (uccm *UnionChainCommitteeModule) handleTx2ConfirmMag(mag2confirms []*message.Mag2Confirm) {
	b := uccm.broker
	fmt.Println("receive confirm  brokerTx2 len ", len(mag2confirms))
	num := 0
	uccm.brokerModuleLock.Lock()
	for _, mag2confirm := range mag2confirms {
		RawMeg := mag2confirm.RawMeg
		b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)], string(mag2confirm.Tx2Hash))
		if len(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)]) == 2 {
			num++
		} else {
			fmt.Println(len(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)]))
		}
	}
	uccm.brokerModuleLock.Unlock()
	fmt.Println("finish ctx with adding tx1 and tx2 to txpool,len", num)
}
