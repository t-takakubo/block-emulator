package measure

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"strconv"
	"sync"
	"time"
)

// to test average Transaction_Confirm_Latency (TCL)  in this system
type TestModule_TCL_Relay struct {
	epochID int

	totTxLatencyEpoch     []float64 // record the Transaction_Confirm_Latency in each epoch, only for excuted txs (normal txs & relay2 txs)
	relay1CommitLatency   []int64
	relay2CommitLatency   []int64
	ctxCommitLatency      []int64
	normalTxCommitLatency []int64

	relay1CommitTS map[string]time.Time

	crossShardFunctionCallLatency map[string]float64
	innerSCTxCommitLatency        map[string]float64

	normalTxNum                 []int
	relay1TxNum                 []int
	relay2TxNum                 []int
	crossShardFunctionCallTxNum []int
	innerSCTxNum                []int
	txNum                       []float64 // record the txNumber in each epoch

	lock sync.Mutex
}

func NewTestModule_TCL_Relay() *TestModule_TCL_Relay {
	return &TestModule_TCL_Relay{
		epochID:           -1,
		totTxLatencyEpoch: make([]float64, 0),

		relay1CommitLatency:   make([]int64, 0),
		relay2CommitLatency:   make([]int64, 0),
		normalTxCommitLatency: make([]int64, 0),
		ctxCommitLatency:      make([]int64, 0),

		relay1CommitTS: make(map[string]time.Time),

		crossShardFunctionCallLatency: make(map[string]float64),
		innerSCTxCommitLatency:        make(map[string]float64),

		normalTxNum:                 make([]int, 0),
		relay1TxNum:                 make([]int, 0),
		relay2TxNum:                 make([]int, 0),
		crossShardFunctionCallTxNum: make([]int, 0),
		innerSCTxNum:                make([]int, 0),
		txNum:                       make([]float64, 0),
	}
}

func (tml *TestModule_TCL_Relay) OutputMetricName() string {
	return "Transaction_Confirm_Latency"
}

// modified latency
func (tml *TestModule_TCL_Relay) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}

	tml.lock.Lock()
	defer tml.lock.Unlock()

	epochid := b.Epoch
	mTime := b.CommitTime

	// extend
	for tml.epochID < epochid {
		tml.txNum = append(tml.txNum, 0)
		tml.totTxLatencyEpoch = append(tml.totTxLatencyEpoch, 0)

		tml.relay1CommitLatency = append(tml.relay1CommitLatency, 0)
		tml.relay2CommitLatency = append(tml.relay2CommitLatency, 0)
		tml.normalTxCommitLatency = append(tml.normalTxCommitLatency, 0)
		tml.ctxCommitLatency = append(tml.ctxCommitLatency, 0)

		tml.relay1TxNum = append(tml.relay1TxNum, 0)
		tml.relay2TxNum = append(tml.relay2TxNum, 0)
		tml.normalTxNum = append(tml.normalTxNum, 0)
		tml.crossShardFunctionCallTxNum = append(tml.crossShardFunctionCallTxNum, 0)
		tml.innerSCTxNum = append(tml.innerSCTxNum, 0)

		tml.epochID++
	}

	tml.normalTxNum[epochid] += len(b.InnerShardTxs)
	tml.relay1TxNum[epochid] += len(b.Relay1Txs)
	tml.relay2TxNum[epochid] += len(b.Relay2Txs)

	tml.crossShardFunctionCallTxNum[epochid] += len(b.CrossShardFunctionCall)
	tml.innerSCTxNum[epochid] += len(b.InnerSCTxs)

	tml.txNum[epochid] += float64(len(b.InnerShardTxs)) + float64(len(b.Relay1Txs)+len(b.Relay2Txs))/2
	tml.txNum[epochid] += float64(len(b.CrossShardFunctionCall)) + float64(len(b.InnerSCTxs))

	// relay1 tx
	for _, r1tx := range b.Relay1Txs {
		tml.relay1CommitTS[string(r1tx.TxHash)] = mTime
		tml.relay1CommitLatency[epochid] += int64(mTime.Sub(r1tx.Time).Milliseconds())
	}

	// relay2 tx
	for _, r2tx := range b.Relay2Txs {
		tml.totTxLatencyEpoch[epochid] += mTime.Sub(r2tx.Time).Seconds()

		if r1CommitTime, ok := tml.relay1CommitTS[string(r2tx.TxHash)]; ok {
			tml.relay2CommitLatency[epochid] += int64(mTime.Sub(r1CommitTime).Milliseconds())
			tml.ctxCommitLatency[epochid] += int64(mTime.Sub(r2tx.Time).Milliseconds())
		}
	}

	// normal tx
	for _, ntx := range b.InnerShardTxs {
		tml.totTxLatencyEpoch[epochid] += mTime.Sub(ntx.Time).Seconds()

		tml.normalTxCommitLatency[epochid] += int64(mTime.Sub(ntx.Time).Milliseconds())
	}

	// cross shard function call tx
	for _, cftx := range b.CrossShardFunctionCall {
		txHashStr := string(cftx.TxHash)
		// Update the latest commit time
		if existingLatency, exists := tml.crossShardFunctionCallLatency[txHashStr]; !exists {
			tml.crossShardFunctionCallLatency[txHashStr] = mTime.Sub(cftx.Time).Seconds()
		} else if existingLatency < mTime.Sub(cftx.Time).Seconds() {
			tml.crossShardFunctionCallLatency[txHashStr] = mTime.Sub(cftx.Time).Seconds()
		}
	}

	// inner shard contract tx
	for _, sctx := range b.InnerSCTxs {
		txHashStr := string(sctx.TxHash)
		// Update the latest commit time
		if existingLatency, exists := tml.innerSCTxCommitLatency[txHashStr]; !exists {
			tml.innerSCTxCommitLatency[txHashStr] = mTime.Sub(sctx.Time).Seconds()
		} else if existingLatency < mTime.Sub(sctx.Time).Seconds() {
			tml.innerSCTxCommitLatency[txHashStr] = mTime.Sub(sctx.Time).Seconds()
		}
	}
}

func (tml *TestModule_TCL_Relay) HandleExtraMessage([]byte) {}

func (tml *TestModule_TCL_Relay) OutputRecord() (perEpochLatency []float64, totLatency float64) {
	tml.writeToCSV()

	// calculate the simple result
	perEpochLatency = make([]float64, 0)
	latencySum := 0.0
	totTxNum := 0.0
	for eid, totLatency := range tml.totTxLatencyEpoch {
		perEpochLatency = append(perEpochLatency, totLatency/tml.txNum[eid])
		latencySum += totLatency
		totTxNum += tml.txNum[eid]
	}

	for _, lat := range tml.crossShardFunctionCallLatency {
		latencySum += float64(lat)
	}

	for _, lat := range tml.innerSCTxCommitLatency {
		latencySum += float64(lat)
	}

	totLatency = latencySum / float64(params.TotalDataSize)
	return
}

func (tml *TestModule_TCL_Relay) writeToCSV() {
	fileName := tml.OutputMetricName()
	measureName := []string{
		"EpochID",
		"Total tx # in this epoch",
		"Normal tx # in this epoch",
		"Relay1 tx # in this epoch",
		"Relay2 tx # in this epoch",
		"Sum of Relay1 TCL (ms) (Duration: Relay1 Tx Propose -> Relay1 Tx Commit)",
		"Sum of Relay2 TCL (ms) (Duration: Relay2 Tx Propose -> Relay2 Tx Commit)",
		"Sum of innerShardTx TCL (ms)",
		"Sum of CTX TCL (ms) (Duration: Relay1 Tx Propose -> Relay2 Tx Commit)",
		"Sum of All Tx TCL (sec.)",
	}
	measureVals := make([][]string, 0)

	for eid, totTxInE := range tml.txNum {
		csvLine := []string{
			strconv.Itoa(eid),
			strconv.FormatFloat(totTxInE, 'f', 8, 64),
			strconv.Itoa(tml.normalTxNum[eid]),
			strconv.Itoa(tml.relay1TxNum[eid]),
			strconv.Itoa(tml.relay2TxNum[eid]),
			strconv.FormatInt(tml.relay1CommitLatency[eid], 10),
			strconv.FormatInt(tml.relay2CommitLatency[eid], 10),
			strconv.FormatInt(tml.normalTxCommitLatency[eid], 10),
			strconv.FormatInt(tml.ctxCommitLatency[eid], 10),
			strconv.FormatFloat(tml.totTxLatencyEpoch[eid], 'f', 8, 64),
		}
		measureVals = append(measureVals, csvLine)
	}
	WriteMetricsToCSV(fileName, measureName, measureVals)
}
