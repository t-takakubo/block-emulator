package measure

import (
	"blockEmulator/message"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"time"
)

type txMetricDetailTime struct {
	// normal tx time
	TxProposeTimestamp, BlockProposeTimestamp, TxCommitTimestamp time.Time

	// relay tx time
	Relay1CommitTimestamp, Relay2CommitTimestamp time.Time

	// broker tx time
	Broker1CommitTimestamp, Broker2CommitTimestamp time.Time

	// sc tx time
	CrossFunctionCallCommitTimestamp, InnerSCCommitTimestamp time.Time

	isCrossShardTx, isInnerSCTx bool
}

// to test Tx detail
type TestTxDetail struct {
	txHash2DetailTime map[string]*txMetricDetailTime

	normalTxNum int
	relay1TxNum int
	relay2TxNum int

	broker1TxNum int
	broker2TxNum int

	crossShardFunctionCallTxNum int
	innerSCTxNum                int

	lock sync.Mutex
}

func NewTestTxDetail() *TestTxDetail {
	return &TestTxDetail{
		txHash2DetailTime: make(map[string]*txMetricDetailTime),
	}
}

func (ttd *TestTxDetail) OutputMetricName() string {
	return "Tx_Details"
}

func (ttd *TestTxDetail) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}

	ttd.lock.Lock()
	defer ttd.lock.Unlock()

	for _, innertx := range b.InnerShardTxs {
		if _, ok := ttd.txHash2DetailTime[string(innertx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(innertx.TxHash)] = &txMetricDetailTime{}
			ttd.normalTxNum++
		}
		ttd.txHash2DetailTime[string(innertx.TxHash)].TxProposeTimestamp = innertx.Time
		ttd.txHash2DetailTime[string(innertx.TxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(innertx.TxHash)].TxCommitTimestamp = b.CommitTime
	}

	for _, r1tx := range b.Relay1Txs {
		if _, ok := ttd.txHash2DetailTime[string(r1tx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(r1tx.TxHash)] = &txMetricDetailTime{}
		}

		ttd.relay1TxNum++
		ttd.txHash2DetailTime[string(r1tx.TxHash)].TxProposeTimestamp = r1tx.Time
		ttd.txHash2DetailTime[string(r1tx.TxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(r1tx.TxHash)].Relay1CommitTimestamp = b.CommitTime
	}

	for _, r2tx := range b.Relay2Txs {
		if _, ok := ttd.txHash2DetailTime[string(r2tx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(r2tx.TxHash)] = &txMetricDetailTime{}
		}
		ttd.relay2TxNum++
		ttd.txHash2DetailTime[string(r2tx.TxHash)].Relay2CommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(r2tx.TxHash)].TxCommitTimestamp = b.CommitTime
	}

	for _, b1tx := range b.Broker1Txs {
		if _, ok := ttd.txHash2DetailTime[string(b1tx.RawTxHash)]; !ok {
			ttd.txHash2DetailTime[string(b1tx.RawTxHash)] = &txMetricDetailTime{}
			ttd.broker1TxNum++
		}
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].TxProposeTimestamp = b1tx.Time
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].Broker1CommitTimestamp = b.CommitTime
	}

	for _, b2tx := range b.Broker2Txs {
		if _, ok := ttd.txHash2DetailTime[string(b2tx.RawTxHash)]; !ok {
			ttd.txHash2DetailTime[string(b2tx.RawTxHash)] = &txMetricDetailTime{}
			ttd.broker2TxNum++
		}
		ttd.txHash2DetailTime[string(b2tx.RawTxHash)].Broker2CommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(b2tx.RawTxHash)].TxCommitTimestamp = b.CommitTime

		ttd.broker2TxNum++
	}

	// add code
	for _, csfc := range b.CrossShardFunctionCall {
		if _, ok := ttd.txHash2DetailTime[string(csfc.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(csfc.TxHash)] = &txMetricDetailTime{}
			ttd.txHash2DetailTime[string(csfc.TxHash)].isCrossShardTx = true
			ttd.crossShardFunctionCallTxNum++
		}

		if ttd.txHash2DetailTime[string(csfc.TxHash)].isInnerSCTx {
			ttd.crossShardFunctionCallTxNum++
			ttd.innerSCTxNum--
		}

		ttd.txHash2DetailTime[string(csfc.TxHash)].TxProposeTimestamp = csfc.Time
		ttd.txHash2DetailTime[string(csfc.TxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(csfc.TxHash)].TxCommitTimestamp = b.CommitTime
	}

	for _, innerSCtx := range b.InnerSCTxs {
		if _, ok := ttd.txHash2DetailTime[string(innerSCtx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(innerSCtx.TxHash)] = &txMetricDetailTime{}
			ttd.txHash2DetailTime[string(innerSCtx.TxHash)].isInnerSCTx = true
			ttd.innerSCTxNum++
		}

		if ttd.txHash2DetailTime[string(innerSCtx.TxHash)].isCrossShardTx {
			ttd.crossShardFunctionCallTxNum--
			ttd.innerSCTxNum++
		}

		ttd.txHash2DetailTime[string(innerSCtx.TxHash)].TxProposeTimestamp = innerSCtx.Time
		ttd.txHash2DetailTime[string(innerSCtx.TxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(innerSCtx.TxHash)].TxCommitTimestamp = b.CommitTime
	}
}

func (ttd *TestTxDetail) HandleExtraMessage([]byte) {}

func (ttd *TestTxDetail) OutputRecord() (perEpochCTXs []float64, totTxNum float64) {
	ttd.writeToCSV()
	fmt.Println("[Normal, Relay1, Relay2, Broker1, Broker2, CrossShardFunctionCall, InnerSC], Total Tx")
	txNumEachType := []float64{
		float64(ttd.normalTxNum),
		float64(ttd.relay1TxNum),
		float64(ttd.relay2TxNum),
		float64(ttd.broker1TxNum),
		float64(ttd.broker2TxNum),
		float64(ttd.crossShardFunctionCallTxNum),
		float64(ttd.innerSCTxNum),
	}

	return txNumEachType, float64(len(ttd.txHash2DetailTime))
}

func (ttd *TestTxDetail) writeToCSV() {
	fileName := ttd.OutputMetricName()
	measureName := []string{
		"TxHash (Byte -> Big Int)",
		"Tx propose timestamp",
		"Block propose timestamp",
		"Tx finally commit timestamp",
		"Relay1 Tx commit timestamp (not a relay tx -> nil)",
		"Relay2 Tx commit timestamp (not a relay tx -> nil)",
		"Broker1 Tx commit timestamp (not a broker tx -> nil)",
		"Broker2 Tx commit timestamp (not a broker tx -> nil)",
		"CrosShardFunctionCall Tx commit timestamp (not a cross shard tx -> nil)",
		"InnerSCTx Tx commit timestamp (not a inner sc tx -> nil)",
		"Confirmed latency of this tx (ms)",
	}
	measureVals := make([][]string, 0)

	for key, val := range ttd.txHash2DetailTime {
		csvLine := []string{
			new(big.Int).SetBytes([]byte(key)).String(),

			timestampToString(val.TxProposeTimestamp),
			timestampToString(val.BlockProposeTimestamp),
			timestampToString(val.TxCommitTimestamp),

			timestampToString(val.Relay1CommitTimestamp),
			timestampToString(val.Relay2CommitTimestamp),

			timestampToString(val.Broker1CommitTimestamp),
			timestampToString(val.Broker2CommitTimestamp),

			timestampToString(val.CrossFunctionCallCommitTimestamp),
			timestampToString(val.InnerSCCommitTimestamp),

			strconv.FormatInt(int64(val.TxCommitTimestamp.Sub(val.TxProposeTimestamp).Milliseconds()), 10),
		}
		measureVals = append(measureVals, csvLine)
	}

	WriteMetricsToCSV(fileName, measureName, measureVals)
}

// zero time to empty string
func timestampToString(thisTime time.Time) string {
	if thisTime.IsZero() {
		return ""
	}
	return strconv.FormatInt(thisTime.UnixMilli(), 10)
}
