package measure

type SCTxResultInfo struct {
	TxHash2SCTxInfo map[string]*SCTxInfo
	CrossShardTxNum int
	InnerSCTxNum    int
	TotalSCTxNum    int
}

type SCTxInfo struct {
	IsCorssShardTx bool
	IsInnerSCTx    bool
}

func NewSCTxResultInfo() *SCTxResultInfo {
	return &SCTxResultInfo{
		TxHash2SCTxInfo: make(map[string]*SCTxInfo),
	}
}

func (sct *SCTxResultInfo) UpdateSCTxInfo(txHash string, isCrossShardTx, isInnerSCTx bool) {
	if scInfo, ok := sct.TxHash2SCTxInfo[txHash]; !ok {
		sct.TxHash2SCTxInfo[txHash] = &SCTxInfo{
			IsCorssShardTx: isCrossShardTx,
			IsInnerSCTx:    isInnerSCTx,
		}

		sct.TotalSCTxNum++

		if isCrossShardTx {
			sct.CrossShardTxNum++
		}

		if isInnerSCTx {
			sct.InnerSCTxNum++
		}
	} else {
		if scInfo.IsCorssShardTx == isInnerSCTx {
			sct.CrossShardTxNum--
			sct.InnerSCTxNum++
		}

		if scInfo.IsInnerSCTx == isCrossShardTx {
			sct.InnerSCTxNum--
			sct.CrossShardTxNum++
		}

		scInfo.IsCorssShardTx = isCrossShardTx
		scInfo.IsInnerSCTx = isInnerSCTx
	}
}

func (sct *SCTxResultInfo) GetCrossShardSCTxNum() int {
	return sct.CrossShardTxNum
}

func (sct *SCTxResultInfo) GetInnerSCTxNum() int {
	return sct.InnerSCTxNum
}

func (sct *SCTxResultInfo) GetTotalSCTxNum() int {
	return sct.TotalSCTxNum
}
