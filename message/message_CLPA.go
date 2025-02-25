package message

import (
	"blockEmulator/core"
	"blockEmulator/partition"
	"bytes"
	"encoding/gob"
	"log"
	"time"
)

var (
	StartCLPA           MessageType = "StartCLPA"
	AccountState_and_TX MessageType = "AccountState&txs"
	PartitionReq        RequestType = "PartitionReq"
	CPartitionMsg       MessageType = "PartitionModifiedMap"
	CPartitionReady     MessageType = "ready for partition"
)

type PartitionModifiedMap struct {
	PartitionModified map[string]uint64
	MergedContracts   map[string]partition.Vertex // key: address, value: mergedVertex
}

type AccountTransferMsg struct {
	ModifiedMap     map[string]uint64
	MergedContracts map[string]partition.Vertex // key: address, value: mergedVertex
	Addrs           []string
	AccountState    []*core.AccountState
	ATid            uint64
}

type PartitionReady struct {
	FromShard uint64
	NowSeqID  uint64
}

// this message used in inter-shard, it will be sent between leaders.
type AccountStateAndTx struct {
	Addrs        []string
	AccountState []*core.AccountState
	Txs          []*core.Transaction
	Requests     []*CrossShardFunctionRequest
	Responses    []*CrossShardFunctionResponse
	FromShard    uint64
}

type CLPAResult struct {
	EpochID           int
	CrossShardEdgeNum int
	VertexsNumInShard []int
	VertexNum         int
	TotalVertexNum    int
	Edges2Shard       []int
	MergedVertexNum   int
	MergedContractNum int
	ExecutionTime     time.Duration
}

func (atm *AccountTransferMsg) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(atm)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeAccountTransferMsg(content []byte) *AccountTransferMsg {
	var atm AccountTransferMsg

	decoder := gob.NewDecoder(bytes.NewReader(content))
	err := decoder.Decode(&atm)
	if err != nil {
		log.Panic(err)
	}

	return &atm
}
