package message

import (
	"blockEmulator/core"
	"math/big"
)

type CrossShardFunctionRequest struct {
	RequestID          string // ID to identify the request
	OriginalSender     string
	SourceShardID      uint64
	DestinationShardID uint64
	Sender             string   // Address of the request sender
	Recepient          string   // Address of the contract receiving the request
	Value              *big.Int // Amount to be transferred
	Arguments          []byte   // Arguments to be passed to the method
	Timestamp          int64    // Timestamp when the request was made
	TypeTraceAddress   string
	Tx                 *core.Transaction
	ProcessedMap       map[string]bool // Key: TypeTraceAddress, Value: Whether it has been processed
}

type CrossShardFunctionResponse struct {
	ResponseID         string // ID corresponding to the response
	OriginalSender     string
	SourceShardID      uint64   // Shard ID sending the response
	DestinationShardID uint64   // Shard ID receiving the response (original request shard)
	Sender             string   // Address of the response sender
	Recipient          string   // Address of the contract receiving the response
	Value              *big.Int // Amount to be transferred
	ResultData         []byte   // Data of the processing result
	Timestamp          int64    // Timestamp when the response was generated
	TypeTraceAddress   string
	Tx                 *core.Transaction
	ProcessedMap       map[string]bool // Key: TypeTraceAddress, Value: Whether it has been processed
}
