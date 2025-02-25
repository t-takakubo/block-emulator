package core

import (
	"blockEmulator/utils"
	"fmt"
	"math/big"
	"strings"
	"time"
)

type InternalTransaction struct {
	Sender       utils.Address
	Recipient    utils.Address
	Value        *big.Int
	ParentTxHash string

	Time time.Time // TimeStamp the tx proposed.

	// used in transaction relaying
	Relayed bool

	SenderIsContract    bool
	RecipientIsContract bool

	TypeTraceAddress string
	CallType         string // ex: "call", "staticcall", "delegatecall", "create"
}

func (itx *InternalTransaction) PrintTx() string {
	vals := []interface{}{
		itx.Sender[:],
		itx.Recipient[:],
		itx.Value,
		string(itx.ParentTxHash[:]),
	}
	res := fmt.Sprintf("%v\n", vals)
	return res
}

// new a internal transaction
func NewInternalTransaction(sender, recipient, parentTxHash, typeTraceAddress string, value *big.Int, nonce uint64, proposeTime time.Time, senderIsContract, recipientIsContract bool) *InternalTransaction {
	itx := &InternalTransaction{
		Sender:              sender,
		Recipient:           recipient,
		Value:               value,
		ParentTxHash:        parentTxHash,
		Time:                proposeTime,
		SenderIsContract:    senderIsContract,
		RecipientIsContract: recipientIsContract,
		TypeTraceAddress:    typeTraceAddress,
		CallType:            strings.Split(typeTraceAddress, "_")[0],
	}

	return itx
}
