package core

import (
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
)

type CallNode struct {
	TypeTraceAddress    string // ex: "0" "0_1", "0_1_0" ...
	CallType            string // ex: "call", "staticcall", "delegatecall", "create"
	Sender              string
	SenderIsContract    bool
	SenderShardID       int
	Recipient           string
	RecipientIsContract bool
	RecipientShardID    int
	Value               *big.Int
	Children            []*CallNode
	Parent              *CallNode `json:"-"`
	IsLeaf              bool
	IsProcessed         bool
}

func BuildExecutionCallTree(tx *Transaction, processedMap map[string]bool) *CallNode {
	root := &CallNode{
		TypeTraceAddress:    "root",
		CallType:            "root",
		Sender:              tx.Sender,
		SenderShardID:       Addr2Shard(tx.Sender),
		SenderIsContract:    false,
		Recipient:           tx.Recipient,
		RecipientIsContract: true,
		RecipientShardID:    Addr2Shard(tx.Recipient),
		Value:               tx.Value,
		IsLeaf:              false,
		IsProcessed:         processedMap["root"],
	}
	nodeMap := make(map[string]*CallNode)
	nodeMap["root"] = root

	for _, itx := range tx.InternalTxs {
		current := root
		parts := strings.Split(itx.TypeTraceAddress, "_")
		numParts := parts[1:] // ex. ["0", "0", "1"]
		currentPath := ""

		for i, part := range numParts {
			if currentPath == "" {
				currentPath = part
			} else {
				currentPath += "_" + part
			}

			if existing, exists := nodeMap[currentPath]; exists {
				current = existing
				if i == len(numParts)-1 {
					current.IsLeaf = true
				}
				continue
			}

			newNode := &CallNode{
				TypeTraceAddress:    currentPath,
				CallType:            itx.CallType,
				Sender:              itx.Sender,
				SenderIsContract:    itx.SenderIsContract,
				SenderShardID:       Addr2Shard(itx.Sender),
				Recipient:           itx.Recipient,
				RecipientIsContract: itx.RecipientIsContract,
				RecipientShardID:    Addr2Shard(itx.Recipient),
				Value:               itx.Value,
				Parent:              current,
				IsLeaf:              true,
				IsProcessed:         processedMap[currentPath],
			}
			current.IsLeaf = false
			current.Children = append(current.Children, newNode)
			nodeMap[currentPath] = newNode
			current = newNode
		}
	}

	return root
}

func Addr2Shard(addr string) int {
	last8_addr := addr
	if len(last8_addr) > 8 {
		last8_addr = last8_addr[len(last8_addr)-8:]
	}
	num, err := strconv.ParseUint(last8_addr, 16, 64)
	if err != nil {
		log.Panic(err)
	}
	return int(num) % 2
}

func (node *CallNode) FindNodeByTTA(target string) *CallNode {
	if node == nil {
		return nil
	}

	if node.TypeTraceAddress == target {
		return node
	}

	for _, child := range node.Children {
		found := child.FindNodeByTTA(target)
		if found != nil {
			return found
		}
	}

	return nil
}

func (node *CallNode) FindParentNodeByTTA(target string) *CallNode {
	if node == nil {
		return nil
	}

	if node.TypeTraceAddress == target {
		return node
	}

	for _, child := range node.Children {
		if child.TypeTraceAddress == target {
			return node
		}

		foundParent := child.FindParentNodeByTTA(target)
		if foundParent != nil {
			return foundParent
		}
	}

	return nil
}

func (node *CallNode) PrintTree(level int) {
	if node == nil {
		return
	}
	indent := strings.Repeat("  ", level)
	fmt.Printf("%s%s_%s Sender: %s %d, Recepient: %s %d IsLeaf: %t IsProceed: %t\n",
		indent, node.CallType, node.TypeTraceAddress, node.Sender, node.SenderShardID, node.Recipient, node.RecipientShardID, node.IsLeaf, node.IsProcessed)
	for _, child := range node.Children {
		child.PrintTree(level + 1)
	}
}
