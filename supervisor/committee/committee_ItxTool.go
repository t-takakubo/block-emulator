package committee

import (
	"blockEmulator/core"
	"encoding/csv"
	"io"
	"log"
	"math/big"
	"os"
	"time"
)

func LoadInternalTxsFromCSV(csvPath string) map[string][]*core.InternalTransaction {
	file, err := os.Open(csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	internalTxMap := make(map[string][]*core.InternalTransaction) // parentTxHash -> []*InternalTransaction

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if len(data) < 9 {
			log.Printf("Skipping row due to insufficient columns: %v", data)
			continue
		}

		parentTxHash := data[2]
		if parentTxHash == "" {
			log.Printf("Skipping row due to empty parentTxHash: %v", data)
			continue
		}

		typeTraceAddress := data[3]
		if typeTraceAddress == "" {
			log.Printf("Skipping row due to empty typeTraceAddress: %v", data)
			continue
		}

		sender := data[4]
		recipient := data[5]
		if len(sender) < 40 || len(recipient) < 40 {
			log.Printf("Skipping row due to invalid sender or recipient: %v", data)
			continue
		}

		senderIsContract := data[6] == "1"
		recipientIsContract := data[7] == "1"
		if data[6] != "1" && data[6] != "0" {
			log.Printf("Skipping row due to invalid senderIsContract: %v", data)
			continue
		}
		if data[7] != "1" && data[7] != "0" {
			log.Printf("Skipping row due to invalid recipientIsContract: %v", data)
			continue
		}

		valueStr := data[8]
		value, ok := new(big.Int).SetString(valueStr, 10)
		if !ok {
			log.Printf("Skipping row due to invalid value: %v", data)
			continue
		}

		nonce := uint64(0)
		internalTx := core.NewInternalTransaction(sender[2:], recipient[2:], parentTxHash, typeTraceAddress, value, nonce, time.Now(), senderIsContract, recipientIsContract)
		internalTxMap[parentTxHash] = append(internalTxMap[parentTxHash], internalTx)
	}
	return internalTxMap
}
