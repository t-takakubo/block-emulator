package build

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func readIpTable(ipTablePath string) map[uint64]map[uint64]string {
	// Read the contents of ipTable.json
	file, err := os.ReadFile(ipTablePath)
	if err != nil {
		// handle error
		fmt.Println(err)
	}
	// Create a map to store the IP addresses
	var ipMap map[uint64]map[uint64]string
	// Unmarshal the JSON data into the map
	err = json.Unmarshal(file, &ipMap)
	if err != nil {
		// handle error
		fmt.Println(err)
	}
	return ipMap
}

// make sure the file is uptodate
var fileUpToDataSet = make(map[string]struct{})

func attachLineToFile(filePath string, line string) error {
	if _, exist := fileUpToDataSet[filePath]; !exist {
		if _, err := os.Stat(filePath); err == nil {
			if delErr := os.Remove(filePath); delErr != nil {
				log.Printf("Failed to remove file %s: %v", filePath, delErr)
			}
		} else if !os.IsNotExist(err) {
			log.Printf("Error checking file existence %s: %v", filePath, err)
		}
		fileUpToDataSet[filePath] = struct{}{}
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.WriteString(line + "\n"); err != nil {
		return err
	}

	return nil
}
