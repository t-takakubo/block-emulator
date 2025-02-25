package dataSupport

import (
	"fmt"
	"sync"
)

// SmartContractLockManager manages locks for smart contract accounts based on contract addresses and request IDs.
type SmartContractLockManager struct {
	// Maps contract addresses to accounts and their associated request IDs
	accountLocks map[string]map[string]string // contractAddress -> account -> requestID
	// Maps request IDs to associated contract addresses and accounts
	requestIDToAccounts map[string]map[string]string // requestID -> contractAddress -> account
	mutex               sync.Mutex
}

// NewSmartContractLockManager initializes and returns a new instance of SmartContractLockManager.
func NewSmartContractLockManager() *SmartContractLockManager {
	return &SmartContractLockManager{
		accountLocks:        make(map[string]map[string]string),
		requestIDToAccounts: make(map[string]map[string]string),
	}
}

// LockAccount locks an account under a specific smart contract for a given request ID.
func (lm *SmartContractLockManager) LockAccount(contractAddress, account, requestID string) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Ensure the map for the contract address exists
	if _, exists := lm.accountLocks[contractAddress]; !exists {
		lm.accountLocks[contractAddress] = make(map[string]string)
	}

	// Check if the account is already locked
	if existingRequestID, locked := lm.accountLocks[contractAddress][account]; locked && existingRequestID != requestID {
		return fmt.Errorf("account %s under contract %s is already locked by requestID %s", account, contractAddress, existingRequestID)
	}

	// Lock the account for the given request ID
	lm.accountLocks[contractAddress][account] = requestID

	// Ensure the map for the request ID exists
	if _, exists := lm.requestIDToAccounts[requestID]; !exists {
		lm.requestIDToAccounts[requestID] = make(map[string]string)
	}

	// Map the contract address to the account for the given request ID
	lm.requestIDToAccounts[requestID][contractAddress] = account

	return nil
}

// UnlockAccount unlocks a specific account under a smart contract.
func (lm *SmartContractLockManager) UnlockAccount(contractAddress, account, requestID string) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Check if the account exists under the contract address
	if accounts, exists := lm.accountLocks[contractAddress]; exists {
		// Check if the account is locked by the same request ID
		if lockedRequestID, locked := accounts[account]; locked && lockedRequestID == requestID {
			// Unlock the account
			delete(accounts, account)
			// Clean up the contract entry if no accounts remain locked
			if len(accounts) == 0 {
				delete(lm.accountLocks, contractAddress)
			}

			// Remove the account from requestIDToAccounts
			if requestAccounts, exists := lm.requestIDToAccounts[requestID]; exists {
				delete(requestAccounts, contractAddress)
				if len(requestAccounts) == 0 {
					delete(lm.requestIDToAccounts, requestID)
				}
			}

			return nil
		}
	}
	return fmt.Errorf("no lock found for account %s under contract %s with requestID %s", account, contractAddress, requestID)
}

// UnlockAllByRequestID unlocks all accounts associated with a specific request ID.
func (lm *SmartContractLockManager) UnlockAllByRequestID(requestID string) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Get the list of accounts to unlock for the given request ID
	accountsToUnlock, exists := lm.requestIDToAccounts[requestID]
	if !exists {
		return fmt.Errorf("no locks found for requestID %s", requestID)
	}

	// Unlock all accounts
	for contractAddress, account := range accountsToUnlock {
		delete(lm.accountLocks[contractAddress], account)
		// Remove the contract entry if no accounts remain locked
		if len(lm.accountLocks[contractAddress]) == 0 {
			delete(lm.accountLocks, contractAddress)
		}
	}

	// Remove the request ID entry
	delete(lm.requestIDToAccounts, requestID)

	return nil
}

// IsAccountLocked checks if a specific account under a contract is locked,
// but considers it unlocked if the provided requestID matches the lock's requestID.
func (lm *SmartContractLockManager) IsAccountLocked(contractAddress, account, requestID string) bool {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	if accounts, exists := lm.accountLocks[contractAddress]; exists {
		if lockedRequestID, locked := accounts[account]; locked {
			// Return true if requestID does not match, false otherwise
			return lockedRequestID != requestID
		}
	}
	return false
}

// DebugPrint prints the current state of locks (for debugging purposes).
func (lm *SmartContractLockManager) DebugPrint() {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	fmt.Println("Current SmartContractLockManager State:")
	for contractAddress, accounts := range lm.accountLocks {
		fmt.Printf("  Contract %s:\n", contractAddress)
		for account, requestID := range accounts {
			fmt.Printf("    Account %s -> RequestID %s\n", account, requestID)
		}
	}
	for requestID, entries := range lm.requestIDToAccounts {
		fmt.Printf("  RequestID %s:\n", requestID)
		for contractAddress, account := range entries {
			fmt.Printf("    Contract %s, Account %s\n", contractAddress, account)
		}
	}
}
