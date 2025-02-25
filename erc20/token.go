package erc20

import (
	"blockEmulator/params"
	"errors"
	"fmt"
	"math/big"
)

type ERC20Token struct {
	Name       string
	Symbol     string
	Decimals   uint8
	Balances   map[string]*big.Int
	Allowances map[string]map[string]*big.Int
}

// NewERC20Token is a constructor for the ERC20Token struct.
func NewERC20Token(name string, symbol string) *ERC20Token {
	token := &ERC20Token{
		Name:       name,
		Symbol:     symbol,
		Decimals:   18,
		Balances:   make(map[string]*big.Int),
		Allowances: make(map[string]map[string]*big.Int),
	}
	// Initially assign all tokens to the creator
	token.GenerateFixedAccounts(100, params.Init_Balance)
	return token
}

// Transfer transfers tokens from sender to recipient.
func (t *ERC20Token) Transfer(sender, recipient string, amount *big.Int) error {
	if t == nil {
		return errors.New("ERC20: token does not exist")
	}
	if t.Balances[sender].Cmp(amount) < 0 {
		return errors.New("ERC20: transfer amount exceeds balance")
	}
	t.Balances[sender].Sub(t.Balances[sender], amount)
	t.Balances[recipient].Add(t.Balances[recipient], amount)
	return nil
}

// Deposit increases the balance of a specific account by a given amount.
func (t *ERC20Token) Deposit(account string, amount *big.Int) {
	if _, exists := t.Balances[account]; !exists {
		t.Balances[account] = big.NewInt(0)
	}
	t.Balances[account].Add(t.Balances[account], amount)
}

// Deduct decreases the balance of a specific account by a given amount.
func (t *ERC20Token) Deduct(account string, amount *big.Int) error {
	if t.Balances[account].Cmp(amount) < 0 {
		return errors.New("ERC20: insufficient balance to decrement")
	}
	t.Balances[account].Sub(t.Balances[account], amount)
	return nil
}

// GetBalance retrieves the balance of a specific account.
func (t *ERC20Token) GetBalance(account string) *big.Int {
	// Return 0 if the account does not exist
	if balance, exists := t.Balances[account]; exists {
		return new(big.Int).Set(balance) // Return a copy to avoid external mutation
	}
	return big.NewInt(0)
}

// GenerateFixedAccounts creates accounts with fixed balances.
func (t *ERC20Token) GenerateFixedAccounts(numAccounts int, initialBalance *big.Int) {
	for i := 0; i < numAccounts; i++ {
		// Generate an address as a string
		address := fmt.Sprintf("%d", i)

		// Assign the fixed initial balance
		t.Balances[address] = new(big.Int).Set(initialBalance)
	}
}
