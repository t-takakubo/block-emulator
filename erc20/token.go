package erc20

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"blockEmulator/params"
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

func (t *ERC20Token) GobEncode() ([]byte, error) {
	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)

	if err := encoder.Encode(t.Name); err != nil {
		return nil, err
	}
	if err := encoder.Encode(t.Symbol); err != nil {
		return nil, err
	}
	if err := encoder.Encode(t.Decimals); err != nil {
		return nil, err
	}

	var balanceKeys []string
	for k := range t.Balances {
		balanceKeys = append(balanceKeys, k)
	}
	sort.Strings(balanceKeys)
	if err := encoder.Encode(len(balanceKeys)); err != nil {
		return nil, err
	}
	for _, k := range balanceKeys {
		if err := encoder.Encode(k); err != nil {
			return nil, err
		}
		if err := encoder.Encode(t.Balances[k]); err != nil {
			return nil, err
		}
	}

	var allowanceKeys []string
	for k := range t.Allowances {
		allowanceKeys = append(allowanceKeys, k)
	}
	sort.Strings(allowanceKeys)
	if err := encoder.Encode(len(allowanceKeys)); err != nil {
		return nil, err
	}
	for _, k := range allowanceKeys {
		if err := encoder.Encode(k); err != nil {
			return nil, err
		}
		innerMap := t.Allowances[k]
		var innerKeys []string
		for ik := range innerMap {
			innerKeys = append(innerKeys, ik)
		}
		sort.Strings(innerKeys)
		if err := encoder.Encode(len(innerKeys)); err != nil {
			return nil, err
		}
		for _, ik := range innerKeys {
			if err := encoder.Encode(ik); err != nil {
				return nil, err
			}
			if err := encoder.Encode(innerMap[ik]); err != nil {
				return nil, err
			}
		}
	}
	return buff.Bytes(), nil
}

func (t *ERC20Token) GobDecode(data []byte) error {
	buff := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buff)

	if err := decoder.Decode(&t.Name); err != nil {
		return err
	}
	if err := decoder.Decode(&t.Symbol); err != nil {
		return err
	}
	if err := decoder.Decode(&t.Decimals); err != nil {
		return err
	}

	var balanceLen int
	if err := decoder.Decode(&balanceLen); err != nil {
		return err
	}
	t.Balances = make(map[string]*big.Int)
	for i := 0; i < balanceLen; i++ {
		var key string
		if err := decoder.Decode(&key); err != nil {
			return err
		}
		var value big.Int
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		t.Balances[key] = new(big.Int).Set(&value)
	}

	var allowanceLen int
	if err := decoder.Decode(&allowanceLen); err != nil {
		return err
	}
	t.Allowances = make(map[string]map[string]*big.Int)
	for i := 0; i < allowanceLen; i++ {
		var key string
		if err := decoder.Decode(&key); err != nil {
			return err
		}
		var innerLen int
		if err := decoder.Decode(&innerLen); err != nil {
			return err
		}
		innerMap := make(map[string]*big.Int)
		for j := 0; j < innerLen; j++ {
			var innerKey string
			if err := decoder.Decode(&innerKey); err != nil {
				return err
			}
			var value big.Int
			if err := decoder.Decode(&value); err != nil {
				return err
			}
			innerMap[innerKey] = new(big.Int).Set(&value)
		}
		t.Allowances[key] = innerMap
	}
	return nil
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
	if balance, exists := t.Balances[account]; exists {
		return new(big.Int).Set(balance)
	}
	return big.NewInt(0)
}

// GenerateFixedAccounts creates accounts with fixed balances.
func (t *ERC20Token) GenerateFixedAccounts(numAccounts int, initialBalance *big.Int) {
	for i := 0; i < numAccounts; i++ {
		address := fmt.Sprintf("%d", i)
		t.Balances[address] = new(big.Int).Set(initialBalance)
	}
}
