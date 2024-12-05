/*
 * Copyright (c) 2023, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to carry out basic operations against a NamedMap where a key and value are structs.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
)

// AccountKey defines the key for an account.
type AccountKey struct {
	AccountID   int    `json:"accountID"`
	AccountType string `json:"accountType"`
}

func (ak AccountKey) String() string {
	return fmt.Sprintf("AccountKey{accountID=%d, accountType=%s}", ak.AccountID, ak.AccountType)
}

// Account defines the attributes for an account.
type Account struct {
	AccountID   int     `json:"accountID"`
	AccountType string  `json:"accountType"`
	Name        string  `json:"name"`
	Balance     float32 `json:"balance"`
}

// GetKey returns the AccountKey for an Account.
func (a Account) GetKey() AccountKey {
	return AccountKey{
		AccountID:   a.AccountID,
		AccountType: a.AccountType,
	}
}

func (a Account) String() string {
	return fmt.Sprintf("Account{accountKey=%v, name=%s, balance=$%.2f}", a.GetKey(), a.Name, a.Balance)
}

func main() {
	var (
		account *Account
		size    int
		ctx     = context.Background()
	)

	// create a new Session
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}

	defer session.Close()

	// create a new NamedMap of Account with key of AccountKey
	namedMap, err := coherence.GetNamedMap[AccountKey, Account](session, "accounts")
	if err != nil {
		panic(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	newAccountKey := AccountKey{AccountID: 100, AccountType: "savings"}
	newAccount := Account{AccountID: newAccountKey.AccountID, AccountType: newAccountKey.AccountType,
		Name: "John Doe", Balance: 100_000}

	fmt.Println("Add new Account", newAccount, "with key", newAccountKey)
	if _, err = namedMap.Put(ctx, newAccount.GetKey(), newAccount); err != nil {
		panic(err)
	}

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size)

	// get the Account
	if account, err = namedMap.Get(ctx, newAccountKey); err != nil {
		panic(err)
	}
	fmt.Println("Account from Get() is", *account)

	fmt.Println("Update account balance using processor")
	// Update the balance by $1000
	_, err = coherence.Invoke[AccountKey, Account, bool](ctx, namedMap, newAccountKey,
		processors.Update("balance", account.Balance+1000))
	if err != nil {
		panic(err)
	}

	// get the Account
	if account, err = namedMap.Get(ctx, newAccountKey); err != nil {
		panic(err)
	}
	fmt.Println("Updated account is", *account)

	_, err = namedMap.Remove(ctx, newAccountKey)
	if err != nil {
		panic(err)
	}

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size)
}
