/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to run open-ended queries against a NamedMap or NamedCache using keys.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
)

func main() {
	type Person struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	var (
		ctx    = context.Background()
		entry  *coherence.Entry[int, Person]
		person *Person
		key    *int
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	_ = namedMap.Clear(ctx)

	fmt.Println("Adding 10 random people")
	for i := 1; i <= 10; i++ {
		p := Person{ID: i, Name: fmt.Sprintf("Name-%d", i), Age: 15 + i}
		_, err = namedMap.Put(ctx, p.ID, p)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Running KeySet()")
	iter := namedMap.KeySet(ctx)
	count := 0

	for {
		key, err = iter.Next()
		if err == coherence.ErrDone {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Println("key", *key)
		count++
	}
	fmt.Println("KeySet count", count)

	fmt.Println("Running EntrySet()")
	iter2 := namedMap.EntrySet(ctx)
	count = 0

	for {
		entry, err = iter2.Next()
		if err == coherence.ErrDone {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Println("Key:", entry.Key, "Value:", entry.Value)
		count++
	}
	fmt.Println("EntrySet count", count)

	fmt.Println("Running Values()")
	iter3 := namedMap.Values(ctx)
	count = 0

	for {
		person, err = iter3.Next()
		if err == coherence.ErrDone {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Println(*person)
		count++
	}
	fmt.Println("Values() count", count)
}
