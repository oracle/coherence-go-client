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

	ctx := context.Background()

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
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
	count := 0

	for result := range namedMap.KeySet(ctx) {
		if result.Err != nil {
			panic(result.Err)
		}
		fmt.Println("key", result.Key)
		count++
	}
	fmt.Println("KeySet count", count)

	fmt.Println("Running EntrySet()")
	count = 0

	for result := range namedMap.EntrySet(ctx) {
		if result.Err != nil {
			panic(result.Err)
		}
		fmt.Println("Key:", result.Key, "Value:", result.Value)
		count++
	}
	fmt.Println("EntrySet count", count)

	fmt.Println("Running Values()")
	count = 0

	for result := range namedMap.Values(ctx) {
		if result.Err != nil {
			panic(result.Err)
		}
		fmt.Println(result.Value)
		count++
	}
	fmt.Println("Values() count", count)
}
