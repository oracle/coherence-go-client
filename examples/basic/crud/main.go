/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to carry out basic operations against a NamedMap with primitive types.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
)

func main() {
	var (
		value *string
		ctx   = context.Background()
		size  int
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// create a new NamedMap with key of int and value of string
	namedMap, err := coherence.GetNamedMap[int, string](session, "my-map")
	if err != nil {
		panic(err)
	}

	fmt.Println("Put key 1, value one")
	// put a new key / value
	if _, err = namedMap.Put(ctx, 1, "one"); err != nil {
		panic(err)
	}

	// get the value for the given key
	if value, err = namedMap.Get(ctx, 1); err != nil {
		panic(err)
	}
	fmt.Println("Value for key 1 is", *value)

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size)

	// update the value for key 1
	if _, err = namedMap.Put(ctx, 1, "ONE"); err != nil {
		panic(err)
	}

	// get the updated value for the given key
	if value, err = namedMap.Get(ctx, 1); err != nil {
		panic(err)
	}
	fmt.Println("Updated value is", *value)

	fmt.Println("Remove key", 1)
	if _, err = namedMap.Remove(ctx, 1); err != nil {
		panic(err)
	}

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size)
}
