/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to put entries that expire in a NamedCache.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"time"
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

	// create a new NamedCache with key of int and value of string.
	// NOTE: A NamedCache is required to call the PutWithExpiry function.
	namedCache, err := coherence.GetNamedCache[int, string](session, "my-cache")
	if err != nil {
		panic(err)
	}

	fmt.Println("Put key 1, value \"one\" with expiry 5 seconds")
	// put a new key / value with expiry of 5 seconds
	if _, err = namedCache.PutWithExpiry(ctx, 1, "one", time.Duration(5)*time.Second); err != nil {
		panic(err)
	}

	// get the value for the given key, it should exist
	if value, err = namedCache.Get(ctx, 1); err != nil {
		panic(err)
	}
	fmt.Printf("Value for key 1 is %v, sleep 6 seconds and cache size should be zero\n", *value)

	time.Sleep(time.Duration(6) * time.Second)

	if size, err = namedCache.Size(ctx); err != nil {
		panic(err)
	}
	// cache size should now be zero
	fmt.Printf("Cache size = %d\n", size)

	if value, err = namedCache.Get(ctx, 1); err != nil {
		panic(err)
	}
	fmt.Printf("Value for key 1 is %v, should be nil pointer as entry no longer exists\n", value)
}
