/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use a near cache with a NamedMap or NamedCache with high units memory of 10k.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
)

func main() {
	var (
		ctx  = context.Background()
		size int
	)

	const maxEntries = 5_000

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// near cache options to use for NamedCache
	nearCacheOptions := coherence.NearCacheOptions{HighUnitsMemory: 10 * 1024}

	// create a new NamedCache with key of int and value of string which has a
	// near cache that will keep local entries up to max size of 10K
	namedCache, err := coherence.GetNamedCache[int, string](session, "my-near-cache-high-units-memory", coherence.WithNearCache(&nearCacheOptions))
	if err != nil {
		panic(err)
	}

	defer namedCache.Release()

	err = namedCache.Clear(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println("Adding", maxEntries, "entries")
	buffer := make(map[int]string, 0)
	for i := 1; i <= maxEntries; i++ {
		buffer[i] = fmt.Sprintf("value-%v", i)
	}

	err = namedCache.PutAll(ctx, buffer)
	if err != nil {
		panic(err)
	}
	size, err = namedCache.Size(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Cache size is %v, doing %v Get() operations to populate near cache\n", size, 1000)

	for i := 1; i <= 1000; i++ {
		_, err = namedCache.Get(ctx, i)
		if err != nil {
			panic(err)
		}
		if i%100 == 0 {
			fmt.Println("count=", i)
		}
	}

	fmt.Printf("The near cache should not have a size of 1000 as it will be over the size limit\n\n")
	fmt.Println(namedCache.GetNearCacheStats())
}
