/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use a near cache with a [NamedMap] or [NamedCache] with an expiry of 10 seconds.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
	"time"
)

func main() {
	var (
		value *string
		ctx   = context.Background()
		size  int
	)

	const maxEntries = 100

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// near cache options to use for NamedCache
	nearCacheOptions := coherence.NearCacheOptions{TTL: time.Duration(10) * time.Second}

	// create a new NamedCache with key of int and value of string which has a
	// near cache that will keep local entries for up to 10 seconds
	namedCache, err := coherence.GetNamedCache[int, string](session, "my-near-cache", coherence.WithNearCache(&nearCacheOptions))
	if err != nil {
		panic(err)
	}

	defer namedCache.Release()

	fmt.Println("Adding", maxEntries, "entries")
	// add maxEntries entries
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

	fmt.Printf("Cache size is %v, doing %v Get() operations to populate near cache\n", size, maxEntries)

	// loop through twice, first time initial get will populate the near cache and second will use the near cache
	for loop := 1; loop <= 2; loop++ {

		if loop == 2 {
			fmt.Printf("Doing second set of %v Get() operations which will come from near cache\n", maxEntries)
			fmt.Println("Notice the difference in time")
		}

		fmt.Printf("\nLoop %d, near cache size is %d\n\n", loop, namedCache.GetNearCacheStats().Size())

		start := time.Now()
		for i := 1; i <= maxEntries; i++ {
			_, err = namedCache.Get(ctx, i)
			if err != nil {
				panic(err)
			}
		}
		duration := time.Since(start)

		fmt.Printf("Total duration of 100 Get() is: %v. Time per individual Get()=%v\n", duration, duration/maxEntries)
		stats := namedCache.GetNearCacheStats()

		fmt.Println("Near Cache Stats", stats)
	}

	fmt.Println("Clearing the cache")
	err = namedCache.Clear(ctx)
	if err != nil {
		panic(err)
	}

	namedCache.GetNearCacheStats().ResetStats()

	// add a new cache entry and demonstrate if we wait for longer than
	// the near cache expiry of 10 seconds, the entry will be gone and re-read
	_, err = namedCache.Put(ctx, 1, "one")
	if err != nil {
		panic(err)
	}

	// issue Get() to populate near cache
	_, err = namedCache.Get(ctx, 1)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Size of near cache is %v, sleep 11 seconds\n", namedCache.GetNearCacheStats().Size())

	time.Sleep(time.Duration(11) * time.Second)

	fmt.Printf("Size of near cache is now %v as near cache has been cleared\n", namedCache.GetNearCacheStats().Size())

	fmt.Println("Clearing the cache again and resetting near cache stats")
	err = namedCache.Clear(ctx)
	if err != nil {
		panic(err)
	}

	namedCache.GetNearCacheStats().ResetStats()

	// add a new cache entry and demonstrate if we update the entry via and entry processor,
	// to simulate a back-end update, it will update the near cache

	_, err = namedCache.Put(ctx, 1, "one")
	if err != nil {
		panic(err)
	}

	// issue Get() to populate near cache
	_, err = namedCache.Get(ctx, 1)
	if err != nil {
		panic(err)
	}

	_, err = coherence.Invoke[int, string, string](ctx, namedCache, 1, processors.ConditionalPut(filters.Always(), "ONE"))
	if err != nil {
		panic(err)
	}

	// sleep for a second for the update to come through (should be much quicker than that)
	time.Sleep(time.Duration(1) * time.Second)

	start := time.Now()
	value, err = namedCache.Get(ctx, 1)
	if err != nil {
		panic(err)
	}
	duration := time.Since(start)
	fmt.Printf("Time to get new value of %v is %v. Near cache size is %v, hits is %v\n\n",
		*value, duration, namedCache.GetNearCacheStats().Size(), namedCache.GetNearCacheStats().GetCacheHits())

}
