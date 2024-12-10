/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */
package coherence

import (
	"fmt"
	"github.com/onsi/gomega"
	"math"
	"sync"
	"testing"
	"time"
)

func TestBasicLocalCacheOperations(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache-1")
	g.Expect(cache.Size()).To(gomega.Equal(0))

	old := cache.Put(1, "one")
	g.Expect(cache.Size()).To(gomega.Equal(1))
	g.Expect(old).To(gomega.BeNil())

	value := cache.Get(1)
	g.Expect(*value).To(gomega.Equal("one"))

	oldValue := cache.Put(1, "ONE")
	g.Expect(*oldValue).To(gomega.Equal("one"))

	oldValue = cache.Remove(1)
	g.Expect(*oldValue).To(gomega.Equal("ONE"))
	g.Expect(cache.Size()).To(gomega.Equal(0))

	value = cache.Get(1)
	g.Expect(value).To(gomega.BeNil())

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")
	g.Expect(cache.Size()).To(gomega.Equal(3))

	cache.Clear()
	g.Expect(cache.Size()).To(gomega.Equal(0))

	cache.PutWithExpiry(1, "one", time.Duration(3)*time.Second)
	Sleep(4)
	g.Expect(cache.Get(1)).To(gomega.BeNil())
	g.Expect(cache.Size()).To(gomega.Equal(0))

	cache.PutWithExpiry(1, "one", time.Duration(3)*time.Second)
	Sleep(4)
	g.Expect(cache.Remove(1)).To(gomega.BeNil())
	g.Expect(cache.Size()).To(gomega.Equal(0))

	cache.PutWithExpiry(1, "one", time.Duration(3)*time.Second)
	Sleep(4)
	g.Expect(cache.Size()).To(gomega.Equal(0))

	cache.PutWithExpiry(1, "one", time.Duration(3)*time.Millisecond)
	time.Sleep(time.Duration(4) * time.Millisecond)
	g.Expect(cache.Size()).To(gomega.Equal(0))

	fmt.Println(cache)
}

func TestBasicLocalCacheWithDefaultExpiry(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache", withLocalCacheExpiry(time.Duration(2)*time.Second))
	g.Expect(cache.Size()).To(gomega.Equal(0))

	cache.Put(1, "one")
	g.Expect(cache.Size()).To(gomega.Equal(1))

	Sleep(3)
	g.Expect(cache.Size()).To(gomega.Equal(0))
}

func TestBasicLocalCacheClear(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache-clear", withLocalCacheExpiry(time.Duration(2)*time.Second))
	g.Expect(cache.Size()).To(gomega.Equal(0))

	cache.Put(1, "one")
	g.Expect(cache.Size()).To(gomega.Equal(1))
	cache.Clear()

	g.Expect(cache.Size()).To(gomega.Equal(0))
}

func TestBasicLocalCacheRelease(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache-clear", withLocalCacheExpiry(time.Duration(2)*time.Second))
	g.Expect(cache.Size()).To(gomega.Equal(0))

	cache.Put(1, "one")
	g.Expect(cache.Size()).To(gomega.Equal(1))
	cache.Release()

	g.Expect(cache.Size()).To(gomega.Equal(0))
}

func TestBasicLocalCacheGetAll(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache-get-all", withLocalCacheExpiry(time.Duration(10)*time.Second))
	g.Expect(cache.Size()).To(gomega.Equal(0))
	g.Expect(len(cache.GetAll([]int{1, 2, 3}))).To(gomega.Equal(0))

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")
	cache.Put(4, "four")
	cache.Put(5, "five")
	g.Expect(cache.Size()).To(gomega.Equal(5))

	results := cache.GetAll([]int{1, 5})
	g.Expect(len(results)).To(gomega.Equal(2))

	v, ok := results[1]
	g.Expect(ok).To(gomega.Equal(true))
	g.Expect(*v).To(gomega.Equal("one"))

	v, ok = results[5]
	g.Expect(ok).To(gomega.Equal(true))
	g.Expect(*v).To(gomega.Equal("five"))

	v, ok = results[6]
	g.Expect(ok).To(gomega.Equal(false))
	g.Expect(v).To(gomega.BeNil())
}

func TestLocalCacheWithHighUnitsOnly(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache-high-unit1", withLocalCacheHighUnits(100))

	for i := 0; i < 100; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
	}

	g.Expect(cache.Size()).To(gomega.Equal(100))

	// put a new entry which should cause prune of 20 entries
	cache.Put(100, "one hundred")

	expectedSize := int(math.Round(float64(float32(100) * cache.options.PruneFactor)))

	g.Expect(cache.Size()).To(gomega.Equal(expectedSize))
	g.Expect(cache.GetCachePrunes()).To(gomega.Equal(int64(1)))
	fmt.Println(cache)
}

func TestLocalCacheWithHighUnitsMemoryOnly(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache-high-unit2", withLocalCacheHighUnitsMemory(1024*100))

	for i := 0; i < 10_000; i++ {
		cache.Put(i, fmt.Sprintf("value2-%v", i))
	}

	// cache size should be less than 10,000 as it would not all fit in under 100K
	g.Expect(cache.Size() < 10_000).To(gomega.Equal(true))

	fmt.Println(cache)
}

func TestLocalCacheWithHighUnitsOnlyAccessTime(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache-high-unit3", withLocalCacheHighUnits(100))

	for i := 0; i < 100; i++ {
		cache.Put(i, fmt.Sprintf("value3-%v", i))
	}

	g.Expect(cache.Size()).To(gomega.Equal(100))

	// access key 1, 2 and 3, when we prune we should not see these entries be removed
	// as they were most recently accessed
	cache.Get(1)
	cache.Get(2)
	cache.Get(3)
	time.Sleep(time.Duration(5) * time.Second)

	// put a new entry which should cause prune of 20 entries
	cache.Put(100, "one hundred")

	expectedSize := int(math.Round(float64(float32(100) * cache.options.PruneFactor)))

	g.Expect(cache.Size()).To(gomega.Equal(expectedSize))
	g.Expect(cache.GetCachePrunes()).To(gomega.Equal(int64(1)))

	// entries 1, 2 and three should not be removed as they were accessed
	g.Expect(cache.Get(1)).To(gomega.Not(gomega.BeNil()))
	g.Expect(cache.Get(2)).To(gomega.Not(gomega.BeNil()))
	g.Expect(cache.Get(3)).To(gomega.Not(gomega.BeNil()))
}

func TestLocalCacheWithHighUnitsAndTTL(t *testing.T) {
	g := gomega.NewWithT(t)

	cache := newLocalCache[int, string]("my-cache-high-unit", withLocalCacheHighUnits(100), withLocalCacheExpiry(time.Duration(2)*time.Second))

	for i := 0; i < 100; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
	}

	g.Expect(cache.Size()).To(gomega.Equal(100))

	// sleep for 1 second and add a new entry
	time.Sleep(time.Duration(1) * time.Second)

	cache.Put(100, "one hundred")
	cache.Put(101, "one hundred and one")

	g.Expect(cache.Size()).To(gomega.Equal(81))
	g.Expect(cache.Get(100)).To(gomega.Not(gomega.BeNil()))
	g.Expect(cache.Get(101)).To(gomega.Not(gomega.BeNil()))

	time.Sleep(time.Duration(2) * time.Second)
	// put 20 new entries, all the entries, all entries < 100 should be expired

	for i := 110; i < 130; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
	}

	g.Expect(cache.Size()).To(gomega.Equal(20))
}

func TestLocalCacheGoRoutines(t *testing.T) {
	var (
		g     = gomega.NewWithT(t)
		cache = newLocalCache[int, string]("my-cache-2")
		wg    sync.WaitGroup
	)

	routines := 500
	iterations := 30

	fmt.Println("Start " + time.Now().String())
	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func(id, iters int) {
			defer wg.Done()
			for j := 1; j <= iters; j++ {
				cache.Put(id*1_0000_000+j, "Value-")
			}
		}(i, iterations)
	}

	wg.Wait()
	fmt.Println("End   " + time.Now().String())
	size := cache.Size()

	g.Expect(size).To(gomega.Equal(routines * iterations))

	fmt.Println(cache.GetStats())
}

func TestBasicLocalCacheSizeCalculation(t *testing.T) {
	g := gomega.NewWithT(t)
	const maxEntries = 10_000

	cache := newLocalCache[int, string]("my-size-calc-size")
	g.Expect(cache.Size()).To(gomega.Equal(0))

	// add maxEntries entries, this should update the memory
	for i := 1; i <= maxEntries; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
	}

	// we should still have memory used
	g.Expect(cache.cacheMemory > 0).To(gomega.BeTrue())

	// update maxEntries entries with a bigger value, this should update the memory with the delta of the new-old
	for i := 1; i <= maxEntries; i++ {
		cache.Put(i, fmt.Sprintf("new-value-bigger-%v", i))
	}
	g.Expect(cache.cacheMemory > 0).To(gomega.BeTrue())

	// update maxEntries entries with a smaller value, this should update the memory with the delta of the new-old
	for i := 1; i <= maxEntries; i++ {
		cache.Put(i, fmt.Sprintf("%v", i))
	}
	g.Expect(cache.cacheMemory > 0).To(gomega.BeTrue())

	// remove each entry and the local cache size should get back to zero to show that
	// the memory calculation is working both ways
	for i := 1; i <= maxEntries; i++ {
		cache.Remove(i)
	}

	g.Expect(cache.cacheMemory).Should(gomega.Equal(int64(0)))
}

func Sleep(seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
}
