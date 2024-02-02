/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */
package coherence

import (
	"fmt"
	. "github.com/onsi/gomega"
	"sync"
	"testing"
	"time"
)

func TestBasicLocalCacheOperations(t *testing.T) {
	g := NewWithT(t)

	cache := newLocalCache[int, string]("my-cache")
	g.Expect(cache.Size()).To(Equal(0))

	cache.Put(1, "one")
	g.Expect(cache.Size()).To(Equal(1))

	value := cache.Get(1)
	g.Expect(*value).To(Equal("one"))

	oldValue := cache.Put(1, "ONE")
	g.Expect(*oldValue).To(Equal("one"))

	oldValue = cache.Remove(1)
	g.Expect(*oldValue).To(Equal("ONE"))
	g.Expect(cache.Size()).To(Equal(0))

	value = cache.Get(1)
	g.Expect(value).To(BeNil())

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")
	g.Expect(cache.Size()).To(Equal(3))

	cache.Clear()
	g.Expect(cache.Size()).To(Equal(0))

	cache.PutWithExpiry(1, "one", time.Duration(3)*time.Second)
	Sleep(4)
	g.Expect(cache.Get(1)).To(BeNil())
	g.Expect(cache.Size()).To(Equal(0))

	cache.PutWithExpiry(1, "one", time.Duration(3)*time.Second)
	Sleep(4)
	g.Expect(cache.Remove(1)).To(BeNil())
	g.Expect(cache.Size()).To(Equal(0))

	cache.PutWithExpiry(1, "one", time.Duration(3)*time.Second)
	Sleep(4)
	g.Expect(cache.Size()).To(Equal(0))

	cache.PutWithExpiry(1, "one", time.Duration(3)*time.Millisecond)
	time.Sleep(time.Duration(4) * time.Millisecond)
	g.Expect(cache.Size()).To(Equal(0))

	fmt.Println(cache.GetStats())
}

func TestBasicLocalCacheWithDefaultExpiry(t *testing.T) {
	g := NewWithT(t)

	cache := newLocalCache[int, string]("my-cache", WithLocalCacheExpiry(time.Duration(2)*time.Second))
	g.Expect(cache.Size()).To(Equal(0))

	cache.Put(1, "one")
	g.Expect(cache.Size()).To(Equal(1))

	Sleep(3)
	g.Expect(cache.Size()).To(Equal(0))
}

func TestLocalCacheGoRoutines(t *testing.T) {
	var (
		g     = NewWithT(t)
		cache = newLocalCache[int, string]("my-cache")
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

	g.Expect(size).To(Equal(routines * iterations))

	fmt.Println(cache.GetStats())
}

func Sleep(seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
}
