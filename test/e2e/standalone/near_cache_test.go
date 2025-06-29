/*
 * Copyright (c) 2024, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"math"
	"strconv"
	"testing"
	"time"
)

const (
	nearCacheName = "near-cache"
	nearMapName   = "near-map"
	noNearCache   = "no-near-cache"
)

// TestNearCacheOperationsAgainstMapAndCache runs all near cache tests against NamedMap and NamedCache.
func TestNearCacheOperationsAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	nearCacheOptions10Seconds := coherence.NearCacheOptions{TTL: time.Duration(10) * time.Second}
	nearCacheOptions120Seconds := coherence.NearCacheOptions{TTL: time.Duration(120) * time.Second}
	nearCacheOptionsHighUnits1 := coherence.NearCacheOptions{HighUnits: 100}
	nearCacheOptionsHighUnits2 := coherence.NearCacheOptions{HighUnitsMemory: 50 * 1024}
	nearCacheOptionsHighUnits3 := coherence.NearCacheOptions{HighUnits: 100, PruneFactor: 0.2}

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, utils.Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, utils.Person])
	}{
		{"RunTestNearCacheBasicNamedMap", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-basic-map", coherence.WithNearCache(&nearCacheOptions10Seconds)), RunTestNearCacheBasic},
		{"RunTestNearCacheBasicNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-basic-cache", coherence.WithNearCache(&nearCacheOptions10Seconds)), RunTestNearCacheBasic},
		{"RunTestNearWithClearNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-clear-cache", coherence.WithNearCache(&nearCacheOptions10Seconds)), RunTestNearWithClear},
		{"RunTestNearWithClearNamedMap", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-clear-map", coherence.WithNearCache(&nearCacheOptions10Seconds)), RunTestNearWithClear},
		{"RunTestNearCacheRemovesNamedMap", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-removes-map", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheRemoves},
		{"RunTestNearCacheRemovesNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-removes-cache", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheRemoves},
		{"RunTestNearCacheContainsKeyNamedMap", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-removes-map", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheContainsKey},
		{"RunTestNearCacheContainsKeyNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-removes-cache", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheContainsKey},
		{"RunTestNearCacheReplacesNamedMap", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-replaces-map", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheReplaces},
		{"RunTestNearCacheReplacesNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-replaces-cache", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheReplaces},
		{"RunTestNearCacheGetAllNamedMap", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-get-all-map", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheGetAll},
		{"RunTestNearCacheGetAllNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-get-all-cache", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheGetAll},
		{"RunTestNearCacheGetAllNamedMap2", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-get-all-map2", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheGetAll2},
		{"RunTestNearCacheGetAllNamedCache2", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-get-all-cache2", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheGetAll2},
		{"RunTestNearCacheWithHighUnitsNamedMap", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-high-units-map", coherence.WithNearCache(&nearCacheOptionsHighUnits1)), RunTestNearCacheWithHighUnits},
		{"RunTestNearCacheWithHighUnitsNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-high-units-cache", coherence.WithNearCache(&nearCacheOptionsHighUnits1)), RunTestNearCacheWithHighUnits},
		{"RunTestNearCacheWithHighUnitsNamedMapPruneFactor", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-high-units-prune-map", coherence.WithNearCache(&nearCacheOptionsHighUnits3)), RunTestNearCacheWithHighUnits},
		{"RunTestNearCacheWithHighUnitsNamedCachePruneFactor", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-high-units-prune-cache", coherence.WithNearCache(&nearCacheOptionsHighUnits3)), RunTestNearCacheWithHighUnits},
		{"RunTestNearCacheWithHighUnitsMemoryNamedMap", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-high-units-mem-map", coherence.WithNearCache(&nearCacheOptionsHighUnits2)), RunTestNearCacheWithHighUnitsMemory},
		{"RunTestNearCacheWithHighUnitsMemoryNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-high-units-mem-cache", coherence.WithNearCache(&nearCacheOptionsHighUnits2)), RunTestNearCacheWithHighUnitsMemory},
		{"RunTestNearCacheWithHighUnitsAccessNamedMap", GetNearCacheNamedMap[int, utils.Person](g, session, "near-cache-high-units-access-mem-map", coherence.WithNearCache(&nearCacheOptionsHighUnits1)), RunTestNearCacheWithHighUnitsAccess},
		{"RunTestNearCacheWithHighUnitsAccessNamedCache", GetNearCacheNamedCache[int, utils.Person](g, session, "near-cache-high-units-access-mem-cache", coherence.WithNearCache(&nearCacheOptionsHighUnits1)), RunTestNearCacheWithHighUnitsAccess},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunTestNearCacheBasic(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		person1  = utils.Person{ID: 1, Name: "Tim"}
		oldValue *utils.Person
	)

	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize[int, utils.Person](g, namedMap, 1)

	// this should add to the near cache
	_, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	stats := namedMap.GetNearCacheStats()
	g.Expect(stats).To(gomega.Not(gomega.BeNil()))

	// should be no hits, 1 miss and 1 put
	g.Expect(stats.GetCacheHits()).To(gomega.Equal(int64(0)))
	g.Expect(stats.GetCacheMisses()).To(gomega.Equal(int64(1)))
	g.Expect(stats.GetCachePuts()).To(gomega.Equal(int64(1)))
	g.Expect(stats.Size()).To(gomega.Equal(1))

	// do a second get, should be quicker
	_, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	stats = namedMap.GetNearCacheStats()
	g.Expect(stats).To(gomega.Not(gomega.BeNil()))

	// sleep for 15 seconds, this should expiry the entry and cause re-read
	utils.Sleep(15)

	AssertSize[int, utils.Person](g, namedMap, 1)

	_, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// now remove the entry from the cache and the delete event should remove from the near cache
	_, err = namedMap.Remove(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(0))
	AssertSize[int, utils.Person](g, namedMap, 0)

	// add new entry and do a get to populate near cache
	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize[int, utils.Person](g, namedMap, 1)

	_, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))

	// Remove the entry via an entry processor, sleep 5 seconds, which should be time for remove event to be received
	_, err = coherence.Invoke[int, utils.Person, utils.Person](ctx, namedMap, 1, processors.ConditionalRemove(filters.Always(), false))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	utils.Sleep(5)
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(0))

	fmt.Println(namedMap.GetNearCacheStats())

	namedMap.Release()
}

func RunTestNearCacheGetAll(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		person1 = utils.Person{ID: 1, Name: "Tim1"}
		person2 = utils.Person{ID: 2, Name: "Tim1"}
		person3 = utils.Person{ID: 3, Name: "Tim3"}
	)

	people := make(map[int]utils.Person, 0)
	people[1] = person1
	people[2] = person2
	people[3] = person3

	// populate
	err = namedMap.PutAll(ctx, people)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	AssertSize[int, utils.Person](g, namedMap, 3)

	count := 0
	// issue a GetAll for all keys, should be no hits
	for ch := range namedMap.GetAll(ctx, []int{1, 2, 3}) {
		count++
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(ch.Value).ShouldNot(gomega.BeNil())
	}

	g.Expect(count).To(gomega.Equal(3))

	// we should have no hits but have size of 3
	g.Expect(namedMap.GetNearCacheStats().GetCacheHits()).To(gomega.Equal(int64(0)))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(3))

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	namedMap.GetNearCacheStats().ResetStats()

	// sleep tp wait for map events to be delivered
	utils.Sleep(10)

	// add the entries back
	err = namedMap.PutAll(ctx, people)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	AssertSize[int, utils.Person](g, namedMap, 3)

	// issue a Get for key 1 and 2 only
	_, err = namedMap.Get(ctx, 1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = namedMap.Get(ctx, 2)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	g.Expect(namedMap.GetNearCacheStats().GetCacheHits()).To(gomega.Equal(int64(0)))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(2))

	// issue GetAll() should have 2 hits and 1 miss
	count = 0
	for ch := range namedMap.GetAll(ctx, []int{1, 2, 3}) {
		count++
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(ch.Value).ShouldNot(gomega.BeNil())
	}

	g.Expect(count).To(gomega.Equal(3))

	g.Expect(namedMap.GetNearCacheStats().GetCacheHits()).To(gomega.Equal(int64(2)))
	g.Expect(namedMap.GetNearCacheStats().GetCacheMisses()).To(gomega.Equal(int64(3)))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(3))
}

func RunTestNearCacheGetAll2(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g   = gomega.NewWithT(t)
		err error
		p1  = utils.Person{ID: 1, Name: "Tim1-Name"}
		p2  = utils.Person{ID: 2, Name: "Tim2-Name"}
		p3  = utils.Person{ID: 3, Name: "Tim3-Name"}
	)

	people := make(map[int]utils.Person, 0)
	people[1] = p1
	people[2] = p2
	people[3] = p3

	err = namedMap.PutAll(ctx, people)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	AssertSize[int, utils.Person](g, namedMap, 3)

	count := 0
	// issue a GetAll for all keys, should be no hits
	for ch := range namedMap.GetAll(ctx, []int{1, 2, 3}) {
		count++
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(ch.Value).ShouldNot(gomega.BeNil())
	}
	g.Expect(count).To(gomega.Equal(3))

	// we should have no hits but have size of 3
	g.Expect(namedMap.GetNearCacheStats().GetCacheHits()).To(gomega.Equal(int64(0)))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(3))

	count = 0
	// issue a second set of GetAll for all keys, should be 3 hits and all streamed from near cache
	for ch := range namedMap.GetAll(ctx, []int{1, 2, 3}) {
		count++
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(ch.Value).ShouldNot(gomega.BeNil())
	}

	g.Expect(count).To(gomega.Equal(3))
	g.Expect(namedMap.GetNearCacheStats().GetCacheHits()).To(gomega.Equal(int64(3)))
}

func RunTestNearCacheRemoves(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		value   *utils.Person
		removed bool
		person1 = utils.Person{ID: 1, Name: "Tim"}
		person2 = utils.Person{ID: 2, Name: "Tim2"}
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	_, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// issue get, should be in near cache
	value, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))
	g.Expect(*value).To(gomega.Equal(person1))

	// execute remove mapping which should not succeed, and near cache should not be emptied
	removed, err = namedMap.RemoveMapping(ctx, 1, person2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(false))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))

	// execute remove mapping which succeeds, the near cache should be removed
	removed, err = namedMap.RemoveMapping(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// wait for back-end update
	utils.Sleep(5)
	g.Expect(removed).Should(gomega.Equal(true))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(0))
}

func RunTestNearCacheContainsKey(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		value   *utils.Person
		person1 = utils.Person{ID: 1, Name: "Tim"}
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	_, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// issue get, should be in near cache
	value, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))
	g.Expect(*value).To(gomega.Equal(person1))
	hits := namedMap.GetNearCacheStats().GetCacheHits()

	// should get a hit from contains key
	contains, err := namedMap.ContainsKey(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(contains).Should(gomega.BeTrue())
	g.Expect(namedMap.GetNearCacheStats().GetCacheHits()).To(gomega.Equal(hits + 1))
}

// RunTestNearCacheWithHighUnits tests near cache with high units of 100.
func RunTestNearCacheWithHighUnits(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	for i := 1; i <= 200; i++ {
		person := utils.Person{ID: i, Name: fmt.Sprintf("person-%v", i)}
		_, err = namedMap.Put(ctx, person.ID, person)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	AssertSize[int, utils.Person](g, namedMap, 200)

	// issue 100 gets to fill the near cache
	for i := 1; i <= 100; i++ {
		_, err = namedMap.Get(ctx, i)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	// should have 100 entries in near cache
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(100))

	// issue a Get() for an entry not in the cache which will trigger the HighUnits and prune entries
	_, err = namedMap.Get(ctx, 110)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// get the current PuneFactor
	pruneFactor := coherence.GetNearCachePruneFactor(namedMap)
	expectedSize := int(math.Round(float64(float32(100) * pruneFactor)))

	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(expectedSize))
	g.Expect(namedMap.GetNearCacheStats().GetCachePrunes()).ToNot(gomega.Equal(int64(0)))
}

// RunTestNearWithClear tests a near cache that issues puts, get then clears..
func RunTestNearWithClear(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g     = gomega.NewWithT(t)
		err   error
		p1Get *utils.Person
		p2Get *utils.Person
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	p1 := utils.Person{ID: 1, Name: "p1"}
	p2 := utils.Person{ID: 2, Name: "p2"}

	for i := 1; i < 100; i++ {
		_, err = namedMap.Put(ctx, p1.ID, p1)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())

		_, err = namedMap.Put(ctx, p2.ID, p2)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())

		AssertSize[int, utils.Person](g, namedMap, 2)

		p1Get, err = namedMap.Get(ctx, p1.ID)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(*p1Get).Should(gomega.Equal(p1))

		p2Get, err = namedMap.Get(ctx, p2.ID)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(*p2Get).Should(gomega.Equal(p2))

		err = namedMap.Clear(ctx)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())

		p1Get, err = namedMap.Get(ctx, p1.ID)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(p1Get).Should(gomega.BeNil())

		p2Get, err = namedMap.Get(ctx, p2.ID)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(p2Get).Should(gomega.BeNil())
	}
}

// RunTestNearCacheWithHighUnits tests near cache with high units of 100 and accessing entries to ensure they are not removed.
func RunTestNearCacheWithHighUnitsAccess(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	for i := 1; i <= 200; i++ {
		person := utils.Person{ID: i, Name: fmt.Sprintf("person-%v", i)}
		_, err = namedMap.Put(ctx, person.ID, person)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	AssertSize[int, utils.Person](g, namedMap, 200)

	// issue 50 gets, should add entries to the near cache
	for i := 1; i <= 50; i++ {
		_, err = namedMap.Get(ctx, i)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(50))

	//Issue another 50 gets
	for i := 51; i <= 100; i++ {
		_, err = namedMap.Get(ctx, i)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	// should have 100 entries in near cache
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(100))

	// issue a Get for key 10, this will be a hit and update the accessTime, so it will not be removed
	_, err = namedMap.Get(ctx, 10)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// issue a Get() for an entry not in the near cache which will trigger the HighUnits and prune to 80 entries
	// but only entries that have not been accessed should be removed. The entry with key 50 should remain as it was accessed
	_, err = namedMap.Get(ctx, 110)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// get the current PuneFactor
	pruneFactor := coherence.GetNearCachePruneFactor(namedMap)
	expectedSize := int(math.Round(float64(float32(100) * pruneFactor)))

	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(expectedSize))
	g.Expect(namedMap.GetNearCacheStats().GetCachePrunes()).ToNot(gomega.Equal(int64(0)))

	t.Log("near cache stats before get", namedMap.GetNearCacheStats())

	// issue a get for id = 10 this should cause a hit as it should not have been removed
	_, err = namedMap.Get(ctx, 10)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	namedMap.Release()
}

// RunTestNearCacheWithHighUnitsMemory tests near cache with high units of 50KB.
func RunTestNearCacheWithHighUnitsMemory(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	buffer := make(map[int]utils.Person)
	for i := 1; i <= 5_000; i++ {
		buffer[i] = utils.Person{ID: 1, Name: fmt.Sprintf("person-%v", i)}
	}
	err = namedMap.PutAll(ctx, buffer)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	AssertSize[int, utils.Person](g, namedMap, 5_000)

	// issue 10_000 gets to fill the near cache
	for i := 1; i <= 5_000; i++ {
		_, err = namedMap.Get(ctx, i)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	// should have less than 10,000 entries. We can't be exact here as we won't be sure of memory across machine architectures
	g.Expect(namedMap.GetNearCacheStats().Size() < 5_000).To(gomega.Equal(true))

	fmt.Println("cache=", namedMap.GetNearCacheStats())
	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

// TestDuplicateNamedCache runs tests to ensure that we can't create a cache and try to get the
// same cache name without named cache.
func TestDuplicateNamedCache(t *testing.T) {
	var (
		err        error
		session    *coherence.Session
		g          = gomega.NewWithT(t)
		namedCache coherence.NamedCache[int, string]
		namedMap   coherence.NamedMap[int, string]
	)

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	nearCacheOptions10Seconds := coherence.NearCacheOptions{TTL: time.Duration(10) * time.Second}
	g.Expect(nearCacheOptions10Seconds).To(gomega.Not(gomega.BeNil()))

	// test creating a NamedCache with near cache and then trying to get a NamedCache without near cache
	namedCache, err = coherence.GetNamedCache[int, string](session, nearCacheName, coherence.WithNearCache(&nearCacheOptions10Seconds))
	g.Expect(namedCache).To(gomega.Not(gomega.BeNil()))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try to get the same cache name with no near cache config, should fail
	_, err = coherence.GetNamedCache[int, string](session, nearCacheName)
	fmt.Println(err)
	g.Expect(err).Should(gomega.HaveOccurred())

	namedCache.Release()

	// test creating a NamedMap with near cache and then trying to get a NamedMap without near cache
	namedMap, err = coherence.GetNamedMap[int, string](session, nearMapName, coherence.WithNearCache(&nearCacheOptions10Seconds))
	g.Expect(namedMap).To(gomega.Not(gomega.BeNil()))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try to get the same map name with no near cache config, should fail
	_, err = coherence.GetNamedMap[int, string](session, nearMapName)
	g.Expect(err).Should(gomega.HaveOccurred())

	namedMap.Release()

	// test creating a NamedCache WITHOUT near cache and then trying to get a NamedCache WITH near cache
	namedCache, err = coherence.GetNamedCache[int, string](session, noNearCache)
	g.Expect(namedCache).To(gomega.Not(gomega.BeNil()))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try to get the same cache name with near cache config, should fail
	_, err = coherence.GetNamedCache[int, string](session, noNearCache, coherence.WithNearCache(&nearCacheOptions10Seconds))
	fmt.Println(err)
	g.Expect(err).Should(gomega.HaveOccurred())

	namedCache.Release()

	// test creating a NamedMap with WITHOUT cache and then trying to get a NamedMap WITH near cache
	namedMap, err = coherence.GetNamedMap[int, string](session, "no-near-map")
	g.Expect(namedMap).To(gomega.Not(gomega.BeNil()))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try to get the same map name with near cache config, should fail
	_, err = coherence.GetNamedMap[int, string](session, "no-near-map", coherence.WithNearCache(&nearCacheOptions10Seconds))
	g.Expect(err).Should(gomega.HaveOccurred())

	namedMap.Release()
}

// TestNearCachePruneFactor runs tests to ensure we honor the pruneFactor.
func TestNearCachePruneFactor(t *testing.T) {
	g := gomega.NewWithT(t)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	// test value of 0.2
	nearCacheOptions1 := coherence.NearCacheOptions{HighUnits: 100, PruneFactor: 0.2}
	namedCache, err := coherence.GetNamedCache[int, string](session, nearCacheName, coherence.WithNearCache(&nearCacheOptions1))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// validate the factor
	g.Expect(coherence.GetNearCachePruneFactor[int, string](namedCache)).To(gomega.Equal(nearCacheOptions1.PruneFactor))
	namedCache.Release()

	// test default value of 0.8
	nearCacheOptions2 := coherence.NearCacheOptions{HighUnits: 100}
	namedCache, err = coherence.GetNamedCache[int, string](session, nearCacheName, coherence.WithNearCache(&nearCacheOptions2))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(coherence.GetNearCachePruneFactor[int, string](namedCache)).To(gomega.Equal(float32(0.8)))
}

// TestNearCacheComparison runs tests to compare near and normal cache and outputs size and memory usage.
func TestNearCacheComparison(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	const maxValues = 2_000

	nearCacheOptions := &coherence.NearCacheOptions{HighUnits: maxValues * 2}
	namedCache, err := coherence.GetNamedCache[string, string](session, noNearCache)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	namedCacheNear, err := coherence.GetNamedCache[string, string](session, nearCacheName, coherence.WithNearCache(nearCacheOptions))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(namedCache.Clear(ctx)).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedCacheNear.Clear(ctx)).ShouldNot(gomega.HaveOccurred())

	values := make(map[string]string, 0)

	// populate the map
	for i := 1; i <= maxValues; i++ {
		kv := strconv.Itoa(i)
		values[kv] = kv
	}

	log.Printf("Insert %v entries into caches", maxValues)

	g.Expect(namedCache.PutAll(ctx, values)).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedCacheNear.PutAll(ctx, values)).ShouldNot(gomega.HaveOccurred())

	log.Println("Start", maxValues, "gets on normal cache")
	start := time.Now()
	for i := 1; i <= maxValues; i++ {
		kv := strconv.Itoa(i)
		_, err = namedCache.Get(ctx, kv)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	log.Printf("Time to get %v from normal cache is %v", maxValues, time.Since(start))

	for j := 1; j <= 2; j++ {
		log.Printf("Run %v of get %v gets on near cache", j, maxValues)
		start = time.Now()
		for i := 1; i <= maxValues; i++ {
			kv := strconv.Itoa(i)
			_, err = namedCacheNear.Get(ctx, kv)
			g.Expect(err).ShouldNot(gomega.HaveOccurred())
		}

		log.Printf("Run: %v time to get %v from near cache is %v", j, maxValues, time.Since(start))
	}

	log.Println(namedCacheNear.GetNearCacheStats())

	g.Expect(namedCacheNear.Destroy(ctx)).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedCache.Destroy(ctx)).ShouldNot(gomega.HaveOccurred())
}

// TestInvalidNearCacheOptions runs tests to ensure that we can't create a named cache/map with invalid options.
func TestInvalidNearCacheOptions(t *testing.T) {
	var (
		err     error
		session *coherence.Session
		g       = gomega.NewWithT(t)
	)

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	// cannot have empty options
	nearCacheOptionsBad1 := coherence.NearCacheOptions{}

	_, err = coherence.GetNamedCache[int, string](session, nearCacheName, coherence.WithNearCache(&nearCacheOptionsBad1))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidNearCache))

	_, err = coherence.GetNamedMap[int, string](session, nearMapName, coherence.WithNearCache(&nearCacheOptionsBad1))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidNearCache))

	// cannot have TTL + HighUnits + HighUnitsMemory
	nearCacheOptionsBad2 := coherence.NearCacheOptions{TTL: time.Duration(10) * time.Millisecond, HighUnitsMemory: 199, HighUnits: 33}

	_, err = coherence.GetNamedCache[int, string](session, nearCacheName, coherence.WithNearCache(&nearCacheOptionsBad2))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidNearCacheWithTTL))

	_, err = coherence.GetNamedMap[int, string](session, nearMapName, coherence.WithNearCache(&nearCacheOptionsBad2))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidNearCacheWithTTL))

	// cannot have HighUnits + HighUnitsMemory
	nearCacheOptionsBad3 := coherence.NearCacheOptions{HighUnitsMemory: 199, HighUnits: 33}

	_, err = coherence.GetNamedCache[int, string](session, nearCacheName, coherence.WithNearCache(&nearCacheOptionsBad3))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidNearCacheWithNoTTL))

	_, err = coherence.GetNamedMap[int, string](session, nearMapName, coherence.WithNearCache(&nearCacheOptionsBad3))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidNearCacheWithNoTTL))

	// cannot have Invalid PruneFactor
	nearCacheOptionsBad4 := coherence.NearCacheOptions{HighUnits: 1000, PruneFactor: -1.0}
	_, err = coherence.GetNamedMap[int, string](session, nearMapName, coherence.WithNearCache(&nearCacheOptionsBad4))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidPruneFactor))

	nearCacheOptionsBad4.PruneFactor = 0.05
	_, err = coherence.GetNamedMap[int, string](session, nearMapName, coherence.WithNearCache(&nearCacheOptionsBad4))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidPruneFactor))

	nearCacheOptionsBad4.PruneFactor = 1.01
	_, err = coherence.GetNamedMap[int, string](session, nearMapName, coherence.WithNearCache(&nearCacheOptionsBad4))
	g.Expect(err).Should(gomega.HaveOccurred())
	g.Expect(err).Should(gomega.Equal(coherence.ErrInvalidPruneFactor))

}

// TestIncompatibleNearCacheOptions runs tests to ensure that we can't create a named cache/map with incompatible.
func TestIncompatibleNearCacheOptions(t *testing.T) {
	const (
		namedMap   = "named-map"
		namedCache = "named-cache"
	)

	var (
		nearCacheOptions1 = coherence.NearCacheOptions{TTL: time.Duration(10) * time.Second}
		nearCacheOptions2 = coherence.NearCacheOptions{TTL: time.Duration(5) * time.Second}
	)

	g := gomega.NewWithT(t)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	// get a namedMap without near cache
	namedMapNoNearCache, err := coherence.GetNamedMap[int, string](session, namedMap)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedMapNoNearCache).To(gomega.Not(gomega.BeNil()))

	// try to create a new namedMap with same name but using near cache, should not work
	_, err = coherence.GetNamedMap[int, string](session, namedMap, coherence.WithNearCache(&nearCacheOptions1))
	g.Expect(err).Should(gomega.HaveOccurred())

	// release and re-create namedMap with near cache options
	namedMapNoNearCache.Release()

	utils.Sleep(10)

	namedMapNoNearCache, err = coherence.GetNamedMap[int, string](session, namedMap, coherence.WithNearCache(&nearCacheOptions1))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedMapNoNearCache).To(gomega.Not(gomega.BeNil()))

	// try and create a new cache with the same name without near cache
	_, err = coherence.GetNamedMap[int, string](session, namedMap)
	g.Expect(err).Should(gomega.HaveOccurred())

	// try and create a new cache with different near cache options
	_, err = coherence.GetNamedMap[int, string](session, namedMap, coherence.WithNearCache(&nearCacheOptions2))
	g.Expect(err).Should(gomega.HaveOccurred())

	// get the cache with same options, should work
	namedMap2, err := coherence.GetNamedMap[int, string](session, namedMap, coherence.WithNearCache(&nearCacheOptions1))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedMap2).To(gomega.Equal(namedMapNoNearCache))

	// get a namedCache without near cache
	namedCacheNoNearCache, err := coherence.GetNamedCache[int, string](session, namedCache)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedCacheNoNearCache).To(gomega.Not(gomega.BeNil()))

	// try to create a new namedCache with same name but using near cache, should not work
	_, err = coherence.GetNamedCache[int, string](session, namedCache, coherence.WithNearCache(&nearCacheOptions1))
	g.Expect(err).Should(gomega.HaveOccurred())

	// destroy and re-create namedMap with near cache options
	namedCacheNoNearCache.Release()

	namedCacheNoNearCache, err = coherence.GetNamedCache[int, string](session, namedCache, coherence.WithNearCache(&nearCacheOptions1))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedCacheNoNearCache).To(gomega.Not(gomega.BeNil()))

	// try and create a new cache with the same name without near cache
	_, err = coherence.GetNamedCache[int, string](session, namedCache)
	g.Expect(err).Should(gomega.HaveOccurred())

	// try and create a new cache with different near cache options
	_, err = coherence.GetNamedCache[int, string](session, namedCache, coherence.WithNearCache(&nearCacheOptions2))
	g.Expect(err).Should(gomega.HaveOccurred())

	// get the cache with same options, should work
	namedCache2, err := coherence.GetNamedCache[int, string](session, namedCache, coherence.WithNearCache(&nearCacheOptions1))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedCache2).To(gomega.Equal(namedCacheNoNearCache))
}

func RunTestNearCacheReplaces(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		value   *utils.Person
		removed bool
		person1 = utils.Person{ID: 1, Name: "Tim"}
		person2 = utils.Person{ID: 1, Name: "Tim2"}
	)

	_, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// issue get, should be in near cache
	value, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))
	g.Expect(*value).To(gomega.Equal(person1))

	// execute replace which should succeed as there is a value and near cache should be updated
	value, err = namedMap.Replace(ctx, 1, person2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).Should(gomega.Equal(person1))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))

	hits := namedMap.GetNearCacheStats().GetCacheHits()

	// sleep enough time for near cache to be updated
	utils.Sleep(5)

	// execute get from near cache and it should be there
	value, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal(person2))
	g.Expect(namedMap.GetNearCacheStats().GetCacheHits()).To(gomega.Equal(hits + 1))

	// clear the cache
	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	AssertSize[int, utils.Person](g, namedMap, 0)

	utils.Sleep(5)
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(0))

	// put entry back into cache
	_, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// issue get, should be in near cache
	value, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))
	g.Expect(*value).To(gomega.Equal(person1))

	// execute replace mapping which succeeds, the near cache should be updated
	removed, err = namedMap.ReplaceMapping(ctx, 1, person1, person2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(true))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))

	utils.Sleep(5)

	value, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))
	g.Expect(*value).To(gomega.Equal(person2))
}

func GetNearCacheNamedCache[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName string, options ...func(options *coherence.CacheOptions)) coherence.NamedCache[K, V] {
	namedCache, err := coherence.GetNamedCache[K, V](session, cacheName, options...)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedCache.Clear(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	return namedCache
}

func GetNearCacheNamedMap[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName string, options ...func(options *coherence.CacheOptions)) coherence.NamedMap[K, V] {
	namedMap, err := coherence.GetNamedMap[K, V](session, cacheName, options...)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedMap.Clear(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	return namedMap
}
