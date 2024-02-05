/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
	. "github.com/oracle/coherence-go-client/test/utils"
	"testing"
	"time"
)

const (
	nearCacheName = "near-cache"
	nearMapName   = "near-map"
)

// TestNearCacheOperationsAgainstMapAndCache runs all near cache tests against NamedMap and NamedCache.
func TestNearCacheOperationsAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	nearCacheOptions10Seconds := coherence.NearCacheOptions{TTL: time.Duration(10) * time.Second}
	nearCacheOptions120Seconds := coherence.NearCacheOptions{TTL: time.Duration(120) * time.Second}

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, Person])
	}{
		{"TestNearCacheBasicNamedMap", GetNearCacheNamedMap[int, Person](g, session, "near-cache-basic-map", coherence.WithNearCache(&nearCacheOptions10Seconds)), RunTestNearCacheBasic},
		{"TestNearCacheBasicNamedCache", GetNearCacheNamedCache[int, Person](g, session, "near-cache-basic-cache", coherence.WithNearCache(&nearCacheOptions10Seconds)), RunTestNearCacheBasic},
		{"RunTestNearCacheRemovesNamedMap", GetNearCacheNamedMap[int, Person](g, session, "near-cache-removes-map", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheRemoves},
		{"RunTestNearCacheRemovesNamedCache", GetNearCacheNamedCache[int, Person](g, session, "near-cache-removes-cache", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheRemoves},
		{"RunTestNearCacheReplacesNamedMap", GetNearCacheNamedMap[int, Person](g, session, "near-cache-replaces-map", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheReplaces},
		{"RunTestNearCacheReplacesNamedCache", GetNearCacheNamedCache[int, Person](g, session, "near-cache-replaces-cache", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheReplaces},
		{"RunTestNearCacheGetAllNamedMap", GetNearCacheNamedMap[int, Person](g, session, "near-cache-get-all-map", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheGetAll},
		{"RunTestNearCacheGetAllNamedCache", GetNearCacheNamedCache[int, Person](g, session, "near-cache-get-all-cache", coherence.WithNearCache(&nearCacheOptions120Seconds)), RunTestNearCacheGetAll},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunTestNearCacheBasic(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		person1  = Person{ID: 1, Name: "Tim"}
		oldValue *Person
	)

	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize[int, Person](g, namedMap, 1)

	// this should add to the near cache
	_, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	stats := namedMap.GetNearCacheStats()
	g.Expect(stats).To(gomega.Not(gomega.BeNil()))
	// should be no hits, 1 miss and 1 put
	g.Expect(stats.GetCacheHits()).To(gomega.Equal(int64(0)))
	g.Expect(stats.GetCacheMisses()).To(gomega.Equal(int64(1)))
	g.Expect(stats.GetCachePuts()).To(gomega.Equal(int64(1)))

	// check the near cache size, should be 1
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))

	// do a second get, should be quicker
	_, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	stats = namedMap.GetNearCacheStats()
	g.Expect(stats).To(gomega.Not(gomega.BeNil()))

	// sleep for 11 seconds, this should expiry the entry and cause re-read
	Sleep(11)

	_, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	g.Expect(namedMap.GetNearCacheStats().GetCachePuts()).To(gomega.Equal(int64(2)))

	// now remove the entry from the cache and the delete event should remove from the near cache immediately
	_, err = namedMap.Remove(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(0))
	AssertSize[int, Person](g, namedMap, 0)

	// add new entry and do a get to populate near cache
	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize[int, Person](g, namedMap, 1)

	_, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))

	// Remove the entry via an entry processor, sleep 5 seconds, which should be time for remove event to be received
	_, err = coherence.Invoke[int, Person, Person](ctx, namedMap, 1, processors.ConditionalRemove(filters.Always(), false))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	Sleep(5)
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(0))

	namedMap.Release()
}

func RunTestNearCacheGetAll(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		person1 = Person{ID: 1, Name: "Tim1"}
		person2 = Person{ID: 2, Name: "Tim1"}
		person3 = Person{ID: 3, Name: "Tim3"}
	)

	people := make(map[int]Person, 0)
	people[1] = person1
	people[2] = person2
	people[3] = person3

	// populate
	err = namedMap.PutAll(ctx, people)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	AssertSize[int, Person](g, namedMap, 3)

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

	// add the entries back
	err = namedMap.PutAll(ctx, people)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	AssertSize[int, Person](g, namedMap, 3)

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

func RunTestNearCacheRemoves(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		value   *Person
		removed bool
		person1 = Person{ID: 1, Name: "Tim"}
		person2 = Person{ID: 1, Name: "Tim2"}
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
	g.Expect(removed).Should(gomega.Equal(true))
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(0))
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

	session, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	nearCacheOptions10Seconds := coherence.NearCacheOptions{TTL: time.Duration(10) * time.Second}

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
	namedCache, err = coherence.GetNamedCache[int, string](session, "no-near-cache")
	g.Expect(namedCache).To(gomega.Not(gomega.BeNil()))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try to get the same cache name with near cache config, should fail
	_, err = coherence.GetNamedCache[int, string](session, "no-near-cache", coherence.WithNearCache(&nearCacheOptions10Seconds))
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

// TestInvalidNearCacheOptions runs tests to ensure that we can't create a named cache with invalid options.
func TestInvalidNearCacheOptions(t *testing.T) {
	var (
		err     error
		session *coherence.Session
		g       = gomega.NewWithT(t)
	)

	session, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	nearCacheOptions10Seconds := coherence.NearCacheOptions{}

	_, err = coherence.GetNamedCache[int, string](session, nearCacheName, coherence.WithNearCache(&nearCacheOptions10Seconds))
	g.Expect(err).Should(gomega.HaveOccurred())

	_, err = coherence.GetNamedMap[int, string](session, nearMapName, coherence.WithNearCache(&nearCacheOptions10Seconds))
	g.Expect(err).Should(gomega.HaveOccurred())

}

func RunTestNearCacheReplaces(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		value   *Person
		removed bool
		person1 = Person{ID: 1, Name: "Tim"}
		person2 = Person{ID: 1, Name: "Tim2"}
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

	// sleep enough time for near cache to be update
	Sleep(5)

	// execute get from near cache and it should be there
	value, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal(person2))
	g.Expect(namedMap.GetNearCacheStats().GetCacheHits()).To(gomega.Equal(hits + 1))

	// clear the cache
	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	AssertSize[int, Person](g, namedMap, 0)

	Sleep(5)
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

	Sleep(5)

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
