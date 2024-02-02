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

	start := time.Now()
	// this should add to the near cache
	_, err = namedMap.Get(ctx, person1.ID)
	duration1 := time.Since(start)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	fmt.Println("Initial get", duration1)

	stats := namedMap.GetNearCacheStats()
	g.Expect(stats).To(gomega.Not(gomega.BeNil()))
	// should be no hits, 1 miss and 1 put
	g.Expect(stats.GetCacheHits()).To(gomega.Equal(int64(0)))
	g.Expect(stats.GetCacheMisses()).To(gomega.Equal(int64(1)))
	g.Expect(stats.GetCachePuts()).To(gomega.Equal(int64(1)))

	// check the near cache size, should be 1
	g.Expect(namedMap.GetNearCacheStats().Size()).To(gomega.Equal(1))

	// do a second get, should be quicker
	start = time.Now()
	_, err = namedMap.Get(ctx, person1.ID)
	duration2 := time.Since(start)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	fmt.Println("Subsequent get", duration2)

	stats = namedMap.GetNearCacheStats()
	g.Expect(stats).To(gomega.Not(gomega.BeNil()))

	// should take much less time
	g.Expect(duration2.Milliseconds() < duration1.Milliseconds()).To(gomega.Equal(true))

	// sleep for 11 seconds, this should expiry the entry and cause re-read
	Sleep(11)

	start = time.Now()
	_, err = namedMap.Get(ctx, person1.ID)
	duration3 := time.Since(start)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	fmt.Println("Subsequent get", duration3)

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
