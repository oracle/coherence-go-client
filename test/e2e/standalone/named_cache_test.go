/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/aggregators"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"testing"
	"time"
)

func TestPutWithExpiry(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		person1  = utils.Person{ID: 1, Name: "Tim"}
		oldValue *utils.Person
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	namedCache := GetNamedCache[int, utils.Person](g, session, "put-with-expiry")

	defer session.Close()

	oldValue, err = namedCache.PutWithExpiry(ctx, person1.ID, person1, time.Duration(5)*time.Second)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize[int, utils.Person](g, namedCache, 1)

	// sleep for 6 seconds to allow entry to expire
	time.Sleep(6 * time.Second)

	AssertSize[int, utils.Person](g, namedCache, 0)

	// check that expiry is not > 2147483647 or Integer.MAX_VALUE in Java
	_, err = namedCache.PutWithExpiry(ctx, person1.ID, person1, time.Duration(2147483647+1)*time.Millisecond)
	g.Expect(err).To(gomega.HaveOccurred())
}

func TestPutAllWithExpiry(t *testing.T) {
	var (
		g     = gomega.NewWithT(t)
		err   error
		found bool
		size  int
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	namedCache := GetNamedCache[int, utils.Person](g, session, "put-all-with-expiry")

	err = namedCache.PutAllWithExpiry(ctx, peopleData, time.Duration(4)*time.Second)
	if namedCache.GetSession().GetProtocolVersion() == 0 {
		// PutAllWithExpiry is not supported for v0 and should return error
		g.Expect(err).Should(gomega.HaveOccurred())
		return
	}

	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedCache.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(len(peopleData)))

	for k := range peopleData {
		found, err = namedCache.ContainsKey(ctx, k)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(found).To(gomega.BeTrue())
	}

	utils.Sleep(6)
	size, err = namedCache.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(0))

}

// TestPutWithExpiryUsingCacheOption tests that we can se an overall expiry for the cache and this is applied
// when using standard Put().
func TestPutWithExpiryUsingCacheOption(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		person1  = utils.Person{ID: 1, Name: "Tim"}
		oldValue *utils.Person
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	namedCache, err := coherence.GetNamedCache[int, utils.Person](session, "cache-expiry", coherence.WithExpiry(time.Duration(5)*time.Second))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	defer session.Close()
	defer func() {
		_ = namedCache.Destroy(ctx)
	}()

	_, err = namedCache.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize[int, utils.Person](g, namedCache, 1)

	// sleep for 6 seconds to allow entry to expire
	time.Sleep(6 * time.Second)

	AssertSize[int, utils.Person](g, namedCache, 0)

	// issue a PutWithExpiry which should override the default expiry
	_, err = namedCache.PutWithExpiry(ctx, person1.ID, person1, time.Duration(10)*time.Second)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// sleep for 6 seconds, the entry should still be present
	time.Sleep(6 * time.Second)

	AssertSize[int, utils.Person](g, namedCache, 1)

	// sleep for 6 seconds, the entry should now honour the 10-second expiry
	time.Sleep(6 * time.Second)
	AssertSize[int, utils.Person](g, namedCache, 0)
}

// TestBooleanAndFilters tests to ensure that boolean values are serialized correctly for filters.
func TestBooleanAndFilters(t *testing.T) {
	var (
		g     = gomega.NewWithT(t)
		err   error
		test1 = utils.BooleanTest{ID: 1, Name: "Tim", Active: true}
		test2 = utils.BooleanTest{ID: 2, Name: "Jon", Active: true}
		test3 = utils.BooleanTest{ID: 3, Name: "Pam", Active: false}
		size  int
		count *int64
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	namedCache := GetNamedCache[int, utils.BooleanTest](g, session, "bool-test")
	defer session.Close()

	_, err = namedCache.Put(ctx, test1.ID, test1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = namedCache.Put(ctx, test2.ID, test2)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = namedCache.Put(ctx, test3.ID, test3)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	size, err = namedCache.Size(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(3))

	active := extractors.Extract[bool]("active")

	// count the number of active entries
	count, err = coherence.AggregateFilter[int, utils.BooleanTest, int64](ctx, namedCache, filters.Equal(active, true), aggregators.Count())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(*count).To(gomega.Equal(int64(2)))

	// count the number of inactive entries
	count, err = coherence.AggregateFilter[int, utils.BooleanTest, int64](ctx, namedCache, filters.Equal(active, false), aggregators.Count())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(*count).To(gomega.Equal(int64(1)))

}

// TestTouchProcessor tests a touch processor that will update the time of en entry
func TestTouchProcessor(t *testing.T) {
	var (
		g           = gomega.NewWithT(t)
		err         error
		person1     = utils.Person{ID: 1, Name: "Tim"}
		containsKey bool
		oldValue    *utils.Person
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	namedCache := GetNamedCache[int, utils.Person](g, session, "touch")

	defer session.Close()

	// "touch" cache has default TTL of 10 seconds
	_, err = namedCache.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize[int, utils.Person](g, namedCache, 1)

	// sleep for 6 seconds and the entry should still be there
	time.Sleep(6 * time.Second)

	containsKey, err = namedCache.ContainsKey(ctx, 1)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(containsKey).To(gomega.Equal(true))

	// run the Touch processor which will reset the TTL
	_, err = coherence.Invoke[int, utils.Person, any](ctx, namedCache, 1, processors.Touch())
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// sleep another 6 seconds, which will be approx 12 seconds since original put
	// entry should still exist due to Touch processor
	containsKey, err = namedCache.ContainsKey(ctx, 1)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(containsKey).To(gomega.Equal(true))

	// sleep for 10 seconds and the entry should now be evicted
	time.Sleep(10 * time.Second)

	containsKey, err = namedCache.ContainsKey(ctx, 1)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(containsKey).To(gomega.Equal(false))
}

func TestTestMultipleCallsToNamedCache(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		err          error
		person1      = utils.Person{ID: 1, Name: "Tim"}
		personValue1 *utils.Person
		personValue2 *utils.Person
		session      *coherence.Session
	)

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedCache1, err := coherence.GetNamedCache[int, utils.Person](session, "cache-1")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedCache1.Clear(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// retrieve the named map again, should return the same one
	namedCache2, err := coherence.GetNamedCache[int, utils.Person](session, "cache-1")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedCache2.Clear(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	g.Expect(namedCache2).To(gomega.Equal(namedCache1))

	_, err = namedCache1.Put(ctx, person1.ID, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	personValue1, err = namedCache1.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	personValue2, err = namedCache2.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(*personValue1).To(gomega.Equal(*personValue2))

	namedCache3, err := coherence.GetNamedCache[int, utils.Person](session, "cache-2")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	size, err := namedCache3.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(0))

	// try and retrieve a NamedCache that is for the same cache but different type, this should cause error
	_, err = coherence.GetNamedCache[int, string](session, "cache-2")
	g.Expect(err).To(gomega.HaveOccurred())
}
