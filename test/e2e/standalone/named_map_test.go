/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"context"
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

// includeLongRunning indicates if to include long-running tests
const includeLongRunning = "INCLUDE_LONG_RUNNING"

var ctx = context.Background()

// AccountKey defines the key for an account.
type AccountKey struct {
	AccountID   int    `json:"accountID"`
	AccountType string `json:"accountType"`
}

// Account defines the attributes for an account.
type Account struct {
	AccountID   int     `json:"accountID"`
	AccountType string  `json:"accountType"`
	Name        string  `json:"name"`
	Balance     float32 `json:"balance"`
}

// GetKey returns the AccountKey for an Account.
func (a Account) GetKey() AccountKey {
	return AccountKey{
		AccountID:   a.AccountID,
		AccountType: a.AccountType,
	}
}
func TestBasicCrudOperationsVariousTypes(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
	)

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	utils.RunKeyValueTest[int, string](g, getNewNamedMap[int, string](g, session, "c1"), 1, "Tim")
	utils.RunKeyValueTest[int, utils.Person](g, getNewNamedMap[int, utils.Person](g, session, "c2"), 1, utils.Person{ID: 1, Name: "Tim"})
	utils.RunKeyValueTest[int, float32](g, getNewNamedMap[int, float32](g, session, "c3"), 1, float32(1.123))
	utils.RunKeyValueTest[int, float64](g, getNewNamedMap[int, float64](g, session, "c4"), 1, 1.123)
	utils.RunKeyValueTest[int, int](g, getNewNamedMap[int, int](g, session, "c5"), 1, 1)
	utils.RunKeyValueTest[int, int16](g, getNewNamedMap[int, int16](g, session, "c7"), 1, 10)
	utils.RunKeyValueTest[int, int32](g, getNewNamedMap[int, int32](g, session, "c8"), 1, 1333)
	utils.RunKeyValueTest[int, int64](g, getNewNamedMap[int, int64](g, session, "c9"), 1, 1333)
	utils.RunKeyValueTest[int, bool](g, getNewNamedMap[int, bool](g, session, "c10"), 1, false)
	utils.RunKeyValueTest[int, bool](g, getNewNamedMap[int, bool](g, session, "c11"), 1, true)
	utils.RunKeyValueTest[int, byte](g, getNewNamedMap[int, byte](g, session, "c12"), 1, byte(22))
	utils.RunKeyValueTest[string, utils.Person](g, getNewNamedMap[string, utils.Person](g, session, "c13"), "k1", utils.Person{ID: 1, Name: "Tim"})
	utils.RunKeyValueTest[string, string](g, getNewNamedMap[string, string](g, session, "c14"), "k1", "value1")
	utils.RunKeyValueTest[int, utils.Person](g, getNewNamedMap[int, utils.Person](g, session, "c15"), 1,
		utils.Person{ID: 1, Name: "Tim", HomeAddress: utils.Address{Address1: "a1", Address2: "a2", City: "Perth", State: "WA", PostCode: 6000}})
	utils.RunKeyValueTest[int, []string](g, getNewNamedMap[int, []string](g, session, "c16"), 1,
		[]string{"a", "b", "c"})
	utils.RunKeyValueTest[int, map[int]string](g, getNewNamedMap[int, map[int]string](g, session, "c17"), 1,
		map[int]string{1: "one", 2: "two", 3: "three"})

	utils.RunKeyValueTest[int, string](g, getNewNamedCache[int, string](g, session, "c1"), 1, "Tim")
	utils.RunKeyValueTest[int, utils.Person](g, getNewNamedCache[int, utils.Person](g, session, "c2"), 1, utils.Person{ID: 1, Name: "Tim"})
	utils.RunKeyValueTest[int, float32](g, getNewNamedCache[int, float32](g, session, "c3"), 1, float32(1.123))
	utils.RunKeyValueTest[int, float64](g, getNewNamedCache[int, float64](g, session, "c4"), 1, 1.123)
	utils.RunKeyValueTest[int, int](g, getNewNamedCache[int, int](g, session, "c5"), 1, 1)
	utils.RunKeyValueTest[int, int16](g, getNewNamedCache[int, int16](g, session, "c7"), 1, 10)
	utils.RunKeyValueTest[int, int32](g, getNewNamedCache[int, int32](g, session, "c8"), 1, 1333)
	utils.RunKeyValueTest[int, int64](g, getNewNamedCache[int, int64](g, session, "c9"), 1, 1333)
	utils.RunKeyValueTest[int, bool](g, getNewNamedCache[int, bool](g, session, "c10"), 1, false)
	utils.RunKeyValueTest[int, bool](g, getNewNamedCache[int, bool](g, session, "c11"), 1, true)
	utils.RunKeyValueTest[int, byte](g, getNewNamedCache[int, byte](g, session, "c12"), 1, byte(22))
	utils.RunKeyValueTest[string, utils.Person](g, getNewNamedCache[string, utils.Person](g, session, "c13"), "k1", utils.Person{ID: 1, Name: "Tim"})
	utils.RunKeyValueTest[string, string](g, getNewNamedCache[string, string](g, session, "c14"), "k1", "value1")
	utils.RunKeyValueTest[int, utils.Person](g, getNewNamedCache[int, utils.Person](g, session, "c15"), 1,
		utils.Person{ID: 1, Name: "Tim", HomeAddress: utils.Address{Address1: "a1", Address2: "a2", City: "Perth", State: "WA", PostCode: 6000}})
	utils.RunKeyValueTest[int, []string](g, getNewNamedCache[int, []string](g, session, "c16"), 1,
		[]string{"a", "b", "c"})
	utils.RunKeyValueTest[int, map[int]string](g, getNewNamedCache[int, map[int]string](g, session, "c17"), 1,
		map[int]string{1: "one", 2: "two", 3: "three"})
}

func TestSessionWithSpecifiedTimeout(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
	)

	session, err = utils.GetSession(coherence.WithRequestTimeout(time.Duration(10) * time.Second))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	runTimeoutTest(g, session)
}

func TestSessionWithEnvTimeout(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
	)

	t.Setenv("COHERENCE_CLIENT_REQUEST_TIMEOUT", "10000")

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	runTimeoutTest(g, session)
}

func runTimeoutTest(g *gomega.WithT, session *coherence.Session) {
	// we should get an error as we should be > default timeout
	namedMap := getNewNamedMap[int, string](g, session, "timeout-map")
	err := namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	namedCache := getNewNamedCache[int, string](g, session, "timeout-cache")
	err = namedCache.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// create a new context with an existing deadline, it should be honored
	ctxNew, cancel := context.WithTimeout(ctx, time.Duration(1)*time.Nanosecond)
	defer cancel()
	err = namedCache.Clear(ctxNew)
	g.Expect(err).Should(gomega.HaveOccurred())
}

// TestBasicCrudOperationsVariousTypesWithStructKey tests operations against caches that have keys and values as structs.
func TestBasicCrudOperationsVariousTypesWithStructKey(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
	)

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	account := Account{AccountID: 100, AccountType: "savings", Name: "John Doe", Balance: 100_000}

	utils.RunKeyValueTest[AccountKey, Account](g, getNewNamedMap[AccountKey, Account](g, session, "key-map"), account.GetKey(), account)
	utils.RunKeyValueTest[AccountKey, Account](g, getNewNamedCache[AccountKey, Account](g, session, "key-cache"), account.GetKey(), account)
}

// TestInvocationTimeoutAndTruncate tests running an entry processor with a timeout and then truncate caching.
func TestInvocationTimeout(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
	)

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	RunTestInvocationTimeout(g, getNewNamedMap[int, utils.Person](g, session, "map-timeout-test"))
	RunTestInvocationTimeout(g, getNewNamedCache[int, utils.Person](g, session, "cache-timeout-test"))
}

type LongRunningProcessor struct {
	Type string `json:"@class,omitempty"`
	Name string `json:"name"`
}

func (p *LongRunningProcessor) AndThen(next processors.Processor) processors.Processor {
	return next
}

func (p *LongRunningProcessor) When(_ filters.Filter) processors.Processor {
	return nil
}

func RunTestInvocationTimeout(g *gomega.WithT, namedMap coherence.NamedMap[int, utils.Person]) {
	// populate the cache
	populatePeople(g, namedMap)

	processor := LongRunningProcessor{Type: "test.longEntryProcessor", Name: "LongRunningProcessor"}
	ctxNew, cancel := context.WithTimeout(ctx, time.Duration(10)*time.Second)
	defer cancel()

	// run a processor to increment the age of people with key 1 and 2
	ch := coherence.InvokeAllKeys[int, utils.Person, int](ctxNew, namedMap, []int{1}, &processor)

	for se := range ch {
		// we should get an error due to timeout
		g.Expect(se.Err).Should(gomega.HaveOccurred())
	}
}

// TestBasicOperationsAgainstMapAndCache runs all tests against NamedMap and NamedCache.
func TestBasicOperationsAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, utils.Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, utils.Person])
	}{
		{"NamedMapCrudTest", utils.GetNamedMap[int, utils.Person](g, session, "people-map"), RunTestBasicCrudOperations},
		{"NamedCacheCrudTest", utils.GetNamedCache[int, utils.Person](g, session, "people-cache"), RunTestBasicCrudOperations},
		{"NamedMapRunTestGetOrDefault", utils.GetNamedMap[int, utils.Person](g, session, "get-or-default-map"), RunTestGetOrDefault},
		{"NamedCacheRunTestGetOrDefault", utils.GetNamedCache[int, utils.Person](g, session, "get-or-default-cache"), RunTestGetOrDefault},
		{"NamedMapRunTestContainsKey", utils.GetNamedMap[int, utils.Person](g, session, "contains-key-map"), RunTestContainsKey},
		{"NamedCacheRunTestContainsKey", utils.GetNamedCache[int, utils.Person](g, session, "contains-key-cache"), RunTestContainsKey},
		{"NamedMapRunTestPutIfAbsent", utils.GetNamedMap[int, utils.Person](g, session, "put-if-absent-map"), RunTestPutIfAbsent},
		{"NamedCacheRunTestPutIfAbsent", utils.GetNamedCache[int, utils.Person](g, session, "put-of-absent-cache"), RunTestPutIfAbsent},
		{"NamedMapRunTestClearAndIsEmpty", utils.GetNamedMap[int, utils.Person](g, session, "clear-map"), RunTestClearAndIsEmpty},
		{"NamedCacheRunTestClearAndIsEmpty", utils.GetNamedCache[int, utils.Person](g, session, "clear-cache"), RunTestClearAndIsEmpty},
		{"NamedMapRunTestTruncateAndDestroy", utils.GetNamedMap[int, utils.Person](g, session, "truncate-map"), RunTestTruncateAndDestroy},
		{"NamedCacheRunTestTruncateAndDestroy", utils.GetNamedCache[int, utils.Person](g, session, "truncate-cache"), RunTestTruncateAndDestroy},
		{"NamedMapRunTestReplace", utils.GetNamedMap[int, utils.Person](g, session, "replace-map"), RunTestReplace},
		{"NamedCacheRunTestReplace", utils.GetNamedCache[int, utils.Person](g, session, "replace-cache"), RunTestReplace},
		{"NamedMapRunTestReplaceMapping", utils.GetNamedMap[int, utils.Person](g, session, "replace-mapping-map"), RunTestReplaceMapping},
		{"NamedCacheRunTestReplaceMapping", utils.GetNamedCache[int, utils.Person](g, session, "replace-mapping-cache"), RunTestReplaceMapping},
		{"NamedMapRunTestRemoveMapping", utils.GetNamedMap[int, utils.Person](g, session, "remove-mapping-map"), RunTestRemoveMapping},
		{"NamedCacheRunTestRemoveMapping", utils.GetNamedCache[int, utils.Person](g, session, "remove-mapping-cache"), RunTestRemoveMapping},
		{"NamedMapRunTestPutAll", utils.GetNamedMap[int, utils.Person](g, session, "put-all-map"), RunTestPutAll},
		{"NamedCacheRunTestPutAll", utils.GetNamedCache[int, utils.Person](g, session, "put-all-cache"), RunTestPutAll},
		{"NamedMapRunTestContainsValue", utils.GetNamedMap[int, utils.Person](g, session, "contains-value-map"), RunTestContainsValue},
		{"NamedCacheRunTestContainsValue", utils.GetNamedCache[int, utils.Person](g, session, "contains-value-cache"), RunTestContainsValue},
		{"NamedMapRunTestContainsEntry", utils.GetNamedMap[int, utils.Person](g, session, "contains-entry-map"), RunTestContainsEntry},
		{"NamedCacheRunTestContainsEntry", utils.GetNamedCache[int, utils.Person](g, session, "contains-entry-cache"), RunTestContainsEntry},
		{"NamedMapRunTestValuesFilter", utils.GetNamedMap[int, utils.Person](g, session, "values-filter-map"), RunTestValuesFilter},
		{"NamedCacheRunTestValuesFilter", utils.GetNamedCache[int, utils.Person](g, session, "values-filter-cache"), RunTestValuesFilter},
		{"NamedMapRunTestEntrySetFilter", utils.GetNamedMap[int, utils.Person](g, session, "entryset-filter-map"), RunTestEntrySetFilter},
		{"NamedCacheRunTestEntrySetFilter", utils.GetNamedCache[int, utils.Person](g, session, "entryset-filter-cache"), RunTestEntrySetFilter},
		{"NamedMapRunTestEntrySetFilterWithComparator", utils.GetNamedMap[int, utils.Person](g, session, "entryset-filter-map-comparator"), RunTestEntrySetFilterWithComparator},
		{"NamedCacheRunTestEntrySetFilterWithComparator", utils.GetNamedCache[int, utils.Person](g, session, "entryset-filter-cache-comparator"), RunTestEntrySetFilterWithComparator},
		{"NamedMapRunTestKeySetFilter", utils.GetNamedMap[int, utils.Person](g, session, "keyset-map"), RunTestKeySetFilter},
		{"NamedCacheRunTestKeySetFilter", utils.GetNamedCache[int, utils.Person](g, session, "keyset-cache"), RunTestKeySetFilter},
		{"NamedMapRunTestGetAll", utils.GetNamedMap[int, utils.Person](g, session, "getall-filter-map"), RunTestGetAll},
		{"NamedCacheRunTestGetAll", utils.GetNamedCache[int, utils.Person](g, session, "getall-filter-cache"), RunTestGetAll},
		{"NamedMapRunTestInvokeAll", utils.GetNamedMap[int, utils.Person](g, session, "invokeall-keys-map"), RunTestInvokeAllKeysAndFilter},
		{"NamedCacheRunTestInvokeAll", utils.GetNamedCache[int, utils.Person](g, session, "invokeall-keys-cache"), RunTestInvokeAllKeysAndFilter},
		{"NamedMapRunTestKeySet", utils.GetNamedMap[int, utils.Person](g, session, "keyset-map"), RunTestKeySetLong},
		{"NamedCacheRunTestKeySet", utils.GetNamedCache[int, utils.Person](g, session, "keyset-cache"), RunTestKeySetLong},
		{"NamedMapRunTestKeySetShort", utils.GetNamedMap[int, utils.Person](g, session, "keyset-map-short"), RunTestKeySetShort},
		{"NamedCacheRunTestKeySetShort", utils.GetNamedCache[int, utils.Person](g, session, "keyset-cache-short"), RunTestKeySetShort},
		{"NamedMapRunTestEntrySet", utils.GetNamedMap[int, utils.Person](g, session, "entryset-map"), RunTestEntrySetLong},
		{"NamedCacheRunTestEntrySet", utils.GetNamedCache[int, utils.Person](g, session, "entryset-cache"), RunTestEntrySetLong},
		{"NamedMapRunTestEntrySetShort", utils.GetNamedMap[int, utils.Person](g, session, "entryset-map-short"), RunTestEntrySetShort},
		{"NamedCacheRunTestEntrySetShort", utils.GetNamedCache[int, utils.Person](g, session, "entryset-cache-short"), RunTestEntrySetShort},
		{"NamedMapRunTestValues", utils.GetNamedMap[int, utils.Person](g, session, "values-map-short"), RunTestValuesShort},
		{"NamedCacheRunTestValues", utils.GetNamedCache[int, utils.Person](g, session, "values-cache-short"), RunTestValuesShort},
		{"NamedMapRunTestValues", utils.GetNamedMap[int, utils.Person](g, session, "values-map"), RunTestValuesLong},
		{"NamedCacheRunTestValues", utils.GetNamedCache[int, utils.Person](g, session, "values-cache"), RunTestValuesLong},
		{"NamedMapRunTestIsReady", utils.GetNamedMap[int, utils.Person](g, session, "is-ready-map"), RunTestIsReady},
		{"NamedCacheRunTestIsReady", utils.GetNamedCache[int, utils.Person](g, session, "is-ready-cache"), RunTestIsReady},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

// getNewNamedMap returns a map for a session and asserts err is nil.
func getNewNamedMap[K comparable, V any](g *gomega.WithT, session *coherence.Session, name string) coherence.NamedMap[K, V] {
	namedMap, err := coherence.GetNamedMap[K, V](session, name)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	return namedMap
}

// getNewNamedCache returns a cache for a session and asserts err is nil.
func getNewNamedCache[K comparable, V any](g *gomega.WithT, session *coherence.Session, name string) coherence.NamedCache[K, V] {
	namedCache, err := coherence.GetNamedCache[K, V](session, name)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	return namedCache
}

func TestCantSetDefaultExpiryForNamedMap(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	_, err = coherence.GetNamedMap[int, utils.Person](session, "cache-expiry", coherence.WithExpiry(time.Duration(5)*time.Second))
	g.Expect(err).Should(gomega.HaveOccurred())
}

func RunTestIsReady(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		isReady bool
		content []byte
	)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	content, err = utils.IssueGetRequest(utils.GetTestContext().RestURL + "/isIsReadyPresent")
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	isReady, err = namedMap.IsReady(ctx)
	if string(content) == "true" {
		// IsReady is present on the server, so we can proceed with test and it should succeed
		g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))
		g.Expect(isReady).To(gomega.Equal(true))
	} else {
		// should raise an error if not available on the server
		g.Expect(err).Should(gomega.HaveOccurred())
	}
}

func RunTestBasicCrudOperations(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g         = gomega.NewWithT(t)
		result    *utils.Person
		oldValue  *utils.Person
		err       error
		person1   = utils.Person{ID: 1, Name: "Tim"}
		oldPerson *utils.Person
	)

	result, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.BeNil())

	oldPerson, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldPerson).To(gomega.BeNil())

	result, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	utils.AssertPersonResult(g, *result, person1)

	// update the name to "Timothy"
	person1.Name = "Timothy"
	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.Not(gomega.BeNil()))
	g.Expect(oldValue.Name).To(gomega.Equal("Tim"))

	result, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	utils.AssertPersonResult(g, *result, person1)

	oldValue, err = namedMap.Remove(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	utils.AssertPersonResult(g, *oldValue, person1)
}

func TestMultipleCallsToNamedMap(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		err          error
		person1      = utils.Person{ID: 1, Name: "Tim"}
		personValue1 *utils.Person
		personValue2 *utils.Person
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedMap1, err := coherence.GetNamedMap[int, utils.Person](session, "map-1")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedMap1.Clear(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// retrieve the named map again, should return the same one
	namedMap2, err := coherence.GetNamedMap[int, utils.Person](session, "map-1")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedMap2.Clear(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	g.Expect(namedMap2).To(gomega.Equal(namedMap1))

	_, err = namedMap1.Put(ctx, person1.ID, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	personValue1, err = namedMap1.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	personValue2, err = namedMap2.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(*personValue1).To(gomega.Equal(*personValue2))

	namedMap3, err := coherence.GetNamedMap[int, utils.Person](session, "map-2")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	utils.AssertSize(g, namedMap3, 0)

	// try and retrieve a NamedMap that is for the same cache but different type, this should cause error
	_, err = coherence.GetNamedMap[int, string](session, "map-2")
	g.Expect(err).To(gomega.HaveOccurred())
}

func TestMultipleGoRoutines(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		wg  sync.WaitGroup
	)

	const (
		goRoutines  = 20
		insertCount = 1000
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedMap, err := coherence.GetNamedMap[int, int](session, "multiple-go-routines")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(namedMap.Clear(ctx)).ShouldNot(gomega.HaveOccurred())

	// run "goRoutines" routines
	wg.Add(goRoutines)

	for i := 1; i <= goRoutines; i++ {
		go func(start int) {
			defer wg.Done()
			var err1 error

			for j := start; j < insertCount+start; j++ {
				_, err1 = namedMap.Put(ctx, j, j)
				g.Expect(err1).ShouldNot(gomega.HaveOccurred())
			}
		}(i * 10000)
	}

	wg.Wait()

	size, err := namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(goRoutines * insertCount))

	err = namedMap.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func RunTestGetOrDefault(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = utils.Person{ID: 10, Name: "John"}
		result   *utils.Person
		oldValue *utils.Person
		err      error
	)

	// should be able to get default when Value does not exist
	result, err = namedMap.GetOrDefault(ctx, 10, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.ID).To(gomega.Equal(person1.ID))
	g.Expect(result.Name).To(gomega.Equal(person1.Name))

	// put a Value and this Value should be retrieved in subsequent calls
	oldValue, err = namedMap.Put(ctx, 10, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	result, err = namedMap.GetOrDefault(ctx, 10, utils.Person{ID: 111, Name: "Not this one"})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	utils.AssertPersonResult(g, *result, person1)
}

func RunTestContainsKey(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = utils.Person{ID: 1, Name: "Tim"}
		oldValue *utils.Person
		err      error
		found    bool
	)

	// should not contain a Key for an entry that does not exist
	found, err = namedMap.ContainsKey(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeFalse())

	// add a new entry
	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// ensure that the ContainsKey is true
	found, err = namedMap.ContainsKey(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeTrue())
}

func RunTestContainsEntry(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = utils.Person{ID: 1, Name: "Tim"}
		person2  = utils.Person{ID: 2, Name: "Tim2"}
		err      error
		found    bool
		oldValue *utils.Person
	)

	// should not contain a entry for an entry that does not exist
	found, err = namedMap.ContainsEntry(ctx, person1.ID, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeFalse())

	// add a new entry
	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// ensure that the ContainsEntry is true
	found, err = namedMap.ContainsEntry(ctx, person1.ID, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeTrue())

	// ensure that the if the entry is different, then this should fail
	found, err = namedMap.ContainsEntry(ctx, person1.ID, person2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeFalse())

	// ensure that the if the key is different, then this should fail
	found, err = namedMap.ContainsEntry(ctx, person2.ID, person2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeFalse())
}

func RunTestContainsValue(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = utils.Person{ID: 1, Name: "Tim"}
		err      error
		found    bool
		oldValue *utils.Person
	)

	// should not contain a value with no cache entries
	found, err = namedMap.ContainsValue(ctx, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeFalse())

	// add a new entry
	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// ensure that the ContainsKey is true
	found, err = namedMap.ContainsValue(ctx, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeTrue())
}

func RunTestPutIfAbsent(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = utils.Person{ID: 1, Name: "Tim"}
		person2  = utils.Person{ID: 1, Name: "Timothy"}
		err      error
		found    bool
		result   *utils.Person
		oldValue *utils.Person
	)

	// put if absent should return nil if Value is not present
	oldValue, err = namedMap.PutIfAbsent(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// Key should exist
	found, err = namedMap.ContainsKey(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(found).To(gomega.BeTrue())

	// try to put an updated person2. The entry should not be updated
	// and the existing entry should be returned
	result, err = namedMap.PutIfAbsent(ctx, person2.ID, person2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))

	// assert the Value returned is the existing entry
	utils.AssertPersonResult(g, *result, person1)

	// ensure a Get for the person1.ID returns the original person
	// and not the attempted update
	result, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	utils.AssertPersonResult(g, *result, person1)
}

func RunTestClearAndIsEmpty(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = utils.Person{ID: 1, Name: "Tim"}
		err      error
		isEmpty  bool
		oldValue *utils.Person
	)

	isEmpty, err = namedMap.IsEmpty(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(isEmpty).To(gomega.BeTrue())

	_, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	isEmpty, err = namedMap.IsEmpty(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(isEmpty).To(gomega.BeFalse())

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	isEmpty, err = namedMap.IsEmpty(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(isEmpty).To(gomega.BeTrue())
}

func RunTestTruncateAndDestroy(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = utils.Person{ID: 1, Name: "Tim"}
		err      error
		oldValue *utils.Person
	)

	_, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	err = namedMap.Truncate(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = namedMap.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func RunTestReplace(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g             = gomega.NewWithT(t)
		person1       = utils.Person{ID: 1, Name: "Tim"}
		personReplace = utils.Person{ID: 1, Name: "Timothy"}
		err           error
		oldValue      *utils.Person
	)

	// no Value for Key exists so will not replace or return old Value
	oldValue, err = namedMap.Replace(ctx, 1, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// add an entry that we will replace further down
	oldValue, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	utils.AssertSize(g, namedMap, 1)

	// this should work as it's mapped to any Value
	oldValue, err = namedMap.Replace(ctx, 1, personReplace)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.Not(gomega.BeNil()))
	utils.AssertPersonResult(g, *oldValue, person1)
}

func RunTestReplaceMapping(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g             = gomega.NewWithT(t)
		person1       = utils.Person{ID: 1, Name: "Tim"}
		personReplace = utils.Person{ID: 1, Name: "Timothy"}
		personNew     = utils.Person{ID: 1, Name: "Timothy Jones"}
		err           error
		result        bool
		personValue   *utils.Person
		oldValue      *utils.Person
	)

	// no Value for Key exists so will not replace and should return false
	result, err = namedMap.ReplaceMapping(ctx, 1, personReplace, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(false))

	// add an entry that we will replace further down
	oldValue, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	utils.AssertSize(g, namedMap, 1)

	// value exists but doesn't match so should return false
	result, err = namedMap.ReplaceMapping(ctx, 1, personReplace, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(false))

	// now try replacing where exists and matches
	result, err = namedMap.ReplaceMapping(ctx, 1, person1, personNew)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(true))

	// get the value and check that matches
	personValue, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(personValue).To(gomega.Not(gomega.BeNil()))
	g.Expect(*personValue).To(gomega.Equal(personNew))
}

func RunTestRemoveMapping(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		person1 = utils.Person{ID: 1, Name: "Tim"}
		person2 = utils.Person{ID: 1, Name: "Tim2"}
		err     error
		removed bool
	)

	// remove a mapping that doesn't exist
	removed, err = namedMap.RemoveMapping(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(false))

	// add a Key with a Value that will not match
	_, err = namedMap.Put(ctx, 1, person2)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	utils.AssertSize(g, namedMap, 1)

	// remove a mapping that doesn't match
	removed, err = namedMap.RemoveMapping(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(false))
	utils.AssertSize(g, namedMap, 1)

	// set the Key to a Value that will match
	_, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	removed, err = namedMap.RemoveMapping(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(true))
	utils.AssertSize(g, namedMap, 0)
}

var peopleData = map[int]utils.Person{
	1: {ID: 1, Name: "Tim", Age: 33},
	2: {ID: 2, Name: "Andrew", Age: 44},
	3: {ID: 3, Name: "Helen", Age: 20},
	4: {ID: 4, Name: "Alexa", Age: 12},
}

func RunTestPutAll(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g     = gomega.NewWithT(t)
		err   error
		found bool
		size  int
	)

	err = namedMap.PutAll(ctx, peopleData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(len(peopleData)))

	for k := range peopleData {
		found, err = namedMap.ContainsKey(ctx, k)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(found).To(gomega.BeTrue())
	}
}

func RunTestValuesFilter(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]utils.Person, 0)
	)

	// populate the cache
	populatePeople(g, namedMap)

	ch := namedMap.ValuesFilter(ctx, filters.Always())
	for se := range ch {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		results = append(results, se.Value)
	}
	g.Expect(len(results)).To(gomega.Equal(len(peopleData)))

	// reset the results
	results = make([]utils.Person, 0)

	ch2 := namedMap.ValuesFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		results = append(results, se.Value)
	}

	g.Expect(len(results)).To(gomega.Equal(3))
}

func RunTestEntrySetFilter(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]utils.Person, 0)
	)

	// populate the cache
	populatePeople(g, namedMap)

	ch := namedMap.EntrySetFilter(ctx, filters.Always())
	for se := range ch {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		results = append(results, se.Value)
	}
	g.Expect(len(results)).To(gomega.Equal(len(peopleData)))

	// reset the results
	results = make([]utils.Person, 0)

	ch2 := namedMap.EntrySetFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		results = append(results, se.Value)
	}

	g.Expect(len(results)).To(gomega.Equal(3))
}

func RunTestEntrySetFilterWithComparator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g                    = gomega.NewWithT(t)
		results              = make([]utils.Person, 0)
		comparatorAscending  = extractors.ExtractorComparator(extractors.Extract[int]("age"), true)
		comparatorDescending = extractors.ExtractorComparator(extractors.Extract[int]("age"), false)
		count                = 0
		ageResultsAscending  = []int{12, 20, 33, 44}
		ageResultsDescending = []int{44, 33, 20, 12}
	)

	if namedMap.GetSession().GetProtocolVersion() == 0 {
		// skip as not supported in V0
		return
	}

	// populate the cache
	populatePeople(g, namedMap)

	// try ascending first
	ch := coherence.EntrySetFilterWithComparator(ctx, namedMap, filters.Always(), comparatorAscending)
	for se := range ch {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		g.Expect(ageResultsAscending[count]).To(gomega.Equal(se.Value.Age))
		count++
		results = append(results, se.Value)
	}
	g.Expect(len(results)).To(gomega.Equal(len(peopleData)))

	// reset
	count = 0
	results = make([]utils.Person, 0)

	// descending
	ch = coherence.EntrySetFilterWithComparator(ctx, namedMap, filters.Always(), comparatorDescending)
	for se := range ch {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		g.Expect(ageResultsDescending[count]).To(gomega.Equal(se.Value.Age))
		count++
		results = append(results, se.Value)
	}
	g.Expect(len(results)).To(gomega.Equal(len(peopleData)))
}

func RunTestKeySetFilter(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]int, 0)
	)

	// populate the cache
	populatePeople(g, namedMap)

	ch := namedMap.KeySetFilter(ctx, filters.Always())
	for se := range ch {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Key).ShouldNot(gomega.BeNil())
		results = append(results, se.Key)
	}
	g.Expect(len(results)).To(gomega.Equal(len(peopleData)))

	// reset the results
	results = make([]int, 0)

	ch2 := namedMap.KeySetFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Key).ShouldNot(gomega.BeNil())
		results = append(results, se.Key)
	}

	g.Expect(len(results)).To(gomega.Equal(3))
}

func RunTestGetAll(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]int, 0)
	)

	// populate the cache
	populatePeople(g, namedMap)

	ch := namedMap.GetAll(ctx, []int{1, 3})
	for se := range ch {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Key).ShouldNot(gomega.BeNil())
		results = append(results, se.Key)
	}
	g.Expect(len(results)).To(gomega.Equal(2))

	// reset the results
	results = make([]int, 0)

	ch2 := namedMap.GetAll(ctx, []int{333})
	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Key).ShouldNot(gomega.BeNil())
		results = append(results, se.Key)
	}
	g.Expect(len(results)).To(gomega.Equal(0))
}

func RunTestInvokeAllKeysAndFilter(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g            = gomega.NewWithT(t)
		resultsKey   = make([]int, 0)
		resultsValue []int
		person       *utils.Person
	)

	// populate the cache
	populatePeople(g, namedMap)

	// run a processor to increment the age of people with key 1 and 2
	ch := coherence.InvokeAllKeys[int, utils.Person, int](ctx, namedMap, []int{1, 2}, processors.Increment("age", 1, true))

	for se := range ch {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		resultsKey = append(resultsKey, se.Key)
	}

	g.Expect(len(resultsKey)).To(gomega.Equal(2))

	// the resultsKey are the keys that were updated
	g.Expect(containsValue[int](resultsKey, 1)).Should(gomega.BeTrue())
	g.Expect(containsValue[int](resultsKey, 2)).Should(gomega.BeTrue())

	// reset and run for InvokeAllKeysBlind
	populatePeople(g, namedMap)

	err := coherence.InvokeAllKeysBlind[int, utils.Person](ctx, namedMap, []int{1, 2}, processors.Increment("age", 1, true))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	person, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(person.Age).To(gomega.Equal(34))

	// reset and run for filter
	resultsKey = make([]int, 0)
	resultsValue = make([]int, 0)

	// run a processor to increment the age of people who are older than 1
	ch2 := coherence.InvokeAllFilter[int, utils.Person, int](ctx, namedMap,
		filters.Greater(extractors.Extract[int]("age"), 1), processors.Increment("age", 1, true))

	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		resultsKey = append(resultsKey, se.Key)
		resultsValue = append(resultsValue, se.Value)
	}

	// should match all entries
	g.Expect(len(resultsKey)).To(gomega.Equal(4))
	g.Expect(len(resultsValue)).To(gomega.Equal(4))

	// reset and run for InvokeAllFilterBlind
	populatePeople(g, namedMap)

	err = coherence.InvokeAllFilterBlind[int, utils.Person](ctx, namedMap,
		filters.Greater(extractors.Extract[int]("age"), 1), processors.Increment("age", 1, true))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	person, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(person.Age).To(gomega.Equal(34))
}

func RunTestKeySetLong(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	if !includeLongRunningTests() {
		t.Log("Skipping long running tests")
		return
	}

	RunTestKeySetBase(t, namedMap, 400_000)
}

func RunTestKeySetShort(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	RunTestKeySetBase(t, namedMap, 5_000)
}

// RunTestKeySetBase runs the base RunTestKeySet test
func RunTestKeySetBase(t *testing.T, namedMap coherence.NamedMap[int, utils.Person], insertCount int) {
	g := gomega.NewWithT(t)

	err := namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// test with empty cache to ensure we receive no entries
	counter := 0
	for range namedMap.KeySet(ctx) {
		counter++
	}
	g.Expect(counter).To(gomega.Equal(0))

	// test with single entry which will force only 1 page to be returned
	_, err = namedMap.Put(ctx, 1, utils.Person{ID: 1, Name: "Tim", Age: 20})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	for ch := range namedMap.KeySet(ctx) {
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(ch.Key).Should(gomega.Equal(1))
		counter++
	}
	g.Expect(counter).To(gomega.Equal(1))

	// test with enough data to use multiple requests
	addManyPeople(g, namedMap, 1, insertCount)

	counter = 0

	for ch := range namedMap.KeySet(ctx) {
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		counter++
	}

	g.Expect(counter).To(gomega.Equal(insertCount))
	_ = namedMap.Clear(ctx)
}

func RunTestEntrySetLong(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	if !includeLongRunningTests() {
		t.Log("Skipping long running tests")
		return
	}

	RunTestEntrySetBase(t, namedMap, 400_000)
}

func RunTestEntrySetShort(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	RunTestEntrySetBase(t, namedMap, 5_000)
}

func RunTestEntrySetBase(t *testing.T, namedMap coherence.NamedMap[int, utils.Person], insertCount int) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// test with empty cache to ensure we receive no entries
	counter := 0
	for range namedMap.EntrySet(ctx) {
		counter++
	}
	g.Expect(counter).To(gomega.Equal(0))

	// test with single entry which will force only 1 page to be returned
	_, err = namedMap.Put(ctx, 1, utils.Person{ID: 1, Name: "Tim", Age: 20})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	for se := range namedMap.EntrySet(ctx) {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value.ID).To(gomega.Equal(1))
		g.Expect(se.Key).To(gomega.Equal(1))
	}

	// test with enough data to use multiple requests
	addManyPeople(g, namedMap, 1, insertCount)

	counter = 0
	for ch := range namedMap.EntrySet(ctx) {
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(ch.Key).ShouldNot(gomega.BeNil())
		g.Expect(ch.Value).ShouldNot(gomega.BeNil())
		counter++
	}

	g.Expect(counter).To(gomega.Equal(insertCount))
	_ = namedMap.Clear(ctx)
}

func RunTestValuesShort(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	RunTestValuesBase(t, namedMap, 5_000)
}

func RunTestValuesLong(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	if !includeLongRunningTests() {
		t.Log("Skipping long running tests")
		return
	}
	RunTestValuesBase(t, namedMap, 400_000)
}

func RunTestValuesBase(t *testing.T, namedMap coherence.NamedMap[int, utils.Person], insertCount int) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]utils.Person, 0)
		err     error
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// test with empty cache to ensure we receive zero entries
	counter := 0
	for range namedMap.Values(ctx) {
		counter++
	}
	g.Expect(counter).To(gomega.Equal(0))

	// test with single entry which will force only 1 page to be returned
	_, err = namedMap.Put(ctx, 1, utils.Person{ID: 1, Name: "Tim", Age: 20})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	for ch := range namedMap.Values(ctx) {
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(ch.Value.ID).To(gomega.Equal(1))
		counter++
	}

	// test with enough data to use multiple requests
	addManyPeople(g, namedMap, 1, insertCount)

	for ch := range namedMap.Values(ctx) {
		g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(ch.Value.ID).Should(gomega.BeNumerically(">", 0))
		results = append(results, ch.Value)
	}

	g.Expect(len(results)).To(gomega.Equal(insertCount))
	_ = namedMap.Clear(ctx)
}

func addManyPeople(g *gomega.WithT, namedMap coherence.NamedMap[int, utils.Person], startKey, count int) {
	var (
		err error
	)

	buffer := make(map[int]utils.Person, 0)
	for i := startKey; i < count+startKey; i++ {
		buffer[i] = utils.Person{
			ID:   i,
			Name: fmt.Sprintf("Person %d", i),
			Age:  10 + i%50,
		}

		if i%1000 == 0 {
			err = namedMap.PutAll(ctx, buffer)
			g.Expect(err).ShouldNot(gomega.HaveOccurred())
			buffer = make(map[int]utils.Person, 0)
		}
	}

	// write any leftover buffer
	if len(buffer) > 0 {
		err = namedMap.PutAll(ctx, buffer)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// populatePeople populates the people map
func populatePeople(g *gomega.WithT, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		err  error
		size int
	)
	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = namedMap.PutAll(ctx, peopleData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(len(peopleData)))
}

// containsValue returns true if the value is contains within the array
func containsValue[V comparable](values []V, value V) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func includeLongRunningTests() bool {
	if val := os.Getenv(includeLongRunning); val != "" {
		return true
	}
	return false
}
