/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"context"
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
	. "github.com/oracle/coherence-go-client/test/utils"
	"log"
	"os"
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

	session, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	RunKeyValueTest[int, string](g, getNewNamedMap[int, string](g, session, "c1"), 1, "Tim")
	RunKeyValueTest[int, Person](g, getNewNamedMap[int, Person](g, session, "c2"), 1, Person{ID: 1, Name: "Tim"})
	RunKeyValueTest[int, float32](g, getNewNamedMap[int, float32](g, session, "c3"), 1, float32(1.123))
	RunKeyValueTest[int, float64](g, getNewNamedMap[int, float64](g, session, "c4"), 1, 1.123)
	RunKeyValueTest[int, int](g, getNewNamedMap[int, int](g, session, "c5"), 1, 1)
	RunKeyValueTest[int, int16](g, getNewNamedMap[int, int16](g, session, "c7"), 1, 10)
	RunKeyValueTest[int, int32](g, getNewNamedMap[int, int32](g, session, "c8"), 1, 1333)
	RunKeyValueTest[int, int64](g, getNewNamedMap[int, int64](g, session, "c9"), 1, 1333)
	RunKeyValueTest[int, bool](g, getNewNamedMap[int, bool](g, session, "c10"), 1, false)
	RunKeyValueTest[int, bool](g, getNewNamedMap[int, bool](g, session, "c11"), 1, true)
	RunKeyValueTest[int, byte](g, getNewNamedMap[int, byte](g, session, "c12"), 1, byte(22))
	RunKeyValueTest[string, Person](g, getNewNamedMap[string, Person](g, session, "c13"), "k1", Person{ID: 1, Name: "Tim"})
	RunKeyValueTest[string, string](g, getNewNamedMap[string, string](g, session, "c14"), "k1", "value1")
	RunKeyValueTest[int, Person](g, getNewNamedMap[int, Person](g, session, "c15"), 1,
		Person{ID: 1, Name: "Tim", HomeAddress: Address{Address1: "a1", Address2: "a2", City: "Perth", State: "WA", PostCode: 6000}})
	RunKeyValueTest[int, []string](g, getNewNamedMap[int, []string](g, session, "c16"), 1,
		[]string{"a", "b", "c"})
	RunKeyValueTest[int, map[int]string](g, getNewNamedMap[int, map[int]string](g, session, "c17"), 1,
		map[int]string{1: "one", 2: "two", 3: "three"})

	RunKeyValueTest[int, string](g, getNewNamedCache[int, string](g, session, "c1"), 1, "Tim")
	RunKeyValueTest[int, Person](g, getNewNamedCache[int, Person](g, session, "c2"), 1, Person{ID: 1, Name: "Tim"})
	RunKeyValueTest[int, float32](g, getNewNamedCache[int, float32](g, session, "c3"), 1, float32(1.123))
	RunKeyValueTest[int, float64](g, getNewNamedCache[int, float64](g, session, "c4"), 1, 1.123)
	RunKeyValueTest[int, int](g, getNewNamedCache[int, int](g, session, "c5"), 1, 1)
	RunKeyValueTest[int, int16](g, getNewNamedCache[int, int16](g, session, "c7"), 1, 10)
	RunKeyValueTest[int, int32](g, getNewNamedCache[int, int32](g, session, "c8"), 1, 1333)
	RunKeyValueTest[int, int64](g, getNewNamedCache[int, int64](g, session, "c9"), 1, 1333)
	RunKeyValueTest[int, bool](g, getNewNamedCache[int, bool](g, session, "c10"), 1, false)
	RunKeyValueTest[int, bool](g, getNewNamedCache[int, bool](g, session, "c11"), 1, true)
	RunKeyValueTest[int, byte](g, getNewNamedCache[int, byte](g, session, "c12"), 1, byte(22))
	RunKeyValueTest[string, Person](g, getNewNamedCache[string, Person](g, session, "c13"), "k1", Person{ID: 1, Name: "Tim"})
	RunKeyValueTest[string, string](g, getNewNamedCache[string, string](g, session, "c14"), "k1", "value1")
	RunKeyValueTest[int, Person](g, getNewNamedCache[int, Person](g, session, "c15"), 1,
		Person{ID: 1, Name: "Tim", HomeAddress: Address{Address1: "a1", Address2: "a2", City: "Perth", State: "WA", PostCode: 6000}})
	RunKeyValueTest[int, []string](g, getNewNamedCache[int, []string](g, session, "c16"), 1,
		[]string{"a", "b", "c"})
	RunKeyValueTest[int, map[int]string](g, getNewNamedCache[int, map[int]string](g, session, "c17"), 1,
		map[int]string{1: "one", 2: "two", 3: "three"})
}

func TestSessionWithSpecifiedTimeout(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
	)

	session, err = GetSession(coherence.WithSessionTimeout(time.Duration(10) * time.Second))
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

	t.Setenv("COHERENCE_SESSION_TIMEOUT", "10000")

	session, err = GetSession()
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

	session, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	account := Account{AccountID: 100, AccountType: "savings", Name: "John Doe", Balance: 100_000}

	RunKeyValueTest[AccountKey, Account](g, getNewNamedMap[AccountKey, Account](g, session, "key-map"), account.GetKey(), account)
	RunKeyValueTest[AccountKey, Account](g, getNewNamedCache[AccountKey, Account](g, session, "key-cache"), account.GetKey(), account)
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

// TestBasicOperationsAgainstMapAndCache runs all tests against NamedMap and NamedCache.
func TestBasicOperationsAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, Person])
	}{
		{"NamedMapCrudTest", GetNamedMap[int, Person](g, session, "people-map"), RunTestBasicCrudOperations},
		{"NamedCacheCrudTest", GetNamedCache[int, Person](g, session, "people-cache"), RunTestBasicCrudOperations},
		{"NamedMapRunTestGetOrDefault", GetNamedMap[int, Person](g, session, "get-or-default-map"), RunTestGetOrDefault},
		{"NamedCacheRunTestGetOrDefault", GetNamedCache[int, Person](g, session, "get-or-default-cache"), RunTestGetOrDefault},
		{"NamedMapRunTestContainsKey", GetNamedMap[int, Person](g, session, "contains-key-map"), RunTestContainsKey},
		{"NamedCacheRunTestContainsKey", GetNamedCache[int, Person](g, session, "contains-key-cache"), RunTestContainsKey},
		{"NamedMapRunTestPutIfAbsent", GetNamedMap[int, Person](g, session, "put-if-absent-map"), RunTestPutIfAbsent},
		{"NamedCacheRunTestPutIfAbsent", GetNamedCache[int, Person](g, session, "put-of-absent-cache"), RunTestPutIfAbsent},
		{"NamedMapRunTestClearAndIsEmpty", GetNamedMap[int, Person](g, session, "clear-map"), RunTestClearAndIsEmpty},
		{"NamedCacheRunTestClearAndIsEmpty", GetNamedCache[int, Person](g, session, "clear-cache"), RunTestClearAndIsEmpty},
		{"NamedMapRunTestTruncateAndDestroy", GetNamedMap[int, Person](g, session, "truncate-map"), RunTestTruncateAndDestroy},
		{"NamedCacheRunTestTruncateAndDestroy", GetNamedCache[int, Person](g, session, "truncate-cache"), RunTestTruncateAndDestroy},
		{"NamedMapRunTestReplace", GetNamedMap[int, Person](g, session, "replace-map"), RunTestReplace},
		{"NamedCacheRunTestReplace", GetNamedCache[int, Person](g, session, "replace-cache"), RunTestReplace},
		{"NamedMapRunTestReplaceMapping", GetNamedMap[int, Person](g, session, "replace-mapping-map"), RunTestReplaceMapping},
		{"NamedCacheRunTestReplaceMapping", GetNamedCache[int, Person](g, session, "replace-mapping-cache"), RunTestReplaceMapping},
		{"NamedMapRunTestRemoveMapping", GetNamedMap[int, Person](g, session, "remove-mapping-map"), RunTestRemoveMapping},
		{"NamedCacheRunTestRemoveMapping", GetNamedCache[int, Person](g, session, "remove-mapping-cache"), RunTestRemoveMapping},
		{"NamedMapRunTestPutAll", GetNamedMap[int, Person](g, session, "remove-mapping-map"), RunTestPutAll},
		{"NamedCacheRunTestPutAll", GetNamedCache[int, Person](g, session, "remove-mapping-cache"), RunTestPutAll},
		{"NamedMapRunTestContainsValue", GetNamedMap[int, Person](g, session, "contains-value-map"), RunTestContainsValue},
		{"NamedCacheRunTestContainsValue", GetNamedCache[int, Person](g, session, "contains-value-cache"), RunTestContainsValue},
		{"NamedMapRunTestContainsEntry", GetNamedMap[int, Person](g, session, "contains-entry-map"), RunTestContainsEntry},
		{"NamedCacheRunTestContainsEntry", GetNamedCache[int, Person](g, session, "contains-entry-cache"), RunTestContainsEntry},
		{"NamedMapRunTestValuesFilter", GetNamedMap[int, Person](g, session, "values-filter-map"), RunTestValuesFilter},
		{"NamedCacheRunTestValuesFilter", GetNamedCache[int, Person](g, session, "values-filter-cache"), RunTestValuesFilter},
		{"NamedMapRunTestEntrySetFilter", GetNamedMap[int, Person](g, session, "entryset-filter-map"), RunTestEntrySetFilter},
		{"NamedCacheRunTestEntrySetFilter", GetNamedCache[int, Person](g, session, "entryset-filter-cache"), RunTestEntrySetFilter},
		{"NamedMapRunTestKeySetFilter", GetNamedMap[int, Person](g, session, "keyset-map"), RunTestKeySetFilter},
		{"NamedCacheRunTestKeySetFilter", GetNamedCache[int, Person](g, session, "keyset-cache"), RunTestKeySetFilter},
		{"NamedMapRunTestGetAll", GetNamedMap[int, Person](g, session, "getall-filter-map"), RunTestGetAll},
		{"NamedCacheRunTestGetAll", GetNamedCache[int, Person](g, session, "getall-filter-cache"), RunTestGetAll},
		{"NamedMapRunTestInvokeAll", GetNamedMap[int, Person](g, session, "invokeall-keys-map"), RunTestInvokeAllKeys},
		{"NamedCacheRunTestInvokeAll", GetNamedCache[int, Person](g, session, "invokeall-keys-cache"), RunTestInvokeAllKeys},
		{"NamedMapRunTestKeySet", GetNamedMap[int, Person](g, session, "keyset-map"), RunTestKeySetLong},
		{"NamedCacheRunTestKeySet", GetNamedCache[int, Person](g, session, "keyset-cache"), RunTestKeySetLong},
		{"NamedMapRunTestKeySetShort", GetNamedMap[int, Person](g, session, "keyset-map-short"), RunTestKeySetShort},
		{"NamedCacheRunTestKeySetShort", GetNamedCache[int, Person](g, session, "keyset-cache-short"), RunTestKeySetShort},
		{"NamedMapRunTestEntrySet", GetNamedMap[int, Person](g, session, "entryset-map"), RunTestEntrySetLong},
		{"NamedCacheRunTestEntrySet", GetNamedCache[int, Person](g, session, "entryset-cache"), RunTestEntrySetLong},
		{"NamedMapRunTestEntrySetShort", GetNamedMap[int, Person](g, session, "entryset-map-short"), RunTestEntrySetShort},
		{"NamedCacheRunTestEntrySetShort", GetNamedCache[int, Person](g, session, "entryset-cache-short"), RunTestEntrySetShort},
		{"NamedMapRunTestValues", GetNamedMap[int, Person](g, session, "values-map-short"), RunTestValuesShort},
		{"NamedCacheRunTestValues", GetNamedCache[int, Person](g, session, "values-cache-short"), RunTestValuesShort},
		{"NamedMapRunTestValues", GetNamedMap[int, Person](g, session, "values-map"), RunTestValuesLong},
		{"NamedCacheRunTestValues", GetNamedCache[int, Person](g, session, "values-cache"), RunTestValuesLong},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func TestCantSetDefaultExpiryForNamedMap(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	_, err = coherence.GetNamedMap[int, Person](session, "cache-expiry", coherence.WithExpiry(time.Duration(5)*time.Second))
	g.Expect(err).Should(gomega.HaveOccurred())
}

func RunTestBasicCrudOperations(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g         = gomega.NewWithT(t)
		result    *Person
		oldValue  *Person
		err       error
		person1   = Person{ID: 1, Name: "Tim"}
		oldPerson *Person
	)

	oldPerson, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldPerson).To(gomega.BeNil())

	result, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	AssertPersonResult(g, *result, person1)

	// update the name to "Timothy"
	person1.Name = "Timothy"
	oldValue, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.Not(gomega.BeNil()))
	g.Expect(oldValue.Name).To(gomega.Equal("Tim"))

	result, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	AssertPersonResult(g, *result, person1)

	oldValue, err = namedMap.Remove(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	AssertPersonResult(g, *oldValue, person1)
}

func TestTestMultipleCallsToNamedMap(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		err          error
		person1      = Person{ID: 1, Name: "Tim"}
		personValue1 *Person
		personValue2 *Person
	)

	session, err := GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedMap1, err := coherence.GetNamedMap[int, Person](session, "map-1")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedMap1.Clear(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// retrieve the named map again, should return the same one
	namedMap2, err := coherence.GetNamedMap[int, Person](session, "map-1")
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

	namedMap3, err := coherence.GetNamedMap[int, Person](session, "map-2")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	AssertSize(g, namedMap3, 0)

	// try and retrieve a NamedMap that is for the same cache but different type, this should cause error
	_, err = coherence.GetNamedMap[int, string](session, "map-2")
	g.Expect(err).To(gomega.HaveOccurred())
}

func RunTestGetOrDefault(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = Person{ID: 10, Name: "John"}
		result   *Person
		oldValue *Person
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

	result, err = namedMap.GetOrDefault(ctx, 10, Person{ID: 111, Name: "Not this one"})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	AssertPersonResult(g, *result, person1)
}

func RunTestContainsKey(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = Person{ID: 1, Name: "Tim"}
		oldValue *Person
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

func RunTestContainsEntry(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = Person{ID: 1, Name: "Tim"}
		person2  = Person{ID: 2, Name: "Tim2"}
		err      error
		found    bool
		oldValue *Person
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

func RunTestContainsValue(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = Person{ID: 1, Name: "Tim"}
		err      error
		found    bool
		oldValue *Person
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

func RunTestPutIfAbsent(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = Person{ID: 1, Name: "Tim"}
		person2  = Person{ID: 1, Name: "Timothy"}
		err      error
		found    bool
		result   *Person
		oldValue *Person
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
	AssertPersonResult(g, *result, person1)

	// ensure a Get for the person1.ID returns the original person
	// and not the attempted update
	result, err = namedMap.Get(ctx, person1.ID)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	AssertPersonResult(g, *result, person1)
}

func RunTestClearAndIsEmpty(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = Person{ID: 1, Name: "Tim"}
		err      error
		isEmpty  bool
		oldValue *Person
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

func RunTestTruncateAndDestroy(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		person1  = Person{ID: 1, Name: "Tim"}
		err      error
		oldValue *Person
	)

	_, err = namedMap.Put(ctx, person1.ID, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	err = namedMap.Truncate(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = namedMap.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func RunTestReplace(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g             = gomega.NewWithT(t)
		person1       = Person{ID: 1, Name: "Tim"}
		personReplace = Person{ID: 1, Name: "Timothy"}
		err           error
		oldValue      *Person
	)

	// no Value for Key exists so will not replace or return old Value
	oldValue, err = namedMap.Replace(ctx, 1, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// add an entry that we will replace further down
	oldValue, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	AssertSize(g, namedMap, 1)

	// this should work as it's mapped to any Value
	oldValue, err = namedMap.Replace(ctx, 1, personReplace)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.Not(gomega.BeNil()))
	AssertPersonResult(g, *oldValue, person1)
}

func RunTestReplaceMapping(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g             = gomega.NewWithT(t)
		person1       = Person{ID: 1, Name: "Tim"}
		personReplace = Person{ID: 1, Name: "Timothy"}
		personNew     = Person{ID: 1, Name: "Timothy Jones"}
		err           error
		result        bool
		personValue   *Person
		oldValue      *Person
	)

	// no Value for Key exists so will not replace and should return false
	result, err = namedMap.ReplaceMapping(ctx, 1, personReplace, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(false))

	// add an entry that we will replace further down
	oldValue, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	AssertSize(g, namedMap, 1)

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

func RunTestRemoveMapping(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g       = gomega.NewWithT(t)
		person1 = Person{ID: 1, Name: "Tim"}
		person2 = Person{ID: 1, Name: "Tim2"}
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
	AssertSize(g, namedMap, 1)

	// remove a mapping that doesn't match
	removed, err = namedMap.RemoveMapping(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(false))
	AssertSize(g, namedMap, 1)

	// set the Key to a Value that will match
	_, err = namedMap.Put(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	removed, err = namedMap.RemoveMapping(ctx, 1, person1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(true))
	AssertSize(g, namedMap, 0)
}

var peopleData = map[int]Person{
	1: {ID: 1, Name: "Tim", Age: 33},
	2: {ID: 2, Name: "Andrew", Age: 44},
	3: {ID: 3, Name: "Helen", Age: 20},
	4: {ID: 4, Name: "Alexa", Age: 12},
}

func RunTestPutAll(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
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

func RunTestValuesFilter(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]Person, 0)
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
	results = make([]Person, 0)

	ch2 := namedMap.ValuesFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		results = append(results, se.Value)
	}

	g.Expect(len(results)).To(gomega.Equal(3))
}

func RunTestEntrySetFilter(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]Person, 0)
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
	results = make([]Person, 0)

	ch2 := namedMap.EntrySetFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		results = append(results, se.Value)
	}

	g.Expect(len(results)).To(gomega.Equal(3))
}

func RunTestKeySetFilter(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
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

func RunTestGetAll(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
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

func RunTestInvokeAllKeys(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]int, 0)
	)

	// populate the cache
	populatePeople(g, namedMap)

	// run a processor to increment the age of people with key 1 and 2
	ch := coherence.InvokeAllKeys[int, Person, int](ctx, namedMap, []int{1, 2}, processors.Increment("age", 1, true))

	for se := range ch {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		results = append(results, se.Value)
	}
	g.Expect(len(results)).To(gomega.Equal(2))
	// the results are the keys that were updates
	g.Expect(containsValue[int](results, 1)).Should(gomega.BeTrue())
	g.Expect(containsValue[int](results, 2)).Should(gomega.BeTrue())

	// reset and run for filter
	results = make([]int, 0)

	// run a processor to increment the age of people who are older than 1
	ch2 := coherence.InvokeAllFilter[int, Person, int](ctx, namedMap,
		filters.Greater(extractors.Extract[int]("age"), 1), processors.Increment("age", 1, true))

	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		g.Expect(se.Value).ShouldNot(gomega.BeNil())
		results = append(results, se.Value)
	}

	// should match all entries
	g.Expect(len(results)).To(gomega.Equal(4))
}

func RunTestKeySetLong(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	if !includeLongRunningTests() {
		t.Log("Skipping long running tests")
		return
	}

	RunTestKeySetBase(t, namedMap, 400_000)
}

func RunTestKeySetShort(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	RunTestKeySetBase(t, namedMap, 5_000)
}

// RunTestKeySetBase runs the base RunTestKeySet test
func RunTestKeySetBase(t *testing.T, namedMap coherence.NamedMap[int, Person], insertCount int) {
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
	_, err = namedMap.Put(ctx, 1, Person{ID: 1, Name: "Tim", Age: 20})
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

func RunTestEntrySetLong(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	if !includeLongRunningTests() {
		t.Log("Skipping long running tests")
		return
	}

	RunTestEntrySetBase(t, namedMap, 400_000)
}

func RunTestEntrySetShort(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	RunTestEntrySetBase(t, namedMap, 5_000)
}

func RunTestEntrySetBase(t *testing.T, namedMap coherence.NamedMap[int, Person], insertCount int) {
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
	_, err = namedMap.Put(ctx, 1, Person{ID: 1, Name: "Tim", Age: 20})
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

func RunTestValuesShort(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	RunTestValuesBase(t, namedMap, 5_000)
}

func RunTestValuesLong(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	if !includeLongRunningTests() {
		t.Log("Skipping long running tests")
		return
	}
	RunTestValuesBase(t, namedMap, 400_000)
}

func RunTestValuesBase(t *testing.T, namedMap coherence.NamedMap[int, Person], insertCount int) {
	var (
		g       = gomega.NewWithT(t)
		results = make([]Person, 0)
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
	_, err = namedMap.Put(ctx, 1, Person{ID: 1, Name: "Tim", Age: 20})
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

func addManyPeople(g *gomega.WithT, namedMap coherence.NamedMap[int, Person], startKey, count int) {
	var (
		err error
	)

	buffer := make(map[int]Person, 0)
	for i := startKey; i < count+startKey; i++ {
		buffer[i] = Person{
			ID:   i,
			Name: fmt.Sprintf("Person %d", i),
			Age:  10 + i%50,
		}

		if i%1000 == 0 {
			err = namedMap.PutAll(ctx, buffer)
			g.Expect(err).ShouldNot(gomega.HaveOccurred())
			buffer = make(map[int]Person, 0)
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
func populatePeople(g *gomega.WithT, namedMap coherence.NamedMap[int, Person]) {
	var (
		err  error
		size int
	)
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
