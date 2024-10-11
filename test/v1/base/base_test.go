/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package base

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/aggregators"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
	pb1 "github.com/oracle/coherence-go-client/proto/v1"
	"github.com/oracle/coherence-go-client/test/utils"
	"sync"
	"testing"
	"time"
)

var (
	serializerInt32  = coherence.NewSerializer[int32]("json")
	serializerInt64  = coherence.NewSerializer[int64]("json")
	serializerString = coherence.NewSerializer[string]("json")
	serializerAny    = coherence.NewSerializer[any]("json")
)

// TestEnsureCache tests the ensureCache request.
func TestEnsureCache(t *testing.T) {
	g := gomega.NewWithT(t)
	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, "test")
	_ = ensureCache(g, session, "test2")
	_ = ensureCache(g, session, "test3")

}

// TestGetAndPutRequests tests the get and put requests.
func TestGetAndPutRequests(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		ctx          = context.Background()
		cache        = "test-get-put"
		err          error
		currentValue *[]byte
		getValue     *string
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create key and value
	value := ensureSerializedString(g, "value")
	key := ensureSerializedInt32(g, 32)

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())
	assertSize(g, session, cache, 0)

	// test get with no value in the cache
	currentValue, err = coherence.TestGet(ctx, session, cache, key)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(currentValue).Should(gomega.BeNil())

	// put a value into the cache
	currentValue, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())
	// result of put with no value will be "null"
	g.Expect(currentValue).ShouldNot(gomega.BeNil())
	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(getValue).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	currentValue, err = coherence.TestGet(ctx, session, cache, key)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(currentValue).ShouldNot(gomega.BeNil())

	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(*getValue).Should(gomega.Equal("value"))
}

// TestPutIfAbsent tests the put if absent request.
func TestPutIfAbsent(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		ctx          = context.Background()
		cache        = "test-put-if-absent"
		err          error
		currentValue *[]byte
		getValue     *string
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create key and value
	key := ensureSerializedInt32(g, 32)
	value := ensureSerializedString(g, "value")
	value2 := ensureSerializedString(g, "value-2")

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// test put if absent with no entries which should work
	currentValue, err = coherence.TestPutIfAbsent(ctx, session, cache, key, value)
	g.Expect(err).Should(gomega.BeNil())
	getValue, err = serializerString.Deserialize(*currentValue)

	g.Expect(err).Should(gomega.BeNil())
	g.Expect(getValue).Should(gomega.BeNil())
	assertSize(g, session, cache, 1)

	// issue a get, and we should get back the correct value
	currentValue, err = coherence.TestGet(ctx, session, cache, key)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(currentValue).ShouldNot(gomega.BeNil())
	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(*getValue).Should(gomega.Equal("value"))

	// issue putIfAbsent again with "value-2", should not be saved as it already exists
	currentValue, err = coherence.TestPutIfAbsent(ctx, session, cache, key, value2)
	g.Expect(err).Should(gomega.BeNil())

	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(*getValue).Should(gomega.Equal("value"))
}

// TestPutAll tests the putAll request.
func TestPutAll(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		ctx     = context.Background()
		cache   = "test-put-all"
		err     error
		entries = generateEntries(g, 10)
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// test putAll with no expiry
	err = coherence.TestPutAll(ctx, session, cache, entries, 0)
	g.Expect(err).Should(gomega.BeNil())
	assertSize(g, session, cache, 10)

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// test putAll with expiry
	err = coherence.TestPutAll(ctx, session, cache, entries, time.Duration(4)*time.Second)
	g.Expect(err).Should(gomega.BeNil())
	assertSize(g, session, cache, 10)

	utils.Sleep(5)

	assertSize(g, session, cache, 0)
}

// TestPutWithExpiry tests the put with expiry
func TestPutWithExpiry(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		ctx          = context.Background()
		cache        = "test-put-expiry"
		err          error
		currentValue *[]byte
		getValue     *string
	)

	session := getTestSession(t, g)
	defer session.Close()
	_ = ensureCache(g, session, cache)

	// create key and value
	key := ensureSerializedInt32(g, 32)
	value := ensureSerializedString(g, "value")

	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	currentValue, err = coherence.TestPut(ctx, session, cache, key, value, time.Duration(4)*time.Second)
	g.Expect(err).Should(gomega.BeNil())
	// result of put with no value will be "null"
	g.Expect(currentValue).ShouldNot(gomega.BeNil())
	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(getValue).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	utils.Sleep(5)

	assertSize(g, session, cache, 0)
}

// TestRemove tests the remove request.
func TestRemove(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		ctx          = context.Background()
		cache        = "test-remove"
		err          error
		currentValue *[]byte
		getValue     *string
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create key and value
	key := ensureSerializedInt32(g, 1)
	value := ensureSerializedString(g, "value-1")

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// put a value into the cache
	currentValue, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())
	// result of put with no existing value will be "null"
	g.Expect(currentValue).ShouldNot(gomega.BeNil())
	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(getValue).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	currentValue, err = coherence.TestRemove(ctx, session, cache, key)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(currentValue).ShouldNot(gomega.BeNil())
	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(*getValue).Should(gomega.Equal("value-1"))

	assertSize(g, session, cache, 0)

	// test remove when nothing there
	currentValue, err = coherence.TestRemove(ctx, session, cache, key)
	g.Expect(err).Should(gomega.BeNil())
	// result of remove with no existing value will be "null"
	g.Expect(currentValue).ShouldNot(gomega.BeNil())
	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(getValue).Should(gomega.BeNil())
}

// TestRemoveMapping tests the remove mapping request.
func TestRemoveMapping(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		ctx     = context.Background()
		cache   = "test-remove-mapping"
		err     error
		removed bool
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create key and value
	key := ensureSerializedInt32(g, 1)
	value := ensureSerializedString(g, "value-1")
	value2 := ensureSerializedString(g, "value-2")

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// remove a mapping that doesn't exist
	removed, err = coherence.TestRemoveMapping(ctx, session, cache, key, value)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(false))

	// add a Key with a Value that will not match
	_, err = coherence.TestPut(ctx, session, cache, key, value2, 0)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	// remove a second mapping that doesn't match
	removed, err = coherence.TestRemoveMapping(ctx, session, cache, key, value)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(false))

	// set the Key to a Value that will match
	_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())

	removed, err = coherence.TestRemoveMapping(ctx, session, cache, key, value)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(removed).Should(gomega.Equal(true))

	assertSize(g, session, cache, 0)
}

// TestReplace tests the replace request.
func TestReplace(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		ctx          = context.Background()
		cache        = "test-replace"
		key          = ensureSerializedInt32(g, 1)
		value        = ensureSerializedString(g, "value")
		err          error
		currentValue *[]byte
		getValue     *string
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// no Value for Key exists so will not replace or return old Value
	currentValue, err = coherence.TestReplace(ctx, session, cache, key, value)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(currentValue).ShouldNot(gomega.BeNil())
	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(getValue).Should(gomega.BeNil())

	// add an entry that we will replace further down
	_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())
	assertSize(g, session, cache, 1)

	// this should work as it's mapped to any Value
	currentValue, err = coherence.TestReplace(ctx, session, cache, key, value)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(currentValue).To(gomega.Not(gomega.BeNil()))

	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(*getValue).Should(gomega.Equal("value"))
}

// TestReplaceMapping tests the replace mapping request.
func TestReplaceMapping(t *testing.T) {
	var (
		g      = gomega.NewWithT(t)
		ctx    = context.Background()
		cache  = "test-replace-mapping"
		err    error
		result bool
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create key and value
	key := ensureSerializedInt32(g, 1)
	value := ensureSerializedString(g, "value")
	valueReplace := ensureSerializedString(g, "value replace")
	valueNew := ensureSerializedString(g, "value new")

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// no Value for Key exists so will not replace and should return false
	result, err = coherence.TestReplaceMapping(ctx, session, cache, key, valueReplace, value)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(false))

	// add an entry that we will replace further down
	_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())
	assertSize(g, session, cache, 1)

	// value exists but doesn't match so should return false
	result, err = coherence.TestReplaceMapping(ctx, session, cache, key, valueReplace, value)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(false))

	// now try replacing where exists and matches
	result, err = coherence.TestReplaceMapping(ctx, session, cache, key, value, valueNew)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(true))
}

// TestClearAndTruncate tests the clear and truncate requests.
func TestClearAndTruncate(t *testing.T) {
	var (
		g     = gomega.NewWithT(t)
		ctx   = context.Background()
		cache = "test-clear-and-truncate"
		err   error
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create a key of 1
	key := ensureSerializedInt32(g, 32)
	value := ensureSerializedString(g, "value")

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// put a value into the cache
	_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// put a value into the cache
	_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	err = coherence.TestTruncateCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)
}

// TestContainsKey tests the contains key request
func TestContainsKey(t *testing.T) {
	var (
		g           = gomega.NewWithT(t)
		ctx         = context.Background()
		cache       = "test-contains-key"
		containsKey bool
		err         error
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create a key of 1
	key := ensureSerializedInt32(g, 32)
	value := ensureSerializedString(g, "value")

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	containsKey, err = coherence.TestContainsKey(ctx, session, cache, key)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(containsKey).To(gomega.Equal(false))

	// put a value into the cache
	_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	containsKey, err = coherence.TestContainsKey(ctx, session, cache, key)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(containsKey).To(gomega.Equal(true))
}

// TestContainsEntry tests the contains key request
func TestContainsEntry(t *testing.T) {
	var (
		g             = gomega.NewWithT(t)
		ctx           = context.Background()
		cache         = "test-contains-entry"
		containsEntry bool
		err           error
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create a key of 1
	key := ensureSerializedInt32(g, 32)
	value := ensureSerializedString(g, "value")

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	containsEntry, err = coherence.TestContainsEntry(ctx, session, cache, key, value)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(containsEntry).To(gomega.Equal(false))

	// put a value into the cache
	_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	containsEntry, err = coherence.TestContainsEntry(ctx, session, cache, key, value)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(containsEntry).To(gomega.Equal(true))
}

// TestContainsValue tests the contains value request.
func TestContainsValue(t *testing.T) {
	var (
		g             = gomega.NewWithT(t)
		ctx           = context.Background()
		cache         = "test-contains-value"
		containsValue bool
		err           error
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create a key of 1
	key := ensureSerializedInt32(g, 32)
	value := ensureSerializedString(g, "value")

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	containsValue, err = coherence.TestContainsValue(ctx, session, cache, value)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(containsValue).To(gomega.Equal(false))

	// put a value into the cache
	_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 1)

	containsValue, err = coherence.TestContainsValue(ctx, session, cache, value)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(containsValue).To(gomega.Equal(true))
}

// TestGetAll tests the get all request.
func TestGetAll(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		ctx     = context.Background()
		cache   = "test-get-all"
		err     error
		entries = generateEntries(g, 100)
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	key1 := ensureSerializedInt32(g, 1)
	key2 := ensureSerializedInt32(g, 2)

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	err = coherence.TestPutAll(ctx, session, cache, entries, 0)
	g.Expect(err).Should(gomega.BeNil())
	assertSize(g, session, cache, 100)

	keys := make([][]byte, 2)
	keys[0] = key1
	keys[1] = key2

	ch, err := coherence.TestGetAll(ctx, session, cache, keys)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	for v := range ch {
		g.Expect(v.Err).ShouldNot(gomega.HaveOccurred())
		key := ensureDeserializedInt32(g, v.Key)
		val := ensureDeserializedString(g, v.Value)
		g.Expect(key).ShouldNot(gomega.BeNil())
		g.Expect(val).ShouldNot(gomega.BeNil())
	}
}

// TestGetAll tests the get all request.
func TestKeyAndValuePage(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		ctx     = context.Background()
		cache   = "test-key-and-value-page"
		err     error
		entries = generateEntries(g, 100)
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	err = coherence.TestPutAll(ctx, session, cache, entries, 0)
	g.Expect(err).Should(gomega.BeNil())
	assertSize(g, session, cache, 100)

	cookie := make([]byte, 0)

	ch, err := coherence.TestKeyAndValuePage(ctx, session, cache, cookie)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	for v := range ch {
		g.Expect(v.Err).ShouldNot(gomega.HaveOccurred())
		if len(v.Cookie) == 0 {
			key := ensureDeserializedInt32(g, v.Key)
			val := ensureDeserializedString(g, v.Value)
			g.Expect(key).ShouldNot(gomega.BeNil())
			g.Expect(val).ShouldNot(gomega.BeNil())
		}
	}
}

// TestInvoke tests the invoke request.
func TestInvoke(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		ctx     = context.Background()
		cache   = "test-invoke"
		err     error
		entries = generateEntries(g, 100)
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	binProcessor := ensureSerializedAny(g, processors.Preload())

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	err = coherence.TestPutAll(ctx, session, cache, entries, 0)
	g.Expect(err).Should(gomega.BeNil())
	assertSize(g, session, cache, 100)

	keys := makeInt32Keys(g, 3)

	keysOrFilterKeys := &pb1.KeysOrFilter_Keys{Keys: &pb1.CollectionOfBytesValues{Values: keys}}
	keysOrFilter := &pb1.KeysOrFilter{KeyOrFilter: keysOrFilterKeys}

	ch, err := coherence.TestInvoke(ctx, session, cache, binProcessor, keysOrFilter)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	for v := range ch {
		g.Expect(v.Err).ShouldNot(gomega.HaveOccurred())
	}
}

// TestDestroyCache tests the destroy cache request.
func TestDestroyCache(t *testing.T) {
	var (
		g          = gomega.NewWithT(t)
		ctx        = context.Background()
		validCache = "destroy-cache"
		err        error
	)

	session := getTestSession(t, g)
	defer session.Close()

	// get a unique cache name which should never exist
	nonExistentCache := uuid.New().String()
	err = coherence.TestDestroyCache(ctx, session, nonExistentCache)
	g.Expect(err).To(gomega.HaveOccurred())

	_ = ensureCache(g, session, validCache)

	// destroy to the valid cache should work
	err = coherence.TestDestroyCache(ctx, session, validCache)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// cache id should not exist
	cacheID := coherence.GetSessionCacheID(session, validCache)
	g.Expect(cacheID).To(gomega.BeNil())
}

// TestAggregate tests the aggregate request.
func TestAggregate(t *testing.T) {
	var (
		g      = gomega.NewWithT(t)
		ctx    = context.Background()
		cache  = "aggregate"
		err    error
		count  = 10
		result *[]byte
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	value := ensureSerializedInt32(g, int32(1))

	for i := 0; i < count; i++ {
		key := ensureSerializedInt32(g, int32(i))
		_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
		g.Expect(err).Should(gomega.BeNil())
	}

	assertSize(g, session, cache, int32(10))

	binAggregator := ensureSerializedAny(g, aggregators.Count())
	binFilter := ensureSerializedAny(g, filters.Always())

	// test filter
	keysOrFilterFilter := &pb1.KeysOrFilter_Filter{Filter: binFilter}
	keysOrFilter := &pb1.KeysOrFilter{KeyOrFilter: keysOrFilterFilter}

	result, err = coherence.TestAggregate(ctx, session, cache, binAggregator, keysOrFilter)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(result).ShouldNot(gomega.BeNil())

	// deserialized value should be int64(10)
	g.Expect(ensureDeserializedInt64(g, *result)).To(gomega.Equal(int64(10)))

	// test keys
	keys := makeInt32Keys(g, 2)

	keysOrFilterKeys := &pb1.KeysOrFilter_Keys{Keys: &pb1.CollectionOfBytesValues{Values: keys}}
	keysOrFilter = &pb1.KeysOrFilter{KeyOrFilter: keysOrFilterKeys}

	result, err = coherence.TestAggregate(ctx, session, cache, binAggregator, keysOrFilter)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(result).ShouldNot(gomega.BeNil())

	// deserialized value should be int64(2) as we have 2 keys
	g.Expect(ensureDeserializedInt64(g, *result)).To(gomega.Equal(int64(2)))

}

func makeInt32Keys(g *gomega.WithT, count int) [][]byte {
	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = ensureSerializedInt32(g, int32(i+1))
	}
	return keys
}

// TestGoRoutines tests multiple go routines to ensure that we have no locking issues.
func TestGoRoutines(t *testing.T) {
	var (
		g  = gomega.NewWithT(t)
		wg sync.WaitGroup
	)

	session := getTestSession(t, g)
	defer session.Close()

	wg.Add(3)
	go func() {
		defer wg.Done()
		runInsertTest(g, session, "cache-1")
	}()

	go func() {
		defer wg.Done()
		runInsertTest(g, session, "cache-2")
	}()

	go func() {
		defer wg.Done()
		runInsertTest(g, session, "cache-3")
	}()

	wg.Wait()
}

// runInsertTest runs a test to insert then remove entries to test multiple go routines.
func runInsertTest(g *gomega.WithT, session *coherence.Session, cache string) {
	var (
		err   error
		ctx   = context.Background()
		value = ensureSerializedString(g, "value")
		count = 1_000
	)

	_ = ensureCache(g, session, cache)

	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	for i := 0; i < count; i++ {
		key := ensureSerializedInt32(g, int32(i))
		_, err = coherence.TestPut(ctx, session, cache, key, value, 0)
		g.Expect(err).Should(gomega.BeNil())
	}

	assertSize(g, session, cache, int32(count))

	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, int32(0))
}

func assertSize(g *gomega.WithT, session *coherence.Session, cache string, expectedSize int32) {
	ctx := context.Background()

	size, err := coherence.TestSize(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(size).Should(gomega.Equal(expectedSize))

	// check the isEmpty matches
	empty, err := coherence.TestIsEmpty(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(empty).To(gomega.Equal(expectedSize == 0))
}

func getTestSession(t *testing.T, g *gomega.WithT) *coherence.Session {
	t.Setenv("COHERENCE_SESSION_DEBUG", "true")
	t.Setenv("COHERENCE_GRPCV1_DEBUG", "true")

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	return session
}

func ensureSerializedInt32(g *gomega.WithT, v int32) []byte {
	value, err := serializerInt32.Serialize(v)
	g.Expect(err).Should(gomega.BeNil())
	return value
}

func ensureSerializedString(g *gomega.WithT, v string) []byte {
	value, err := serializerString.Serialize(v)
	g.Expect(err).Should(gomega.BeNil())
	return value
}

func ensureSerializedAny(g *gomega.WithT, v any) []byte {
	value, err := serializerAny.Serialize(v)
	g.Expect(err).Should(gomega.BeNil())
	return value
}

func ensureDeserializedInt32(g *gomega.WithT, data []byte) int32 {
	value, err := serializerInt32.Deserialize(data)
	g.Expect(err).Should(gomega.BeNil())
	return *value
}

func ensureDeserializedInt64(g *gomega.WithT, data []byte) int64 {
	value, err := serializerInt64.Deserialize(data)
	g.Expect(err).Should(gomega.BeNil())
	return *value
}

func ensureDeserializedString(g *gomega.WithT, data []byte) string {
	value, err := serializerString.Deserialize(data)
	g.Expect(err).Should(gomega.BeNil())
	return *value
}

func ensureCache(g *gomega.WithT, session *coherence.Session, cache string) *int32 {
	ctx := context.Background()

	cacheID, err := coherence.TestEnsureCache(ctx, session, cache)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(cacheID).ShouldNot(gomega.BeNil())

	id := coherence.GetSessionCacheID(session, cache)
	g.Expect(id).ShouldNot(gomega.BeNil())
	g.Expect(*id).To(gomega.Equal(*cacheID))

	ready, err := coherence.TestIsReady(ctx, session, cache)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(ready).To(gomega.BeTrue())

	return cacheID
}

func generateEntries(g *gomega.WithT, count int) []*pb1.BinaryKeyAndValue {
	entries := make([]*pb1.BinaryKeyAndValue, 0)

	// populate the entries
	for i := 1; i <= count; i++ {
		entries = append(entries, &pb1.BinaryKeyAndValue{
			Key:   ensureSerializedInt32(g, int32(i)),
			Value: ensureSerializedString(g, fmt.Sprintf("value-%v", i)),
		})
	}
	return entries
}
