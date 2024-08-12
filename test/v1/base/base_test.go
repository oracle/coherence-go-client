/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package base

import (
	"context"
	"github.com/google/uuid"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/test/utils"
	"sync"
	"testing"
	"time"
)

var (
	serializerInt32  = coherence.NewSerializer[int32]("json")
	serializerString = coherence.NewSerializer[string]("json")
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
	key := ensureSerializedInt32(g, 32)
	value := ensureSerializedString(g, "value")

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

	// issue a get, and we should get back the correct value
	currentValue, err = coherence.TestGet(ctx, session, cache, key)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(currentValue).ShouldNot(gomega.BeNil())

	getValue, err = serializerString.Deserialize(*currentValue)
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(*getValue).Should(gomega.Equal("value"))
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

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	assertSize(g, session, cache, 0)

	// put a value into the cache
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

	timeout := time.Duration(300) * time.Second
	session, err := utils.GetSession(coherence.WithRequestTimeout(timeout), coherence.WithReadyTimeout(timeout))
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
