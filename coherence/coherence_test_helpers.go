/*
 * Copyright (c) 2024, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	pb1 "github.com/oracle/coherence-go-client/v2/proto/v1"
	"time"
)

// test helpers only for internal use only.

// TestEnsureCache is only exported for integration tests, not for general use.
func TestEnsureCache(ctx context.Context, session *Session, cache string) (*int32, error) {
	return session.v1StreamManagerCache.ensureCache(ctx, cache)
}

// TestClearCache is only exported for integration tests, not for general use.
func TestClearCache(ctx context.Context, session *Session, cache string) error {
	return session.v1StreamManagerCache.clearCache(ctx, cache)
}

// TestRemoveMapping is only exported for integration tests, not for general use.
func TestRemoveMapping(ctx context.Context, session *Session, cache string, key []byte, value []byte) (bool, error) {
	return session.v1StreamManagerCache.removeMapping(ctx, cache, key, value)
}

// TestReplaceMapping is only exported for integration tests, not for general use.
func TestReplaceMapping(ctx context.Context, session *Session, cache string, key []byte, prevValue []byte, newValue []byte) (bool, error) {
	return session.v1StreamManagerCache.replaceMapping(ctx, cache, key, prevValue, newValue)
}

// TestReplace is only exported for integration tests, not for general use.
func TestReplace(ctx context.Context, session *Session, cache string, key []byte, value []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.replace(ctx, cache, key, value)
}

// TestTruncateCache is only exported for integration tests, not for general use.
func TestTruncateCache(ctx context.Context, session *Session, cache string) error {
	return session.v1StreamManagerCache.truncateCache(ctx, cache)
}

// TestDestroyCache is only exported for integration tests, not for general use.
func TestDestroyCache(ctx context.Context, session *Session, cache string) error {
	return session.v1StreamManagerCache.destroyCache(ctx, cache)
}

// TestSize is only exported for integration tests, not for general use.
func TestSize(ctx context.Context, session *Session, cache string) (int32, error) {
	return session.v1StreamManagerCache.size(ctx, cache)
}

// TestIsEmpty is only exported for integration tests, not for general use.
func TestIsEmpty(ctx context.Context, session *Session, cache string) (bool, error) {
	return session.v1StreamManagerCache.isEmpty(ctx, cache)
}

// TestIsReady is only exported for integration tests, not for general use.
func TestIsReady(ctx context.Context, session *Session, cache string) (bool, error) {
	return session.v1StreamManagerCache.isReady(ctx, cache)
}

// TestContainsKey is only exported for integration tests, not for general use.
func TestContainsKey(ctx context.Context, session *Session, cache string, key []byte) (bool, error) {
	return session.v1StreamManagerCache.containsKey(ctx, cache, key)
}

// TestContainsValue is only exported for integration tests, not for general use.
func TestContainsValue(ctx context.Context, session *Session, cache string, value []byte) (bool, error) {
	return session.v1StreamManagerCache.containsValue(ctx, cache, value)
}

// TestContainsEntry is only exported for integration tests, not for general use.
func TestContainsEntry(ctx context.Context, session *Session, cache string, key []byte, value []byte) (bool, error) {
	return session.v1StreamManagerCache.containsEntry(ctx, cache, key, value)
}

// TestGet is only exported for integration tests, not for general use.
func TestGet(ctx context.Context, session *Session, cache string, key []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.get(ctx, cache, key)
}

// TestGetAll is only exported for integration tests, not for general use.
func TestGetAll(ctx context.Context, session *Session, cache string, keys [][]byte) (<-chan BinaryKeyAndValue, error) {
	return session.v1StreamManagerCache.getAll(ctx, cache, keys)
}

// TestKeyAndValuePage is only exported for integration tests, not for general use.
func TestKeyAndValuePage(ctx context.Context, session *Session, cache string, cookie []byte) (<-chan BinaryKeyAndValue, error) {
	return session.v1StreamManagerCache.keyAndValuePage(ctx, cache, cookie)
}

// TestPut is only exported for integration tests, not for general use.
func TestPut(ctx context.Context, session *Session, cache string, key []byte, value []byte, ttl time.Duration) (*[]byte, error) {
	return session.v1StreamManagerCache.put(ctx, cache, key, value, ttl)
}

// TestPutAll is only exported for integration tests, not for general use.
func TestPutAll(ctx context.Context, session *Session, cache string, entries []*pb1.BinaryKeyAndValue, ttl time.Duration) error {
	return session.v1StreamManagerCache.putAll(ctx, cache, entries, ttl)
}

// TestRemove is only exported for integration tests, not for general use.
func TestRemove(ctx context.Context, session *Session, cache string, key []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.remove(ctx, cache, key)
}

// TestPutIfAbsent is only exported for integration tests, not for general use.
func TestPutIfAbsent(ctx context.Context, session *Session, cache string, key []byte, value []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.putIfAbsent(ctx, cache, key, value)
}

// TestAggregate is only exported for integration tests, not for general use.
func TestAggregate(ctx context.Context, session *Session, cache string, agent []byte, keysOrFilter *pb1.KeysOrFilter) (*[]byte, error) {
	return session.v1StreamManagerCache.aggregate(ctx, cache, agent, keysOrFilter)
}

// TestInvoke is only exported for integration tests, not for general use.
func TestInvoke(ctx context.Context, session *Session, cache string, agent []byte, keysOrFilter *pb1.KeysOrFilter) (<-chan BinaryKeyAndValue, error) {
	return session.v1StreamManagerCache.invoke(ctx, cache, agent, keysOrFilter)
}

// TestMapListenerRequest is only exported for integration tests, not for general use.
func TestMapListenerRequest(ctx context.Context, session *Session, cache string, subscribe bool, keyOrFilter *pb1.KeyOrFilter,
	lite bool, synchronous bool, priming bool, filterID int64) error {
	return session.v1StreamManagerCache.mapListenerRequest(ctx, cache, subscribe, keyOrFilter, lite, synchronous, priming, filterID)
}

// GetSessionCacheID is only exported for integration tests, not for general use.
func GetSessionCacheID(session *Session, cache string) *int32 {
	return session.getCacheID(cache)
}

// GetSessionQueueID is only exported for integration tests, not for general use.
func GetSessionQueueID(session *Session, queue string) *int32 {
	return session.getQueueID(queue)
}

// GetNearCachePruneFactor is only exported for integration tests, not for general use.
func GetNearCachePruneFactor[K comparable, V any](namedMap NamedMap[K, V]) float32 {
	ncOptions := namedMap.getBaseClient().cacheOpts.NearCacheOptions
	if ncOptions == nil {
		return 0.0
	}

	return ncOptions.PruneFactor
}

// revive:disable:unexported-return
// GetKeyListenerGroupMap is only exported for integration tests, not for general use.
func GetKeyListenerGroupMap[K comparable, V any](namedMap NamedMap[K, V]) map[K]*listenerGroupV1[K, V] {
	return namedMap.getBaseClient().keyListenersV1
}

// revive:disable:unexported-return
// GetKeyListenerGroupListeners is only exported for integration tests, not for general use.
func GetKeyListenerGroupListeners[K comparable, V any](namedMap NamedMap[K, V], key K) []MapListener[K, V] {
	mapListeners := make([]MapListener[K, V], 0)
	listenerGroupMap := GetKeyListenerGroupMap[K, V](namedMap)
	if listeners, ok := listenerGroupMap[key]; ok {
		for k := range listeners.listeners {
			mapListeners = append(mapListeners, k)
		}
	}

	return mapListeners
}

// revive:disable:unexported-return
// GetFilterListenerGroupMap is only exported for integration tests, not for general use.
func GetFilterListenerGroupMap[K comparable, V any](namedMap NamedMap[K, V]) map[filters.Filter]*listenerGroupV1[K, V] {
	return namedMap.getBaseClient().filterListenersV1
}

// GetFilterListenerGroupListeners is only exported for integration tests, not for general use.
func GetFilterListenerGroupListeners[K comparable, V any](namedMap NamedMap[K, V], f filters.Filter) []MapListener[K, V] {
	mapListeners := make([]MapListener[K, V], 0)
	listenerGroupMap := GetFilterListenerGroupMap[K, V](namedMap)
	if listeners, ok := listenerGroupMap[f]; ok {
		for k := range listeners.listeners {
			mapListeners = append(mapListeners, k)
		}
	}

	return mapListeners
}
