/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
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

// test helpers

//func SubmitRequest(session *Session, req *pb1.ProxyRequest) (proxyRequestChannel, error) {
//	return session.v1StreamManagerCache.submitRequest(req)
//}

func TestEnsureCache(ctx context.Context, session *Session, cache string) (*int32, error) {
	return session.v1StreamManagerCache.ensureCache(ctx, cache)
}

func TestClearCache(ctx context.Context, session *Session, cache string) error {
	return session.v1StreamManagerCache.clearCache(ctx, cache)
}

func TestRemoveMapping(ctx context.Context, session *Session, cache string, key []byte, value []byte) (bool, error) {
	return session.v1StreamManagerCache.removeMapping(ctx, cache, key, value)
}

func TestReplaceMapping(ctx context.Context, session *Session, cache string, key []byte, prevValue []byte, newValue []byte) (bool, error) {
	return session.v1StreamManagerCache.replaceMapping(ctx, cache, key, prevValue, newValue)
}

func TestReplace(ctx context.Context, session *Session, cache string, key []byte, value []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.replace(ctx, cache, key, value)
}

func TestTruncateCache(ctx context.Context, session *Session, cache string) error {
	return session.v1StreamManagerCache.truncateCache(ctx, cache)
}

func TestDestroyCache(ctx context.Context, session *Session, cache string) error {
	return session.v1StreamManagerCache.destroyCache(ctx, cache)
}

func TestSize(ctx context.Context, session *Session, cache string) (int32, error) {
	return session.v1StreamManagerCache.size(ctx, cache)
}

func TestIsEmpty(ctx context.Context, session *Session, cache string) (bool, error) {
	return session.v1StreamManagerCache.isEmpty(ctx, cache)
}

func TestIsReady(ctx context.Context, session *Session, cache string) (bool, error) {
	return session.v1StreamManagerCache.isReady(ctx, cache)
}

func TestContainsKey(ctx context.Context, session *Session, cache string, key []byte) (bool, error) {
	return session.v1StreamManagerCache.containsKey(ctx, cache, key)
}

func TestContainsValue(ctx context.Context, session *Session, cache string, value []byte) (bool, error) {
	return session.v1StreamManagerCache.containsValue(ctx, cache, value)
}

func TestContainsEntry(ctx context.Context, session *Session, cache string, key []byte, value []byte) (bool, error) {
	return session.v1StreamManagerCache.containsEntry(ctx, cache, key, value)
}

func TestGet(ctx context.Context, session *Session, cache string, key []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.get(ctx, cache, key)
}

func TestGetAll(ctx context.Context, session *Session, cache string, keys [][]byte) (<-chan BinaryKeyAndValue, error) {
	return session.v1StreamManagerCache.getAll(ctx, cache, keys)
}

func TestKeyAndValuePage(ctx context.Context, session *Session, cache string, cookie []byte) (<-chan BinaryKeyAndValue, error) {
	return session.v1StreamManagerCache.keyAndValuePage(ctx, cache, cookie)
}

func TestPut(ctx context.Context, session *Session, cache string, key []byte, value []byte, ttl time.Duration) (*[]byte, error) {
	return session.v1StreamManagerCache.put(ctx, cache, key, value, ttl)
}

func TestPutAll(ctx context.Context, session *Session, cache string, entries []*pb1.BinaryKeyAndValue, ttl time.Duration) error {
	return session.v1StreamManagerCache.putAll(ctx, cache, entries, ttl)
}

func TestRemove(ctx context.Context, session *Session, cache string, key []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.remove(ctx, cache, key)
}

func TestPutIfAbsent(ctx context.Context, session *Session, cache string, key []byte, value []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.putIfAbsent(ctx, cache, key, value)
}

func TestAggregate(ctx context.Context, session *Session, cache string, agent []byte, keysOrFilter *pb1.KeysOrFilter) (*[]byte, error) {
	return session.v1StreamManagerCache.aggregate(ctx, cache, agent, keysOrFilter)
}

func TestInvoke(ctx context.Context, session *Session, cache string, agent []byte, keysOrFilter *pb1.KeysOrFilter) (<-chan BinaryKeyAndValue, error) {
	return session.v1StreamManagerCache.invoke(ctx, cache, agent, keysOrFilter)
}

func TestMapListenerRequest(ctx context.Context, session *Session, cache string, subscribe bool, keyOrFilter *pb1.KeyOrFilter,
	lite bool, synchronous bool, priming bool, filterID int64) error {
	return session.v1StreamManagerCache.mapListenerRequest(ctx, cache, subscribe, keyOrFilter, lite, synchronous, priming, filterID)
}

// GetSessionCacheID returns the cache id for a cache name
func GetSessionCacheID(session *Session, cache string) *int32 {
	return session.getCacheID(cache)
}

// GetSessionQueueID returns the queue id for a cache name
func GetSessionQueueID(session *Session, queue string) *int32 {
	return session.getQueueID(queue)
}

func GetNearCachePruneFactor[K comparable, V any](namedMap NamedMap[K, V]) float32 {
	ncOptions := namedMap.getBaseClient().cacheOpts.NearCacheOptions
	if ncOptions == nil {
		return 0.0
	}

	return ncOptions.PruneFactor
}

// revive:disable:unexported-return
func GetKeyListenerGroupMap[K comparable, V any](namedMap NamedMap[K, V]) map[K]*listenerGroupV1[K, V] {
	return namedMap.getBaseClient().keyListenersV1
}

// revive:disable:unexported-return
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
func GetFilterListenerGroupMap[K comparable, V any](namedMap NamedMap[K, V]) map[filters.Filter]*listenerGroupV1[K, V] {
	return namedMap.getBaseClient().filterListenersV1
}

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

func GetCacheServiceProtocol() V1ProxyProtocol {
	return cacheServiceProtocol
}

func NewEnsureCacheRequest(session *Session, cache string) (*pb1.ProxyRequest, error) {
	return session.v1StreamManagerCache.newEnsureCacheRequest(cache)
}
