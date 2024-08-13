/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	pb1 "github.com/oracle/coherence-go-client/proto/v1"
	"time"
)

// test helpers

//func SubmitRequest(session *Session, req *pb1.ProxyRequest) (namedCacheRequest, error) {
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

func TestGet(ctx context.Context, session *Session, cache string, key []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.get(ctx, cache, key)
}

func TestPut(ctx context.Context, session *Session, cache string, key []byte, value []byte, ttl time.Duration) (*[]byte, error) {
	return session.v1StreamManagerCache.put(ctx, cache, key, value, ttl)
}

func TestRemove(ctx context.Context, session *Session, cache string, key []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.remove(ctx, cache, key)
}

func TestPutIfAbsent(ctx context.Context, session *Session, cache string, key []byte, value []byte) (*[]byte, error) {
	return session.v1StreamManagerCache.putIfAbsent(ctx, cache, key, value, 0)
}

// GetSessionCacheID returns the cache id for a cache name
func GetSessionCacheID(session *Session, cache string) *int32 {
	return session.getCacheID(cache)
}

func GetCacheServiceProtocol() V1ProxyProtocol {
	return cacheServiceProtocol
}

func NewEnsureCacheRequest(session *Session, cache string) (*pb1.ProxyRequest, error) {
	return session.v1StreamManagerCache.newEnsureCacheRequest(cache)
}