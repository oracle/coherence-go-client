/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var _ LocalCache[string, string] = &localCache[string, string]{}

// LocalCache implements a local cache of values.
type LocalCache[K comparable, V any] interface {
	Put(key K, value V) *V
	PutWithExpiry(key K, value V, ttl time.Duration) *V
	Get(key K) *V
	Remove(key K) *V
	Size() int
	Clear()
	Release()
	GetStats() CacheStats
}

type localCache[K comparable, V any] struct {
	Name              string
	options           *LocalCacheOptions
	data              sync.Map
	cacheStats        CacheStats
	cacheHits         int64
	cacheMisses       int64
	cacheMissesMillis int64
	cachePuts         int64
	cachePrunes       int64
	cachePrunesMillis int64
}

type localCacheEntry[K comparable, V any] struct {
	key        K
	value      V
	ttl        time.Duration
	insertTime time.Time
	//lastAccess time.Time
}

// Put associates the specified value with the specified key returning the previously
// mapped value. V will be nil if there was no previous value.
func (l *localCache[K, V]) Put(key K, value V) *V {
	return l.PutWithExpiry(key, value, l.options.Expiry)
}

// PutWithExpiry associates the specified value with the specified key. If the cache
// previously contained a value for this key, the old value is replaced.
// This variation of the Put()
// function allows the caller to specify an expiry (or "time to live")
// for the cache entry. V will be nil if there was no previous value.
func (l *localCache[K, V]) PutWithExpiry(key K, value V, ttl time.Duration) *V {
	l.checkExpiry()

	defer l.registerPut()

	newEntry := newLocalCacheEntry[K, V](key, value, ttl)

	prev, loaded := l.data.Swap(key, newEntry)
	if loaded {
		return &prev.(*localCacheEntry[K, V]).value
	}
	return nil
}

// Get returns the value to which the specified key is mapped. V will be nil if there was no previous value.
func (l *localCache[K, V]) Get(key K) *V {
	l.checkExpiry()

	v, ok := l.data.Load(key)
	if !ok {
		return nil
	}

	return &v.(*localCacheEntry[K, V]).value
}

// Remove removes the mapping for a key from the cache if it is present and returns the previously
// mapped value, if any. V will be nil if there was no previous value.
func (l *localCache[K, V]) Remove(key K) *V {
	l.checkExpiry()

	prev, loaded := l.data.LoadAndDelete(key)
	if loaded {
		return &prev.(*localCacheEntry[K, V]).value
	}
	return nil
}

// Size returns the number of mappings contained within the cache.
func (l *localCache[K, V]) Size() int {
	l.checkExpiry()
	size := 0
	l.data.Range(func(key, value any) bool {
		size++
		return true
	})
	return size
}

// Clear removes all mappings from the cache.
func (l *localCache[K, V]) Clear() {
	l.data.Range(func(key, value any) bool {
		l.data.Delete(key)
		return true
	})
}

// Release releases the cache.
func (l *localCache[K, V]) Release() {
	l.Clear()
}

func (l *localCache[K, V]) GetStats() CacheStats {
	return l.cacheStats
}

// checkExpiry goes through the map to see if any entries have expired and remove them.
// TODO: Size based eviction
func (l *localCache[K, V]) checkExpiry() {
	start := time.Now()
	var prunes int64

	l.data.Range(func(key, value any) bool {
		entry := value.(*localCacheEntry[K, V])
		if entry.ttl > 0 && time.Since(entry.insertTime) > entry.ttl {
			l.data.Delete(key)
			prunes++
		}
		return true
	})

	if prunes > 0 {
		l.registerPrune(time.Since(start).Milliseconds())
	}
}

func newLocalCacheEntry[K comparable, V any](key K, value V, ttl time.Duration) *localCacheEntry[K, V] {
	return &localCacheEntry[K, V]{
		key:        key,
		value:      value,
		ttl:        ttl,
		insertTime: time.Now(),
	}
}

func newLocalCache[K comparable, V any](name string, options ...func(localCache *LocalCacheOptions)) *localCache[K, V] {
	cache := &localCache[K, V]{
		Name: name,
		data: sync.Map{},
		options: &LocalCacheOptions{
			Expiry:    0,
			HighUnits: 0,
		},
	}

	// apply any options
	for _, f := range options {
		f(cache.options)
	}

	return cache
}

// LocalCacheOptions defines options for a local cache.
type LocalCacheOptions struct {
	Expiry    time.Duration
	HighUnits int64
}

func (o *LocalCacheOptions) String() string {
	return fmt.Sprintf("LocalCacheOptions{expiry=%v, highUnits=%d}", o.Expiry, o.HighUnits)
}

// WithLocalCacheExpiry returns a function to set the expiry time for a local cache.
func WithLocalCacheExpiry(ttl time.Duration) func(options *LocalCacheOptions) {
	return func(o *LocalCacheOptions) {
		o.Expiry = ttl
	}
}

// WithLocalCacheHighUnits returns a function to set the high units for a local cache.
func WithLocalCacheHighUnits(highUnits int64) func(options *LocalCacheOptions) {
	return func(o *LocalCacheOptions) {
		o.HighUnits = highUnits
	}
}

var _ CacheStats = &localCache[string, string]{}

type CacheStats interface {
	GetCacheHits() int64
	GetCacheMisses() int64
	GetCacheMissesMillis() int64
	GetCachePuts() int64
	GetCachePrunes() int64
	GetCachePrunesMillis() int64
	GetTotalGets() int64
	GetHitRate() float32
	Size() int
	ResetStats()
}

func (l *localCache[K, V]) registerHit() {
	atomic.AddInt64(&l.cacheHits, 1)
}

func (l *localCache[K, V]) registerMiss() {
	atomic.AddInt64(&l.cacheMisses, 1)
}

func (l *localCache[K, V]) registerPut() {
	atomic.AddInt64(&l.cachePuts, 1)
}

func (l *localCache[K, V]) registerPrune(millis int64) {
	atomic.AddInt64(&l.cachePrunes, 1)
	atomic.AddInt64(&l.cachePrunesMillis, millis)
}

func (l *localCache[K, V]) registerMissesMillis(millis int64) {
	atomic.AddInt64(&l.cacheMissesMillis, millis)
}

func (l *localCache[K, V]) GetCacheHits() int64 {
	return l.cacheHits
}

func (l *localCache[K, V]) GetCacheMisses() int64 {
	return l.cacheMisses
}

func (l *localCache[K, V]) GetCacheMissesMillis() int64 {
	return l.cacheMissesMillis
}

func (l *localCache[K, V]) GetCachePuts() int64 {
	return l.cachePuts
}

func (l *localCache[K, V]) GetCachePrunes() int64 {
	return l.cachePrunes
}

func (l *localCache[K, V]) GetCachePrunesMillis() int64 {
	return l.cachePrunesMillis
}

func (l *localCache[K, V]) GetTotalGets() int64 {
	return l.GetCacheHits() + l.GetCacheMisses()
}

func (l *localCache[K, V]) GetHitRate() float32 {
	total := l.cacheHits + l.cacheMisses
	if total == 0 {
		return 0.0
	}
	return float32(l.cacheHits) / float32(total)
}

func (l *localCache[K, V]) ResetStats() {
	atomic.StoreInt64(&l.cachePrunesMillis, 0)
	atomic.StoreInt64(&l.cacheMissesMillis, 0)
	atomic.StoreInt64(&l.cachePrunes, 0)
	atomic.StoreInt64(&l.cacheHits, 0)
	atomic.StoreInt64(&l.cacheMisses, 0)
	atomic.StoreInt64(&l.cachePuts, 0)
}

func (l *localCache[K, V]) String() string {
	return fmt.Sprintf("LocalCache{name=%s, options=%v, stats=CacheStats{puts=%v, gets=%v, hits=%v, misses=%v, missesMillis=%v, hitRate=%v, prunes=%v, prunesMillis=%v}}",
		l.Name, l.options, l.GetCachePuts(), l.GetTotalGets(), l.GetCacheHits(), l.GetCacheMisses(),
		l.GetCacheMissesMillis(), l.GetHitRate()*100, l.GetCachePrunes(), l.GetCachePrunesMillis())
}
