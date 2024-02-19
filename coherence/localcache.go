/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"fmt"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var _ LocalCache[string, string] = &localCache[string, string]{}

const prunePercent = 20

// LocalCache implements a local cache of values.
type LocalCache[K comparable, V any] interface {
	Put(key K, value V) *V
	PutWithExpiry(key K, value V, ttl time.Duration) *V
	Get(key K) *V
	GetAll(keys []K) map[K]*V
	Remove(key K) *V
	Size() int
	Clear()
	Release()
	GetStats() CacheStats
}

type localCache[K comparable, V any] struct {
	Name    string
	options *localCacheOptions
	sync.Mutex
	data              map[K]*localCacheEntry[K, V]
	cacheStats        CacheStats
	cacheHits         int64
	cacheMisses       int64
	cacheMissesNannos int64
	cachePuts         int64
	cachePrunes       int64
	cachePrunesNannos int64
}

type localCacheEntry[K comparable, V any] struct {
	key        K
	value      V
	ttl        time.Duration
	insertTime time.Time
	lastAccess time.Time
}

type pair[K comparable] struct {
	key       K
	timeStamp time.Time
}

type pairList[K comparable] []pair[K]

func (p pairList[K]) Len() int      { return len(p) }
func (p pairList[K]) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p pairList[K]) Less(i, j int) bool {
	return p[i].timeStamp.Nanosecond() < p[j].timeStamp.Nanosecond()
}

// Put associates the specified value with the specified key returning the previously
// mapped value. V will be nil if there was no previous value.
func (l *localCache[K, V]) Put(key K, value V) *V {
	return l.PutWithExpiry(key, value, l.options.TTL)
}

// PutWithExpiry associates the specified value with the specified key. If the cache
// previously contained a value for this key, the old value is replaced.
// This variation of the Put()
// function allows the caller to specify an expiry (or "time to live")
// for the cache entry. V will be nil if there was no previous value.
func (l *localCache[K, V]) PutWithExpiry(key K, value V, ttl time.Duration) *V {
	defer l.registerPut()

	l.Lock()
	defer l.Unlock()

	l.pruneEntries()

	newEntry := newLocalCacheEntry[K, V](key, value, ttl)

	prev, ok := l.data[key]

	l.data[key] = newEntry

	if ok {
		return &prev.value
	}
	return nil
}

// Get returns the value to which the specified key is mapped. V will be nil if there was no previous value.
func (l *localCache[K, V]) Get(key K) *V {
	l.Lock()
	defer l.Unlock()

	l.expireEntries()

	v, ok := l.data[key]
	if !ok {
		return nil
	}

	v.lastAccess = time.Now()

	return &v.value
}

// GetAll returns the entries for each key if it exists.
func (l *localCache[K, V]) GetAll(keys []K) map[K]*V {
	l.Lock()
	defer l.Unlock()

	l.pruneEntries()

	results := make(map[K]*V, 0)

	for _, key := range keys {
		v, ok := l.data[key]
		if ok {
			// have entry so add to the results
			results[key] = &v.value
			v.lastAccess = time.Now()
		}
	}

	return results
}

// Remove removes the mapping for a key from the cache if it is present and returns the previously
// mapped value, if any. V will be nil if there was no previous value.
func (l *localCache[K, V]) Remove(key K) *V {
	l.Lock()
	defer l.Unlock()

	l.expireEntries()

	v, ok := l.data[key]

	if ok {
		delete(l.data, key)
		return &v.value
	}

	return nil
}

// Size returns the number of mappings contained within the cache.
func (l *localCache[K, V]) Size() int {
	l.Lock()
	defer l.Unlock()

	l.expireEntries()

	return len(l.data)
}

// Clear removes all mappings from the cache.
func (l *localCache[K, V]) Clear() {
	l.Lock()
	defer l.Unlock()

	l.data = make(map[K]*localCacheEntry[K, V], 0)
}

// Release releases the cache.
func (l *localCache[K, V]) Release() {
	l.Clear()
}

func (l *localCache[K, V]) GetStats() CacheStats {
	return l.cacheStats
}

// expireEntries goes through the map to see if any entries have expired due to ttl.
func (l *localCache[K, V]) expireEntries() {
	var (
		prunes       int64
		keysToDelete = make([]K, 0)
		start        = time.Now()
	)

	// Check 1 - check for cache expiry
	for k, v := range l.data {
		if v.ttl > 0 && time.Since(v.insertTime) > v.ttl {
			keysToDelete = append(keysToDelete, k)
			prunes++
		}
	}

	// delete all the keys that were flagged from the expiry, this may be enough to free up space
	for _, k := range keysToDelete {
		delete(l.data, k)
	}

	if len(keysToDelete) > 0 {
		l.registerPruneNanos(time.Since(start).Nanoseconds())
	}
}

// pruneEntries goes through the map to see if any entries have expired or size is reached and remove them.
func (l *localCache[K, V]) pruneEntries() {
	var (
		start              = time.Now()
		currentCacheSize   int64
		currentCacheMemory int64
	)

	l.expireEntries()

	currentCacheSize = int64(len(l.data))

	if l.options.HighUnitsMemory > 0 {
		currentCacheMemory = l.getMemoryOfMapEntries()
	}

	// if highUnits or highUnitsMemory are set then check
	if (l.options.HighUnits > 0 && currentCacheSize+1 > l.options.HighUnits) ||
		(l.options.HighUnitsMemory > 0 && currentCacheMemory+1 > l.options.HighUnitsMemory) {

		defer l.registerPruneNanos(time.Since(start).Nanoseconds())

		entriesToDelete := int(math.Round(float64(currentCacheSize * prunePercent / 100.0)))

		// prune to default of 80% of the cache size.
		// we first sort the map by lastAccess time / then insert time, so we remove all
		// entries firstly that have never been accessed.
		index := 0
		sortData := make(pairList[K], len(l.data))

		for k, v := range l.data {
			var timestamp = v.lastAccess

			if !timestamp.IsZero() {
				if v.insertTime.Nanosecond() < v.lastAccess.Nanosecond() {
					timestamp = v.insertTime
				} else {
					timestamp = v.lastAccess
				}
			}
			sortData[index] = pair[K]{key: k, timeStamp: timestamp}

			index++
		}

		sort.Sort(sortData)

		for i, v := range sortData {
			if i > entriesToDelete {
				break
			}
			delete(l.data, v.key)
		}
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

func newLocalCache[K comparable, V any](name string, options ...func(localCache *localCacheOptions)) *localCache[K, V] {
	cache := &localCache[K, V]{
		Name: name,
		data: make(map[K]*localCacheEntry[K, V], 0),
		options: &localCacheOptions{
			TTL:             0,
			HighUnits:       0,
			HighUnitsMemory: 0,
		},
	}

	// apply any options
	for _, f := range options {
		f(cache.options)
	}

	return cache
}

// localCacheOptions defines options for a local cache.
type localCacheOptions struct {
	TTL             time.Duration
	HighUnits       int64
	HighUnitsMemory int64
}

func (o *localCacheOptions) String() string {
	return fmt.Sprintf("localCacheOptions{ttl=%v, highUnits=%d, highUnitsMemory=%v}", o.TTL, o.HighUnits, formatMemory(o.HighUnitsMemory))
}

// withLocalCacheExpiry returns a function to set the expiry time for a local cache.
func withLocalCacheExpiry(ttl time.Duration) func(options *localCacheOptions) {
	return func(o *localCacheOptions) {
		o.TTL = ttl
	}
}

// withLocalCacheHighUnits returns a function to set the high units for a local cache.
func withLocalCacheHighUnits(highUnits int64) func(options *localCacheOptions) {
	return func(o *localCacheOptions) {
		o.HighUnits = highUnits
	}
}

// withLocalCacheHighUnitsMemory returns a function to set the high units as memory for a local cache.
func withLocalCacheHighUnitsMemory(highUnitsMemory int64) func(options *localCacheOptions) {
	return func(o *localCacheOptions) {
		o.HighUnitsMemory = highUnitsMemory
	}
}

var _ CacheStats = &localCache[string, string]{}

type CacheStats interface {
	GetCacheHits() int64
	GetCacheMisses() int64
	GetCacheMissesDuration() time.Duration
	GetCachePuts() int64
	GetCachePrunes() int64
	GetCachePrunesDuration() time.Duration
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

func (l *localCache[K, V]) registerPruneNanos(nanos int64) {
	atomic.AddInt64(&l.cachePrunes, 1)
	atomic.AddInt64(&l.cachePrunesNannos, nanos)
}

func (l *localCache[K, V]) registerMissesNanos(nanos int64) {
	atomic.AddInt64(&l.cacheMissesNannos, nanos)
}

func (l *localCache[K, V]) GetCacheHits() int64 {
	return l.cacheHits
}

func (l *localCache[K, V]) GetCacheMisses() int64 {
	return l.cacheMisses
}

func (l *localCache[K, V]) GetCacheMissesDuration() time.Duration {
	return time.Duration(l.cacheMissesNannos) * time.Nanosecond
}

func (l *localCache[K, V]) GetCachePuts() int64 {
	return l.cachePuts
}

func (l *localCache[K, V]) GetCachePrunes() int64 {
	return l.cachePrunes
}

func (l *localCache[K, V]) GetCachePrunesDuration() time.Duration {
	return time.Duration(l.cachePrunesNannos) * time.Nanosecond
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
	atomic.StoreInt64(&l.cachePrunesNannos, 0)
	atomic.StoreInt64(&l.cacheMissesNannos, 0)
	atomic.StoreInt64(&l.cachePrunes, 0)
	atomic.StoreInt64(&l.cacheHits, 0)
	atomic.StoreInt64(&l.cacheMisses, 0)
	atomic.StoreInt64(&l.cachePuts, 0)
}

func (l *localCache[K, V]) String() string {
	return fmt.Sprintf("LocalCache{name=%s, options=%v, stats=CacheStats{puts=%v, gets=%v, hits=%v, misses=%v, "+
		"missesDuration=%v, hitRate=%v, prunes=%v, prunesDuration=%v, size=%v, memoryUsed=%v}}",
		l.Name, l.options, l.GetCachePuts(), l.GetTotalGets(), l.GetCacheHits(), l.GetCacheMisses(),
		l.GetCacheMissesDuration(), l.GetHitRate()*100, l.GetCachePrunes(), l.GetCachePrunesDuration(), l.Size(), formatMemory(l.getMemoryOfMapEntries()))
}

func (l *localCache[K, V]) getMemoryOfMapEntries() int64 {
	var memorySize int64
	for k, v := range l.data {
		memorySize = memorySize + int64(unsafe.Sizeof(k)) + int64(unsafe.Sizeof(v))
	}

	return memorySize
}

const (
	KB = 1024
	MB = KB * KB
	GB = MB * KB
)

func formatMemory(bytesValue int64) string {
	var printer = message.NewPrinter(language.English)
	if bytesValue < MB {
		return printer.Sprintf("%-0d KB", bytesValue/1024)
	}
	if bytesValue < GB {
		return printer.Sprintf("%-0d MB", bytesValue/1024/1024)
	}
	return printer.Sprintf("%-.1f GB", float64(bytesValue)/1024/1024/1024)
}
