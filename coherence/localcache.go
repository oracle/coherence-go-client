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

var (
	_ localCache[string, string] = &localCacheImpl[string, string]{}
	_ CacheStats                 = &localCacheImpl[string, string]{}
)

const (
	KB                         = 1024
	MB                         = KB * KB
	GB                         = MB * KB
	defaultPruneFactor float32 = 0.8 // prune to 80%
)

// localCache implements a local cache of values.
type localCache[K comparable, V any] interface {
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

// CacheStats contains various statistics for near caches.
type CacheStats interface {
	GetCacheHits() int64                    // the number of entries served from the near cache
	GetCacheMisses() int64                  // the number of entries that had to be retrieved from the cluster
	GetCacheMissesDuration() time.Duration  // the total duration of all misses
	GetHitRate() float32                    // the hit rate of the near cache
	GetCachePuts() int64                    // the number of entries put in the near cache
	GetTotalGets() int64                    // the number of gets against the near cache
	GetCachePrunes() int64                  // the number of times the near cache was pruned
	GetCachePrunesDuration() time.Duration  // the duration of all prunes
	GetCacheExpires() int64                 // the number of times the near cache had expiry event
	GetCacheExpiresDuration() time.Duration // the duration of all expires
	Size() int                              // the number of entries in the near cache
	SizeBytes() int64                       // the number of bytes used by the entries (keys and values) in the near cache
	ResetStats()                            // reset the stats for the near cache, not including Size() or SizeBytes()
}

type localCacheImpl[K comparable, V any] struct {
	Name    string
	options *localCacheOptions
	sync.Mutex
	data               map[K]*localCacheEntry[K, V]
	expiryMap          map[int64]*[]K
	nextExpiry         time.Time
	cacheHits          int64
	cacheMisses        int64
	cacheMissesNannos  int64
	cachePuts          int64
	cachePrunes        int64
	cachePrunesNannos  int64
	cacheExpires       int64
	cacheExpiresNannos int64
	cacheMemory        int64
}

type localCacheEntry[K comparable, V any] struct {
	key        K
	value      V
	ttl        time.Duration
	insertTime time.Time
	lastAccess time.Time
	expiresAt  time.Time
}

type pair[K comparable] struct {
	key       K
	timeStamp time.Time
}

type pairList[K comparable] []pair[K]

func (p pairList[K]) Len() int {
	return len(p)
}

func (p pairList[K]) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p pairList[K]) Less(i, j int) bool {
	return p[i].timeStamp.Nanosecond() < p[j].timeStamp.Nanosecond()
}

// Put associates the specified value with the specified key returning the previously
// mapped value. V will be nil if there was no previous value.
func (l *localCacheImpl[K, V]) Put(key K, value V) *V {
	return l.PutWithExpiry(key, value, l.options.TTL)
}

// PutWithExpiry associates the specified value with the specified key. If the cache
// previously contained a value for this key, the old value is replaced.
// This variation of the Put() function that allows the caller to specify an expiry (or "time to live")
// for the cache entry. V will be nil if there was no previous value.
func (l *localCacheImpl[K, V]) PutWithExpiry(key K, value V, ttl time.Duration) *V {
	l.Lock()
	defer l.Unlock()

	l.registerPut()
	l.pruneEntries()

	newEntry := newLocalCacheEntry[K, V](key, value, ttl)

	l.updateEntrySize(newEntry, 1)
	l.registerExpiry(newEntry)

	prev, ok := l.data[key]

	l.data[key] = newEntry

	if ok {
		l.updateEntrySize(prev, -1)
		return &prev.value
	}
	return nil
}

// Get returns the value to which the specified key is mapped. V will be nil if there was no mapped value.
func (l *localCacheImpl[K, V]) Get(key K) *V {
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
func (l *localCacheImpl[K, V]) GetAll(keys []K) map[K]*V {
	l.Lock()
	defer l.Unlock()

	l.expireEntries()

	results := make(map[K]*V, 0)
	now := time.Now()

	for _, key := range keys {
		v, ok := l.data[key]
		if ok {
			// have entry so add to the results
			results[key] = &v.value
			v.lastAccess = now
		}
	}

	return results
}

// Remove removes the mapping for a key from the cache if it is present and returns the previously
// mapped value, if any. V will be nil if there was no previous value.
func (l *localCacheImpl[K, V]) Remove(key K) *V {
	l.Lock()
	defer l.Unlock()

	l.expireEntries()

	v, ok := l.data[key]

	if ok {
		delete(l.data, key)
		l.updateEntrySize(v, -1)
		return &v.value
	}

	return nil
}

// Size returns the number of mappings contained within the cache.
func (l *localCacheImpl[K, V]) Size() int {
	l.Lock()
	defer l.Unlock()

	l.expireEntries()

	return len(l.data)
}

// SizeBytes returns the number of bytes used by the entries (keys and values) in the near cache.
func (l *localCacheImpl[K, V]) SizeBytes() int64 {
	l.Lock()
	defer l.Unlock()

	l.expireEntries()

	return l.cacheMemory
}

// Clear removes all mappings from the cache.
func (l *localCacheImpl[K, V]) Clear() {
	l.Lock()
	defer l.Unlock()

	l.data = make(map[K]*localCacheEntry[K, V], 0)
	l.expiryMap = make(map[int64]*[]K, 0)
	l.updateCacheMemory(0)
}

// Release releases the cache.
func (l *localCacheImpl[K, V]) Release() {
	l.Clear()
}

func (l *localCacheImpl[K, V]) GetStats() CacheStats {
	return l
}

// expireEntries goes through the map to see if any entries have expired due to ttl.
// this is done in buckets of 1/4 second as so to be more efficient. this means the
// min expiry duration is 1/4 of a second.
func (l *localCacheImpl[K, V]) expireEntries() {
	if len(l.expiryMap) == 0 {
		return
	}

	var (
		bucketsToRemove = make([]int64, 0)
		expiryKeys      = make([]int64, len(l.expiryMap))
		start           = time.Now()
		startUnixMillis = start.UnixMilli()
		index           = 0
	)

	if start.Compare(l.nextExpiry) == -1 {
		return
	}

	// get the keys from the map and sort them so we
	for key := range l.expiryMap {
		expiryKeys[index] = key
		index++
	}

	sort.Slice(expiryKeys, func(p, q int) bool {
		return p < q
	})

	for _, expireTime := range expiryKeys {
		if expireTime < startUnixMillis {
			// need to expire all entries for the expiry key, retrieve the entry
			if v, ok := l.expiryMap[expireTime]; ok {
				bucketsToRemove = append(bucketsToRemove, expireTime)
				for _, k := range *v {
					l.updateEntrySize(l.data[k], -1)
					delete(l.data, k)
				}
			}
		}
	}

	if len(bucketsToRemove) > 0 {
		l.nextExpiry = time.Now().Add(time.Duration(256) * time.Millisecond)

		for _, b := range bucketsToRemove {
			delete(l.expiryMap, b)
		}

		l.registerExpireNanos(time.Since(start).Nanoseconds())
	}
}

// pruneEntries goes through the map to see if any entries have expired or size is reached and remove them.
func (l *localCacheImpl[K, V]) pruneEntries() {
	currentCacheSize := int64(len(l.data))

	l.expireEntries()

	start := time.Now()

	// if highUnits or highUnitsMemory are set then check
	if (l.options.HighUnits > 0 && currentCacheSize+1 > l.options.HighUnits) ||
		(l.options.HighUnitsMemory > 0 && l.cacheMemory+1 > l.options.HighUnitsMemory) {

		defer func() {
			l.registerPruneNanos(time.Since(start).Nanoseconds())
		}()

		entriesToDelete := int(math.Round(float64(float32(currentCacheSize) * (1 - l.options.PruneFactor))))

		// prune to default of l.options.PruneFactor % of the cache size.
		// we first sort the map by lastAccess time / then insert time, so we remove all
		// entries firstly that have never been accessed.
		index := 0
		sortData := make(pairList[K], len(l.data))

		for k, v := range l.data {
			var timestamp = v.lastAccess

			if timestamp.IsZero() {
				// has not been accessed so set the timestamp to the insert time, so when we prune
				// we will prune entries that are older first
				timestamp = v.insertTime
			}
			sortData[index] = pair[K]{key: k, timeStamp: timestamp}

			index++
		}

		sort.Sort(sortData)

		for i, v := range sortData {
			if i > entriesToDelete {
				break
			}
			l.updateEntrySize(l.data[v.key], -1)
			delete(l.data, v.key)
		}
	}
}

func newLocalCacheEntry[K comparable, V any](key K, value V, ttl time.Duration) *localCacheEntry[K, V] {
	now := time.Now()
	entry := &localCacheEntry[K, V]{
		key:        key,
		value:      value,
		ttl:        ttl,
		insertTime: now,
	}
	if ttl > 0 {
		// granularity of expiry is minimum of 250ms
		entry.expiresAt = now.Add(getMillisBucket(ttl))
	}

	return entry
}

func newLocalCache[K comparable, V any](name string, options ...func(localCache *localCacheOptions)) *localCacheImpl[K, V] {
	cache := &localCacheImpl[K, V]{
		Name:       name,
		data:       make(map[K]*localCacheEntry[K, V], 0),
		expiryMap:  make(map[int64]*[]K, 0),
		nextExpiry: time.Now().Add(time.Duration(256) * time.Millisecond),
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

	if cache.options.PruneFactor == 0 {
		cache.options.PruneFactor = defaultPruneFactor
	}

	return cache
}

// localCacheOptions defines options for a local cache.
type localCacheOptions struct {
	TTL                  time.Duration
	HighUnits            int64
	HighUnitsMemory      int64
	InvalidationStrategy InvalidationStrategyType
	PruneFactor          float32
}

func (o *localCacheOptions) String() string {
	return fmt.Sprintf("localCacheOptions{ttl=%v, highUnits=%v, highUnitsMemory=%v, pruneFactor=%.2f, invalidation=%v}",
		o.TTL, o.HighUnits, formatMemory(o.HighUnitsMemory), o.PruneFactor, getInvalidationStrategyString(o.InvalidationStrategy))
}

// withLocalCacheExpiry returns a function to set the expiry time for a local cache.
func withLocalCacheExpiry(ttl time.Duration) func(options *localCacheOptions) {
	return func(o *localCacheOptions) {
		o.TTL = ttl
	}
}

// withInvalidationStrategy returns a function to set the invalidation strategy for a local cache.
func withInvalidationStrategy(strategy InvalidationStrategyType) func(options *localCacheOptions) {
	return func(o *localCacheOptions) {
		o.InvalidationStrategy = strategy
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

// withPruneFactor returns a function to set the prune factor for a local cache.
func withPruneFactor(pruneFactor float32) func(options *localCacheOptions) {
	return func(o *localCacheOptions) {
		o.PruneFactor = pruneFactor
	}
}

func (l *localCacheImpl[K, V]) registerHit() {
	atomic.AddInt64(&l.cacheHits, 1)
}

func (l *localCacheImpl[K, V]) registerMiss() {
	atomic.AddInt64(&l.cacheMisses, 1)
}

func (l *localCacheImpl[K, V]) registerPut() {
	atomic.AddInt64(&l.cachePuts, 1)
}

func (l *localCacheImpl[K, V]) updateCacheMemory(size int64) {
	atomic.AddInt64(&l.cacheMemory, size)
}

func (l *localCacheImpl[K, V]) registerPruneNanos(nanos int64) {
	atomic.AddInt64(&l.cachePrunes, 1)
	atomic.AddInt64(&l.cachePrunesNannos, nanos)
}

func (l *localCacheImpl[K, V]) registerExpireNanos(nanos int64) {
	atomic.AddInt64(&l.cacheExpires, 1)
	atomic.AddInt64(&l.cacheExpiresNannos, nanos)
}

func (l *localCacheImpl[K, V]) registerMissesNanos(nanos int64) {
	atomic.AddInt64(&l.cacheMissesNannos, nanos)
}

func (l *localCacheImpl[K, V]) GetCacheHits() int64 {
	return l.cacheHits
}

func (l *localCacheImpl[K, V]) GetCacheMisses() int64 {
	return l.cacheMisses
}

func (l *localCacheImpl[K, V]) GetCacheMissesDuration() time.Duration {
	return time.Duration(l.cacheMissesNannos) * time.Nanosecond
}

func (l *localCacheImpl[K, V]) GetCachePuts() int64 {
	return l.cachePuts
}

func (l *localCacheImpl[K, V]) GetCachePrunes() int64 {
	return l.cachePrunes
}

func (l *localCacheImpl[K, V]) GetCachePrunesDuration() time.Duration {
	return time.Duration(l.cachePrunesNannos) * time.Nanosecond
}

func (l *localCacheImpl[K, V]) GetCacheExpires() int64 {
	return l.cacheExpires
}

func (l *localCacheImpl[K, V]) GetCacheExpiresDuration() time.Duration {
	return time.Duration(l.cacheExpiresNannos) * time.Nanosecond
}

func (l *localCacheImpl[K, V]) GetTotalGets() int64 {
	return l.GetCacheHits() + l.GetCacheMisses()
}

func (l *localCacheImpl[K, V]) GetHitRate() float32 {
	total := l.cacheHits + l.cacheMisses
	if total == 0 {
		return 0.0
	}
	return float32(l.cacheHits) / float32(total)
}

func (l *localCacheImpl[K, V]) ResetStats() {
	atomic.StoreInt64(&l.cachePrunesNannos, 0)
	atomic.StoreInt64(&l.cacheMissesNannos, 0)
	atomic.StoreInt64(&l.cachePrunes, 0)
	atomic.StoreInt64(&l.cacheHits, 0)
	atomic.StoreInt64(&l.cacheMisses, 0)
	atomic.StoreInt64(&l.cachePuts, 0)
}

func (l *localCacheImpl[K, V]) String() string {
	return fmt.Sprintf("localCache{name=%s, options=%v, stats=CacheStats{puts=%v, gets=%v, hits=%v, misses=%v, "+
		"missesDuration=%v, hitRate=%v%%, prunes=%v, prunesDuration=%v, expires=%v, expiresDuration=%v, size=%v, memoryUsed=%v}}",
		l.Name, l.options, l.GetCachePuts(), l.GetTotalGets(), l.GetCacheHits(), l.GetCacheMisses(),
		l.GetCacheMissesDuration(), l.GetHitRate()*100, l.GetCachePrunes(), l.GetCachePrunesDuration(),
		l.GetCacheExpires(), l.GetCacheExpiresDuration(), l.Size(), formatMemory(l.cacheMemory))
}

// updateEntrySize updates the cacheMemory size based upon a local entry. The sign indicates to either remove or add.
func (l *localCacheImpl[K, V]) updateEntrySize(entry *localCacheEntry[K, V], sign int) {
	var size = int64(unsafe.Sizeof(entry.key)) + int64(unsafe.Sizeof(entry.value)) +
		int64(unsafe.Sizeof(entry.lastAccess)) + int64(unsafe.Sizeof(entry.ttl)) +
		int64(unsafe.Sizeof(entry.insertTime)) + int64(unsafe.Sizeof(entry))
	l.updateCacheMemory(int64(sign) * size)
}

func formatMemory(bytesValue int64) string {
	var printer = message.NewPrinter(language.English)
	if bytesValue < KB {
		return printer.Sprintf("%-1dB", bytesValue)
	}
	if bytesValue < MB {
		return printer.Sprintf("%-.1fKB", float64(bytesValue)/1024)
	}
	if bytesValue < GB {
		return printer.Sprintf("%-.1fMB", float64(bytesValue)/1024/1024)
	}
	return printer.Sprintf("%-.1fGB", float64(bytesValue)/1024/1024/1024)
}

func (l *localCacheImpl[K, V]) registerExpiry(entry *localCacheEntry[K, V]) {
	if entry.ttl > 0 {
		// get the expires millis in unix millis and key on this
		expiresAtMillis := entry.expiresAt.UnixMilli()

		// see if we can find an entry for the expires time as millis
		v, ok := l.expiryMap[expiresAtMillis]
		if !ok {
			// create a new map entry
			newSlice := []K{entry.key}
			l.expiryMap[expiresAtMillis] = &newSlice
		} else {
			// append to the existing one
			*v = append(*v, entry.key)
		}
	}
}

// getMillisBucket returns the ttl in buckets of 256ms for expiry.
func getMillisBucket(ttl time.Duration) time.Duration {
	return time.Duration(ttl.Milliseconds() & ^0xFF) * time.Millisecond
}
