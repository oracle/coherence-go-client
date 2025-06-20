/*
 * Copyright (c) 2024, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */
package coherence

import (
	"fmt"
	"log"
	"math"
	"sync"
	"testing"
	"time"
)

func TestBasicLocalCacheOperations(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-1")
	if cache.Size() != 0 {
		t.Fatalf("expected size 0, got %d", cache.Size())
	}

	old := cache.Put(1, "one")
	if old != nil {
		t.Fatalf("expected nil on first Put, got %v", *old)
	}
	if cache.Size() != 1 {
		t.Fatalf("expected size 1, got %d", cache.Size())
	}

	value := cache.Get(1)
	if value == nil || *value != "one" {
		t.Fatalf("expected Get(1) to return 'one', got %v", value)
	}

	oldValue := cache.Put(1, "ONE")
	if oldValue == nil || *oldValue != "one" {
		t.Fatalf("expected old value to be 'one', got %v", oldValue)
	}

	oldValue = cache.Remove(1)
	if oldValue == nil || *oldValue != "ONE" {
		t.Fatalf("expected removed value to be 'ONE', got %v", oldValue)
	}
	if cache.Size() != 0 {
		t.Fatalf("expected size 0 after removal, got %d", cache.Size())
	}

	value = cache.Get(1)
	if value != nil {
		t.Fatalf("expected nil after removal, got %v", *value)
	}

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")
	if cache.Size() != 3 {
		t.Fatalf("expected size 3 after puts, got %d", cache.Size())
	}

	cache.Clear()
	if cache.Size() != 0 {
		t.Fatalf("expected size 0 after clear, got %d", cache.Size())
	}

	cache.PutWithExpiry(1, "one", 3*time.Second)
	Sleep(4)
	if val := cache.Get(1); val != nil {
		t.Fatalf("expected nil after expiry, got %v", *val)
	}
	if cache.Size() != 0 {
		t.Fatalf("expected size 0 after expiry, got %d", cache.Size())
	}

	cache.PutWithExpiry(1, "one", 3*time.Second)
	Sleep(4)
	if val := cache.Remove(1); val != nil {
		t.Fatalf("expected nil after expiry on remove, got %v", *val)
	}
	if cache.Size() != 0 {
		t.Fatalf("expected size 0 after expired remove, got %d", cache.Size())
	}

	cache.PutWithExpiry(1, "one", 3*time.Second)
	Sleep(4)
	if cache.Size() != 0 {
		t.Fatalf("expected size 0 after expiry, got %d", cache.Size())
	}

	cache.PutWithExpiry(1, "one", 257*time.Millisecond)
	time.Sleep(258 * time.Millisecond)
	if cache.Size() != 0 {
		t.Fatalf("expected size 0 after millisecond expiry, got %d", cache.Size())
	}

	expiryMap := cache.expiryMap
	if len(expiryMap) != 0 {
		t.Fatalf("expected expiry map to be empty, got len=%d", len(expiryMap))
	}

	fmt.Println(cache)
}

func TestBasicLocalCacheOperationsWithExpiry(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-2")

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0, got %d", cache.Size())
	}

	old := cache.PutWithExpiry(1, "one", time.Second)
	if cache.Size() != 1 {
		t.Fatalf("expected cache size 1 after first put, got %d", cache.Size())
	}
	if old != nil {
		t.Fatalf("expected old value to be nil for first key, got %v", old)
	}

	old = cache.PutWithExpiry(2, "two", time.Second)
	if cache.Size() != 2 {
		t.Fatalf("expected cache size 2 after second put, got %d", cache.Size())
	}
	if old != nil {
		t.Fatalf("expected old value to be nil for second key, got %v", old)
	}

	cache.Remove(1)
	if cache.Size() != 1 {
		t.Fatalf("expected cache size 1 after removing key 1, got %d", cache.Size())
	}

	time.Sleep(2 * time.Second)

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0 after expiry, got %d", cache.Size())
	}

	if len(cache.expiryMap) != 0 {
		t.Fatalf("expected expiry map to be empty, got %d entries", len(cache.expiryMap))
	}

	fmt.Println(cache)
}

func TestBasicLocalCacheWithDefaultExpiry(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache", withLocalCacheExpiry(2*time.Second))

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0 initially, got %d", cache.Size())
	}

	cache.Put(1, "one")

	if cache.Size() != 1 {
		t.Fatalf("expected cache size 1 after put, got %d", cache.Size())
	}

	Sleep(3) // assuming this is a helper that sleeps for 3 seconds

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0 after expiry, got %d", cache.Size())
	}
}

func TestBasicLocalCacheClear(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-clear", withLocalCacheExpiry(2*time.Second))

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0 initially, got %d", cache.Size())
	}

	cache.Put(1, "one")

	if cache.Size() != 1 {
		t.Fatalf("expected cache size 1 after put, got %d", cache.Size())
	}

	cache.Clear()

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0 after clear, got %d", cache.Size())
	}
}

func TestBasicLocalCacheRelease(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-clear", withLocalCacheExpiry(2*time.Second))

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0 initially, got %d", cache.Size())
	}

	cache.Put(1, "one")

	if cache.Size() != 1 {
		t.Fatalf("expected cache size 1 after put, got %d", cache.Size())
	}

	cache.Release()

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0 after release, got %d", cache.Size())
	}
}

func TestBasicLocalCacheGetAll(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-get-all", withLocalCacheExpiry(10*time.Second))

	if cache.Size() != 0 {
		t.Fatalf("expected cache size 0 initially, got %d", cache.Size())
	}

	results := cache.GetAll([]int{1, 2, 3})
	if len(results) != 0 {
		t.Fatalf("expected empty result from GetAll, got %d", len(results))
	}

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")
	cache.Put(4, "four")
	cache.Put(5, "five")

	if cache.Size() != 5 {
		t.Fatalf("expected cache size 5 after puts, got %d", cache.Size())
	}

	results = cache.GetAll([]int{1, 5})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	v, ok := results[1]
	if !ok || v == nil || *v != "one" {
		t.Fatalf("expected results[1] to be 'one', got %v", v)
	}

	v, ok = results[5]
	if !ok || v == nil || *v != "five" {
		t.Fatalf("expected results[5] to be 'five', got %v", v)
	}

	v, ok = results[6]
	if ok || v != nil {
		t.Fatalf("expected results[6] to be nil/missing, got %v", v)
	}
}

func TestLocalCacheWithHighUnitsOnly(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-high-unit1", withLocalCacheHighUnits(100))

	for i := 0; i < 100; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
	}

	if cache.Size() != 100 {
		t.Fatalf("expected size 100 after inserts, got %d", cache.Size())
	}

	cache.Put(100, "one hundred")

	expectedSize := int(math.Round(float64(float32(100) * cache.options.PruneFactor)))
	if cache.Size() != expectedSize {
		t.Fatalf("expected size %d after prune, got %d", expectedSize, cache.Size())
	}

	if cache.GetCachePrunes() != 1 {
		t.Fatalf("expected 1 cache prune, got %d", cache.GetCachePrunes())
	}

	fmt.Println(cache)
}

func TestLocalCacheWithHighUnitsMemoryOnly(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-high-unit2", withLocalCacheHighUnitsMemory(1024*100))

	for i := 0; i < 10_000; i++ {
		cache.Put(i, fmt.Sprintf("value2-%v", i))
	}

	if cache.Size() >= 10_000 {
		t.Fatalf("expected cache size to be less than 10,000 due to memory constraints, got %d", cache.Size())
	}

	fmt.Println(cache)
}

func TestLocalCacheWithHighUnitsOnlyAccessTime(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-high-unit3", withLocalCacheHighUnits(100))

	for i := 0; i < 100; i++ {
		cache.Put(i, fmt.Sprintf("value3-%v", i))
	}

	if cache.Size() != 100 {
		t.Fatalf("expected cache size 100 after inserts, got %d", cache.Size())
	}

	// access keys to make them recently used
	cache.Get(1)
	cache.Get(2)
	cache.Get(3)

	time.Sleep(5 * time.Second)

	// add new entry to trigger prune
	cache.Put(100, "one hundred")

	expectedSize := int(math.Round(float64(float32(100) * cache.options.PruneFactor)))
	if cache.Size() != expectedSize {
		t.Fatalf("expected cache size %d after prune, got %d", expectedSize, cache.Size())
	}

	if cache.GetCachePrunes() != 1 {
		t.Fatalf("expected 1 prune operation, got %d", cache.GetCachePrunes())
	}

	// accessed entries should still be present
	if cache.Get(1) == nil {
		t.Fatalf("expected key 1 to be present after prune")
	}
	if cache.Get(2) == nil {
		t.Fatalf("expected key 2 to be present after prune")
	}
	if cache.Get(3) == nil {
		t.Fatalf("expected key 3 to be present after prune")
	}
}

func TestLocalCacheWithHighUnitsAndTTL(t *testing.T) {
	cache := newLocalCache[int, string](
		"my-cache-high-unit",
		withLocalCacheHighUnits(100),
		withLocalCacheExpiry(2*time.Second),
	)

	for i := 0; i < 100; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
	}

	if cache.Size() != 100 {
		t.Fatalf("expected cache size 100 after inserts, got %d", cache.Size())
	}

	time.Sleep(1 * time.Second)

	cache.Put(100, "one hundred")
	cache.Put(101, "one hundred and one")

	if cache.Size() != 81 {
		t.Fatalf("expected cache size 81 after prune, got %d", cache.Size())
	}
	if cache.Get(100) == nil {
		t.Fatalf("expected key 100 to exist")
	}
	if cache.Get(101) == nil {
		t.Fatalf("expected key 101 to exist")
	}

	time.Sleep(2 * time.Second)

	for i := 110; i < 130; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
	}

	if cache.Size() != 20 {
		t.Fatalf("expected cache size 20 after TTL expiry and new inserts, got %d", cache.Size())
	}

	fmt.Println(cache)
}
func TestLocalCacheGoRoutines(t *testing.T) {
	cache := newLocalCache[int, string]("my-cache-2")
	var wg sync.WaitGroup

	routines := 500
	iterations := 10_000

	fmt.Println("Start " + time.Now().String())
	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func(id, iters int) {
			defer wg.Done()
			for j := 1; j <= iters; j++ {
				cache.Put(id*1_0000_000+j, "Value-")
			}
		}(i, iterations)
	}

	wg.Wait()
	fmt.Println("End   " + time.Now().String())

	size := cache.Size()
	expected := routines * iterations

	if size != expected {
		t.Fatalf("expected cache size %d, got %d", expected, size)
	}

	fmt.Println(cache.GetStats())
}

func TestBasicLocalCacheSizeCalculation(t *testing.T) {
	const maxEntries = 10_000
	cache := newLocalCache[int, string]("my-size-calc-size")

	if cache.Size() != 0 {
		t.Fatalf("expected initial cache size 0, got %d", cache.Size())
	}

	// Add entries
	for i := 1; i <= maxEntries; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
	}

	if cache.cacheMemory <= 0 {
		t.Fatal("expected memory usage to be greater than 0 after initial put")
	}

	// Update entries with bigger values
	for i := 1; i <= maxEntries; i++ {
		cache.Put(i, fmt.Sprintf("new-value-bigger-%v", i))
	}

	if cache.cacheMemory <= 0 {
		t.Fatal("expected memory usage to be greater than 0 after updating with bigger values")
	}

	// Update entries with smaller values
	for i := 1; i <= maxEntries; i++ {
		cache.Put(i, fmt.Sprintf("%v", i))
	}

	if cache.cacheMemory <= 0 {
		t.Fatal("expected memory usage to be greater than 0 after updating with smaller values")
	}

	// Remove entries and confirm memory is freed
	for i := 1; i <= maxEntries; i++ {
		cache.Remove(i)
	}

	if mem := cache.cacheMemory; mem != 0 {
		t.Fatalf("expected memory usage to be 0 after removing entries, got %d", mem)
	}
}

type expiryResults struct {
	ttl          time.Duration
	expiryTime   time.Duration
	cacheExpires int64
}

func TestNearCacheExpiry1(_ *testing.T) {
	var results = make([]expiryResults, 0)

	results = append(results, localCacheExpiryTest(time.Duration(1000)*time.Millisecond, 250_000))

	// output results
	for _, r := range results {
		fmt.Printf("ttl=%5v, cache expires=%5v, time spent expiring=%10v\n", r.ttl, r.cacheExpires, r.expiryTime)
	}
}

func TestNearCacheBuckets(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected time.Duration
	}{
		{254 * time.Millisecond, 0 * time.Millisecond},
		{257 * time.Millisecond, 256 * time.Millisecond},
		{511 * time.Millisecond, 256 * time.Millisecond},
		{513 * time.Millisecond, 512 * time.Millisecond},
		{800 * time.Millisecond, 768 * time.Millisecond},
	}

	for _, tt := range tests {
		actual := getMillisBucket(tt.input)
		if actual != tt.expected {
			t.Fatalf("GetMillisBucketForTest(%v): expected %v, got %v", tt.input, tt.expected, actual)
		}
	}
}

func localCacheExpiryTest(ttl time.Duration, count int) expiryResults {
	cache := newLocalCache[int, string]("my-cache-high-unit3", withLocalCacheExpiry(ttl))

	for i := 1; i <= count; i++ {
		cache.Put(i, fmt.Sprintf("value-%v", i))
		if i%10_000 == 0 {
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
	log.Println(cache)

	return expiryResults{ttl: ttl, expiryTime: cache.GetCacheExpiresDuration(), cacheExpires: cache.GetCacheExpires()}
}

func Sleep(seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
}
