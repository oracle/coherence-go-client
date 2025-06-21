/*
* Copyright (c) 2025 Oracle and/or its affiliates.
* Licensed under the Universal Permissive License v 1.0 as shown at
* https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"errors"
	"testing"
	"time"
)

// TestIsNearCacheEqual tests various scenarios where cache options should equal.
func TestIsNearCacheEqual(t *testing.T) {
	var (
		nearCacheOptions1 = NearCacheOptions{TTL: time.Duration(10) * time.Second, PruneFactor: 0.8}
		nearCacheOptions2 = NearCacheOptions{TTL: time.Duration(8) * time.Second, PruneFactor: 0.8}
		nearCacheOptions3 = NearCacheOptions{HighUnits: 100, PruneFactor: 0.8}
		nearCacheOptions4 = NearCacheOptions{HighUnitsMemory: 10_000, PruneFactor: 0.8}
		nearCacheOptions5 = NearCacheOptions{InvalidationStrategy: ListenAll, PruneFactor: 0.8}
		nearCacheOptions6 = NearCacheOptions{HighUnitsMemory: 10_000, PruneFactor: 0.7}
	)

	localCache1 := newLocalCache[int, string]("test", withLocalCacheExpiry(time.Duration(10)*time.Second))
	localCache2 := newLocalCache[int, string]("test", withLocalCacheHighUnits(100))
	localCache3 := newLocalCache[int, string]("test", withLocalCacheHighUnitsMemory(10_000))
	localCache4 := newLocalCache[int, string]("test", withInvalidationStrategy(ListenAll))

	if !isNearCacheEqual[int, string](localCache1, &nearCacheOptions1) {
		t.Fatalf("expected localCache1 to equal nearCacheOptions1")
	}
	if isNearCacheEqual[int, string](localCache1, &nearCacheOptions2) {
		t.Fatalf("expected localCache1 to not equal nearCacheOptions2")
	}
	if isNearCacheEqual[int, string](localCache2, &nearCacheOptions2) {
		t.Fatalf("expected localCache2 to not equal nearCacheOptions2")
	}
	if !isNearCacheEqual[int, string](localCache2, &nearCacheOptions3) {
		t.Fatalf("expected localCache2 to equal nearCacheOptions3")
	}
	if isNearCacheEqual[int, string](localCache3, &nearCacheOptions3) {
		t.Fatalf("expected localCache3 to not equal nearCacheOptions3")
	}
	if !isNearCacheEqual[int, string](localCache3, &nearCacheOptions4) {
		t.Fatalf("expected localCache3 to equal nearCacheOptions4")
	}
	if !isNearCacheEqual[int, string](localCache3, &nearCacheOptions4) {
		t.Fatalf("expected localCache3 to equal nearCacheOptions4")
	}
	if isNearCacheEqual[int, string](localCache4, &nearCacheOptions4) {
		t.Fatalf("expected localCache4 to not equal nearCacheOptions4")
	}
	if !isNearCacheEqual[int, string](localCache4, &nearCacheOptions5) {
		t.Fatalf("expected localCache4 to equal nearCacheOptions5")
	}
	if isNearCacheEqual[int, string](localCache3, &nearCacheOptions6) {
		t.Fatalf("expected localCache3 to not equal nearCacheOptions6")
	}
}

// TestInvalidNearCacheOptions tests various edge cases for near cache options
func TestInvalidNearCacheOptions(t *testing.T) {
	var (
		nearCacheOptions1 = NearCacheOptions{HighUnits: -1}
		nearCacheOptions2 = NearCacheOptions{HighUnitsMemory: -1}
		nearCacheOptions3 = NearCacheOptions{HighUnitsMemory: 1, HighUnits: 1}
		nearCacheOptions4 = NearCacheOptions{TTL: time.Duration(1) * time.Second, HighUnitsMemory: 1, HighUnits: 1}
		nearCacheOptions5 = NearCacheOptions{}
		nearCacheOptions7 = NearCacheOptions{TTL: time.Duration(255) * time.Millisecond}
	)

	err := ensureNearCacheOptions(&nearCacheOptions1)
	if !errors.Is(err, ErrNegativeNearCacheOptions) {
		t.Fatalf("expected ErrNegativeNearCacheOptions, got: %v", err)
	}

	err = ensureNearCacheOptions(&nearCacheOptions2)
	if !errors.Is(err, ErrNegativeNearCacheOptions) {
		t.Fatalf("expected ErrNegativeNearCacheOptions, got: %v", err)
	}

	err = ensureNearCacheOptions(&nearCacheOptions3)
	if !errors.Is(err, ErrInvalidNearCacheWithNoTTL) {
		t.Fatalf("expected ErrInvalidNearCacheWithNoTTL, got: %v", err)
	}

	err = ensureNearCacheOptions(&nearCacheOptions4)
	if !errors.Is(err, ErrInvalidNearCacheWithTTL) {
		t.Fatalf("expected ErrInvalidNearCacheWithTTL, got: %v", err)
	}

	err = ensureNearCacheOptions(&nearCacheOptions5)
	if !errors.Is(err, ErrInvalidNearCache) {
		t.Fatalf("expected ErrInvalidNearCache, got: %v", err)
	}

	err = ensureNearCacheOptions(&nearCacheOptions7)
	if !errors.Is(err, ErrInvalidNearCacheTTL) {
		t.Fatalf("expected ErrInvalidNearCacheTTL, got: %v", err)
	}
}
