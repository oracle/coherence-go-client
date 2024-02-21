/*
* Copyright (c) 2024 Oracle and/or its affiliates.
* Licensed under the Universal Permissive License v 1.0 as shown at
* https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"github.com/onsi/gomega"
	"testing"
	"time"
)

// TestIsNearCacheEqual tests various scenarios where cache options should equal
func TestIsNearCacheEqual(t *testing.T) {
	var (
		g                 = gomega.NewWithT(t)
		nearCacheOptions1 = NearCacheOptions{TTL: time.Duration(10) * time.Second}
		nearCacheOptions2 = NearCacheOptions{TTL: time.Duration(8) * time.Second}
		nearCacheOptions3 = NearCacheOptions{HighUnits: 100}
		nearCacheOptions4 = NearCacheOptions{HighUnitsMemory: 10_000}
		nearCacheOptions5 = NearCacheOptions{InvalidationStrategy: ListenAll}
	)

	localCache1 := newLocalCache[int, string]("test", withLocalCacheExpiry(time.Duration(10)*time.Second))
	localCache2 := newLocalCache[int, string]("test", withLocalCacheHighUnits(100))
	localCache3 := newLocalCache[int, string]("test", withLocalCacheHighUnitsMemory(10_000))
	localCache4 := newLocalCache[int, string]("test", withInvalidationStrategy(ListenAll))

	g.Expect(isNearCacheEqual[int, string](localCache1, &nearCacheOptions1)).To(gomega.Equal(true))
	g.Expect(isNearCacheEqual[int, string](localCache1, &nearCacheOptions2)).To(gomega.Equal(false))
	g.Expect(isNearCacheEqual[int, string](localCache2, &nearCacheOptions2)).To(gomega.Equal(false))
	g.Expect(isNearCacheEqual[int, string](localCache2, &nearCacheOptions3)).To(gomega.Equal(true))
	g.Expect(isNearCacheEqual[int, string](localCache3, &nearCacheOptions3)).To(gomega.Equal(false))
	g.Expect(isNearCacheEqual[int, string](localCache3, &nearCacheOptions4)).To(gomega.Equal(true))
	g.Expect(isNearCacheEqual[int, string](localCache3, &nearCacheOptions4)).To(gomega.Equal(true))
	g.Expect(isNearCacheEqual[int, string](localCache4, &nearCacheOptions4)).To(gomega.Equal(false))
	g.Expect(isNearCacheEqual[int, string](localCache4, &nearCacheOptions5)).To(gomega.Equal(true))
}
