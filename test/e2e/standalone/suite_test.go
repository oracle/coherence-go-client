/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"testing"
)

var localCtx = context.Background()

// The entry point for the test suite
func TestMain(m *testing.M) {
	utils.RunTest(m, 1408, 30000, 8080, true)
}

func GetNamedMapWithScope[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName, _ string) coherence.NamedMap[K, V] {
	namedCache, err := coherence.GetNamedMap[K, V](session, cacheName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedCache.Clear(localCtx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	return namedCache
}

func GetNamedCacheWithScope[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName, _ string) coherence.NamedCache[K, V] {
	namedCache, err := coherence.GetNamedCache[K, V](session, cacheName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedCache.Clear(localCtx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	return namedCache
}

func GetNamedMap[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName string) coherence.NamedMap[K, V] {
	return GetNamedMapWithScope[K, V](g, session, cacheName, "")
}

func GetNamedCache[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName string) coherence.NamedCache[K, V] {
	return GetNamedCacheWithScope[K, V](g, session, cacheName, "")
}

func AssertSize[K comparable, V any](g *gomega.WithT, namedMap coherence.NamedMap[K, V], expectedSize int) {
	size, err := namedMap.Size(localCtx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(expectedSize))
}

func ClearNamedMap[K comparable, V any](g *gomega.WithT, namedCache coherence.NamedMap[K, V]) {
	err := namedCache.Clear(localCtx)
	g.Expect(err).NotTo(gomega.HaveOccurred())
}

func AssertPersonResult(g *gomega.WithT, result utils.Person, expectedValue utils.Person) {
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.Name).To(gomega.Equal(expectedValue.Name))
	g.Expect(result.ID).To(gomega.Equal(expectedValue.ID))
}

// RunKeyValueTest runs a basic Put/Get test against various key/ values
func RunKeyValueTest[K comparable, V any](g *gomega.WithT, cache coherence.NamedMap[K, V], key K, value V) {
	var (
		result   *V
		err      = cache.Clear(localCtx)
		oldValue *V
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = cache.Put(localCtx, key, value)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	result, err = cache.Get(localCtx, key)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(*result).To(gomega.Equal(value))

	oldValue, err = cache.Remove(localCtx, key)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.Equal(result))
}
