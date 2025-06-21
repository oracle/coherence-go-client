/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package scope

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"testing"
)

var localCtx = context.Background()

func TestBasicCrudOperationsVariousTypes(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
	)

	session, err = utils.GetSession(coherence.WithScope("test"))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	RunKeyValueTest[int, bool](g, getNewNamedMap[int, bool](g, session, "c10"), 1, false)
	RunKeyValueTest[int, bool](g, getNewNamedMap[int, bool](g, session, "c11"), 1, true)
	RunKeyValueTest[int, byte](g, getNewNamedMap[int, byte](g, session, "c12"), 1, byte(22))
	RunKeyValueTest[int, string](g, getNewNamedMap[int, string](g, session, "c1"), 1, "Tim")
	RunKeyValueTest[int, utils.Person](g, getNewNamedMap[int, utils.Person](g, session, "c2"), 1, utils.Person{ID: 1, Name: "Tim"})
	RunKeyValueTest[int, float32](g, getNewNamedMap[int, float32](g, session, "c3"), 1, float32(1.123))
	RunKeyValueTest[int, float64](g, getNewNamedMap[int, float64](g, session, "c4"), 1, 1.123)
	RunKeyValueTest[int, int](g, getNewNamedMap[int, int](g, session, "c5"), 1, 1)
	RunKeyValueTest[int, int16](g, getNewNamedMap[int, int16](g, session, "c7"), 1, 10)
	RunKeyValueTest[int, int32](g, getNewNamedMap[int, int32](g, session, "c8"), 1, 1333)
	RunKeyValueTest[int, int64](g, getNewNamedMap[int, int64](g, session, "c9"), 1, 1333)
	RunKeyValueTest[string, utils.Person](g, getNewNamedMap[string, utils.Person](g, session, "c13"), "k1", utils.Person{ID: 1, Name: "Tim"})
	RunKeyValueTest[string, string](g, getNewNamedMap[string, string](g, session, "c14"), "k1", "value1")
	RunKeyValueTest[int, utils.Person](g, getNewNamedMap[int, utils.Person](g, session, "c15"), 1,
		utils.Person{ID: 1, Name: "Tim", HomeAddress: utils.Address{Address1: "a1", Address2: "a2", City: "Perth", State: "WA", PostCode: 6000}})
	RunKeyValueTest[int, []string](g, getNewNamedMap[int, []string](g, session, "c16"), 1,
		[]string{"a", "b", "c"})
	RunKeyValueTest[int, map[int]string](g, getNewNamedMap[int, map[int]string](g, session, "c17"), 1,
		map[int]string{1: "one", 2: "two", 3: "three"})

	RunKeyValueTest[int, float64](g, getNewNamedCache[int, float64](g, session, "c4"), 1, 1.123)
	RunKeyValueTest[int, int](g, getNewNamedCache[int, int](g, session, "c5"), 1, 1)
	RunKeyValueTest[int, int16](g, getNewNamedCache[int, int16](g, session, "c7"), 1, 10)
	RunKeyValueTest[int, int32](g, getNewNamedCache[int, int32](g, session, "c8"), 1, 1333)
	RunKeyValueTest[int, string](g, getNewNamedCache[int, string](g, session, "c1"), 1, "Tim")
	RunKeyValueTest[int, utils.Person](g, getNewNamedCache[int, utils.Person](g, session, "c2"), 1, utils.Person{ID: 1, Name: "Tim"})
	RunKeyValueTest[int, float32](g, getNewNamedCache[int, float32](g, session, "c3"), 1, float32(1.123))
	RunKeyValueTest[int, int64](g, getNewNamedCache[int, int64](g, session, "c9"), 1, 1333)
	RunKeyValueTest[int, bool](g, getNewNamedCache[int, bool](g, session, "c10"), 1, false)
	RunKeyValueTest[int, bool](g, getNewNamedCache[int, bool](g, session, "c11"), 1, true)
	RunKeyValueTest[int, byte](g, getNewNamedCache[int, byte](g, session, "c12"), 1, byte(22))
	RunKeyValueTest[string, utils.Person](g, getNewNamedCache[string, utils.Person](g, session, "c13"), "k1", utils.Person{ID: 1, Name: "Tim"})
	RunKeyValueTest[int, []string](g, getNewNamedCache[int, []string](g, session, "c16"), 1,
		[]string{"a", "b", "c"})
	RunKeyValueTest[int, map[int]string](g, getNewNamedCache[int, map[int]string](g, session, "c17"), 1,
		map[int]string{1: "one", 2: "two", 3: "three"})
	RunKeyValueTest[string, string](g, getNewNamedCache[string, string](g, session, "c14"), "k1", "value1")
	RunKeyValueTest[int, utils.Person](g, getNewNamedCache[int, utils.Person](g, session, "c15"), 1,
		utils.Person{ID: 1, Name: "Tim", HomeAddress: utils.Address{Address1: "a1", Address2: "a2", City: "Perth", State: "WA", PostCode: 6000}})
}

// getNewNamedCache returns a cache for a session and asserts err is nil.
func getNewNamedCache[K comparable, V any](g *gomega.WithT, session *coherence.Session, name string) coherence.NamedCache[K, V] {
	namedCache, err := coherence.GetNamedCache[K, V](session, name)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	return namedCache
}

// getNewNamedMap returns a map for a session and asserts err is nil.
func getNewNamedMap[K comparable, V any](g *gomega.WithT, session *coherence.Session, name string) coherence.NamedMap[K, V] {
	namedMap, err := coherence.GetNamedMap[K, V](session, name)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	return namedMap
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
