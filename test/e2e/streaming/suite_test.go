/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package streaming

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

func GetNamedMap[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName string) coherence.NamedMap[K, V] {
	return GetNamedMapWithScope[K, V](g, session, cacheName, "")
}
