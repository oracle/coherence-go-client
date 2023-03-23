/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	. "github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	. "github.com/oracle/coherence-go-client/test/utils"
	"testing"
)

// TestCacheLifecycle runs tests to ensure correct behaviour when destroying or releasing
// NamedMap and NamedCache instances.
func TestCacheLifecycle(t *testing.T) {
	g := NewWithT(t)
	session, err := GetSession()
	g.Expect(err).ShouldNot(HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, Person])
	}{
		{"NamedMapRunTestDestroy", GetNamedMap[int, Person](g, session, "destroy-map"), RunTestDestroy},
		{"NamedCacheRunTestDestroy", GetNamedCache[int, Person](g, session, "destroy-cache"), RunTestDestroy},
		{"NamedMapRunTestRelease", GetNamedMap[int, Person](g, session, "release-map"), RunTestRelease},
		{"NamedCacheRunTestRelease", GetNamedCache[int, Person](g, session, "release-cache"), RunTestRelease},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunTestDestroy(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g   = NewWithT(t)
		err error
	)

	addPerson(g, namedMap)

	// issue destroy against the namedMap
	err = namedMap.Destroy(ctx)
	g.Expect(err).ShouldNot(HaveOccurred())

	// we should no longer be able to perform operations against this namedMap
	_, err = namedMap.Size(ctx)
	g.Expect(err).To(Equal(coherence.ErrDestroyed))
}

func RunTestRelease(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g   = NewWithT(t)
		err error
	)

	addPerson(g, namedMap)

	// issue release against the namedMap
	namedMap.Release()

	// we should no longer be able to perform operations against this namedMap
	// as it is destroyed and released
	_, err = namedMap.Size(ctx)
	g.Expect(err).To(Equal(coherence.ErrReleased))
}
