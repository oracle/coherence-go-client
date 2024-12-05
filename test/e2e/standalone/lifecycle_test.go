/*
 * Copyright (c) 2023, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"testing"
)

// TestCacheLifecycle runs tests to ensure correct behaviour when destroying or releasing
// NamedMap and NamedCache instances.
func TestCacheLifecycle(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, utils.Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, utils.Person])
	}{
		{"NamedMapRunTestDestroy", utils.GetNamedMap[int, utils.Person](g, session, "destroy-map"), RunTestDestroy},
		{"NamedCacheRunTestDestroy", utils.GetNamedCache[int, utils.Person](g, session, "destroy-cache"), RunTestDestroy},
		{"NamedMapRunTestRelease", utils.GetNamedMap[int, utils.Person](g, session, "release-map"), RunTestRelease},
		{"NamedCacheRunTestRelease", utils.GetNamedCache[int, utils.Person](g, session, "release-cache"), RunTestRelease},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunTestDestroy(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	addPerson(g, namedMap)

	// issue destroy against the namedMap
	err = namedMap.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// we should no longer be able to perform operations against this namedMap
	_, err = namedMap.Size(ctx)
	g.Expect(err).To(gomega.Equal(coherence.ErrDestroyed))
}

func RunTestRelease(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	addPerson(g, namedMap)

	// issue release against the namedMap
	namedMap.Release()

	// we should no longer be able to perform operations against this namedMap
	// as it is destroyed and released
	_, err = namedMap.Size(ctx)
	g.Expect(err).To(gomega.Equal(coherence.ErrReleased))
}
