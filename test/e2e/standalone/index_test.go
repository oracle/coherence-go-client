/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"encoding/json"
	. "github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/aggregators"
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
	. "github.com/oracle/coherence-go-client/test/utils"
	"strings"
	"testing"
)

// TestIndexAgainstMapAndCache runs index tests against NamedMap and NamedCache
func TestIndexAgainstMapAndCache(t *testing.T) {
	g := NewWithT(t)
	session, err := GetSession()
	g.Expect(err).ShouldNot(HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, Person])
	}{
		{"NamedMapRunTestIndex", GetNamedMap[int, Person](g, session, "index-map"), RunTestIndex},
		{"NamedCacheRunTestIndex", GetNamedCache[int, Person](g, session, "index-cache"), RunTestIndex},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunTestIndex(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g   = NewWithT(t)
		err error
	)

	addPerson(g, namedMap)

	// add indexes without comparators
	err = coherence.AddIndex(ctx, namedMap, extractors.Extract[int]("id"), true)
	g.Expect(err).ShouldNot(HaveOccurred())

	Sleep(5)

	g.Expect(canFindIndex(g, namedMap)).To(BeTrue())

	err = coherence.RemoveIndex(ctx, namedMap, extractors.Extract[int]("id"))
	g.Expect(err).ShouldNot(HaveOccurred())

	Sleep(5)

	g.Expect(canFindIndex(g, namedMap)).To(BeFalse())

	// add index with comparator
	err = coherence.AddIndexWithComparator(ctx, namedMap, extractors.Extract[int]("id"), extractors.Extract[int]("name"))
	g.Expect(err).ShouldNot(HaveOccurred())

	Sleep(10)

	g.Expect(canFindIndex(g, namedMap)).To(BeTrue())

	err = coherence.RemoveIndex(ctx, namedMap, extractors.Extract[int]("id"))
	g.Expect(err).ShouldNot(HaveOccurred())

	Sleep(10)

	g.Expect(canFindIndex(g, namedMap)).To(BeFalse())
}

func canFindIndex(g *WithT, namedMap coherence.NamedMap[int, Person]) bool {
	var jsonData []byte
	jsonResult, err := coherence.AggregateFilter[int, Person, map[string]interface{}](ctx, namedMap, filters.Equal(extractors.Extract[int]("id"), 1),
		aggregators.QueryRecorder(aggregators.Explain))
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(len(*jsonResult)).Should(BeNumerically(">", 0))
	g.Expect((*jsonResult)["results"]).To(Not(BeNil()))
	g.Expect((*jsonResult)["type"]).To(Not(BeNil()))

	jsonData, err = json.Marshal((*jsonResult)["results"])
	g.Expect(err).ShouldNot(HaveOccurred())

	jsonString := string(jsonData)
	return strings.Contains(jsonString, "\"extractor\":\".id\",\"index\":\"Simple")
}
