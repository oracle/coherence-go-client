/*
 * Copyright (c) 2023, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"encoding/json"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/aggregators"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"strings"
	"testing"
)

// TestIndexAgainstMapAndCache runs index tests against NamedMap and NamedCache
func TestIndexAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, utils.Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, utils.Person])
	}{
		{"NamedMapRunTestIndex", utils.GetNamedMap[int, utils.Person](g, session, "index-map"), RunTestIndex},
		{"NamedCacheRunTestIndex", utils.GetNamedCache[int, utils.Person](g, session, "index-cache"), RunTestIndex},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunTestIndex(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g           = gomega.NewWithT(t)
		err         error
		idExtractor = extractors.Extract[int]("id")
	)

	addPerson(g, namedMap)

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Println("Add Index")
	// add indexes without comparators
	err = coherence.AddIndex(ctx, namedMap, idExtractor, true)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	utils.Sleep(5)

	log.Println("canFindIndex Index")
	g.Expect(canFindIndex(g, namedMap)).To(gomega.BeTrue())

	log.Println("Remove Index")
	err = coherence.RemoveIndex(ctx, namedMap, idExtractor)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	utils.Sleep(5)

	log.Println("canFindIndex Index")
	g.Expect(canFindIndex(g, namedMap)).To(gomega.BeFalse())

	log.Println("Add Index with Comparator")
	// add index with comparator
	err = coherence.AddIndexWithComparator(ctx, namedMap, idExtractor, extractors.ExtractorComparator[int](idExtractor, true))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	utils.Sleep(10)

	log.Println("canFindIndex Index")
	g.Expect(canFindIndex(g, namedMap)).To(gomega.BeTrue())

	log.Println("Remove Index")
	err = coherence.RemoveIndex(ctx, namedMap, extractors.Extract[int]("id"))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	utils.Sleep(10)

	log.Println("canFindIndex Index")
	g.Expect(canFindIndex(g, namedMap)).To(gomega.BeFalse())
}

func canFindIndex(g *gomega.WithT, namedMap coherence.NamedMap[int, utils.Person]) bool {
	var jsonData []byte
	jsonResult, err := coherence.AggregateFilter[int, utils.Person, map[string]interface{}](ctx, namedMap, filters.Equal(extractors.Extract[int]("id"), 1),
		aggregators.QueryRecorder(aggregators.Explain))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*jsonResult)).Should(gomega.BeNumerically(">", 0))
	g.Expect((*jsonResult)["results"]).To(gomega.Not(gomega.BeNil()))
	g.Expect((*jsonResult)["type"]).To(gomega.Not(gomega.BeNil()))

	jsonData, err = json.Marshal((*jsonResult)["results"])
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	jsonString := string(jsonData)
	return strings.Contains(jsonString, "id\",\"index\":\"Simple") ||
		strings.Contains(jsonString, "id\",\"index\":\"Partitioned")
}
