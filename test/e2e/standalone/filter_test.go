/*
 * Copyright (c) 2023, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"testing"
)

// TestProcessorAgainstMapAndCache runs all processor against NamedMap and NamedCache
func TestFiltersAgainstMapAndCache(t *testing.T) {
	var (
		g         = gomega.NewWithT(t)
		age       = extractors.Extract[int]("age")
		name      = extractors.Extract[string]("name")
		city      = extractors.Extract[string]("homeAddress.city")
		languages = extractors.Universal[string]("languages")
	)

	// various filters to test against
	var (
		ageBetween1and9    = filters.Between(age, 1, 9)
		ageBetween9and11   = filters.Between(age, 9, 11)
		ageEquals9         = filters.Equal(age, 9)
		ageEquals10        = filters.Equal(age, 10)
		ageInFalse         = filters.In(age, []int{1, 2, 4})
		ageInTrue          = filters.In(age, []int{10, 12, 34})
		nameInFalse        = filters.In(name, []string{"John", "Helen", "Claire"})
		nameInTrue         = filters.In(name, []string{"Paul", "Tim", "Fred"})
		ageGreaterThan11   = filters.Greater(age, 11)
		ageGreaterThan9    = filters.Greater(age, 9)
		ageGreaterEqual11  = filters.GreaterEqual(age, 11)
		ageGreaterEqual10  = filters.GreaterEqual(age, 10)
		ageLess3           = filters.Less(age, 3)
		ageLess11          = filters.Less(age, 11)
		ageLessEqual3      = filters.LessEqual(age, 3)
		ageLessEqual10     = filters.LessEqual(age, 10)
		livesInMelbourne   = filters.Equal(city, "Melbourne")
		livesInPerth       = filters.Equal(city, "Perth")
		nameLikeJo         = filters.Like(name, "Jo%", false)
		nameLikeTi         = filters.Like(name, "Ti%", false)
		nameLikeJoIgnore   = filters.Like(name, "jo%", true)
		nameLikeTiIgnore   = filters.Like(name, "ti%", true)
		cityLikeMel        = filters.Like(city, "Mel%", true)
		cityLikePer        = filters.Like(city, "Per%", true)
		nameNotTim         = filters.Not(filters.Equal(name, "Tim"))
		nameNotJohn        = filters.Not(filters.Equal(name, "John"))
		ageNot10           = filters.NotEqual(age, 10)
		ageNot11           = filters.NotEqual(age, 11)
		ageIs11OrNameJohn  = filters.Equal(age, 11).Or(filters.Equal(name, "John"))
		ageIs11OrNameTim   = filters.Equal(age, 11).Or(filters.Equal(name, "Tim"))
		ageIs10andNameJohn = filters.Equal(age, 10).And(filters.Equal(name, "John"))
		ageIs10andNameTim  = filters.Equal(age, 10).And(filters.Equal(name, "Tim"))
		xorFalse           = filters.Equal(age, 10).Xor(filters.Equal(name, "Tim"))
		xorTrue            = filters.Equal(age, 10).Xor(filters.Equal(name, "Helen"))
		anyFalse           = filters.Any(filters.Equal(age, 1), filters.Equal(name, "Claire"))
		anyTrue            = filters.Any(filters.Equal(age, 10), filters.Equal(name, "Claire"))
		allFalse           = filters.All(filters.Equal(age, 1), filters.Equal(name, "Fred"))
		allTrue            = filters.All(filters.Equal(age, 10), filters.Equal(name, "Tim"))
		containsAnyFalse   = filters.ContainsAny(languages, "German")
		containsAnyTrue    = filters.ContainsAny(languages, "French", "English")
		containsAllFalse   = filters.ContainsAll(languages, "German")
		containsAllTrue    = filters.ContainsAll(languages, "French", "English")
		containsFalse      = filters.Contains(languages, "German")
		containsTrue       = filters.Contains(languages, "French")
		xor2False          = filters.Xor(filters.Equal(age, 10), filters.Equal(name, "Tim"))
		xor2True           = filters.Xor(filters.Equal(age, 10), filters.Equal(name, "Helen"))
		regExpFalse        = filters.Regex(name, "^XXX")
		regExpTrue         = filters.Regex(name, "^T.*")
		isNilFalse         = filters.IsNil(name)
		isNilTrue          = filters.IsNil(extractors.Extract[string]("phone22"))
		isNotNilFalse      = filters.IsNotNil(extractors.Extract[string]("phone22"))
		isNotNilTrue       = filters.IsNotNil(name)
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName     string
		nameMap      coherence.NamedMap[int, utils.Person]
		test         func(t *testing.T, namedCache coherence.NamedMap[int, utils.Person], filter filters.Filter, shouldRemove bool)
		filter       filters.Filter
		shouldRemove bool
	}{
		{"NamedMapBetweenFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageBetween1and9, false},
		{"NamedMapBetweenFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageBetween9and11, true},
		{"NamedCacheBetweenFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageBetween1and9, false},
		{"NamedCacheBetweenFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageBetween9and11, true},
		{"NamedMapEqualFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageEquals9, false},
		{"NamedMapEqualFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageEquals10, true},
		{"NamedCacheEqualFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageEquals9, false},
		{"NamedCacheEqualFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageEquals10, true},
		{"NamedMapInFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageInFalse, false},
		{"NamedMapInFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageInTrue, true},
		{"NamedCacheInFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageInFalse, false},
		{"NamedCacheInFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageInTrue, true},
		{"NamedMapInFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, nameInFalse, false},
		{"NamedMapInFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageInTrue, true},
		{"NamedCacheInFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, nameInFalse, false},
		{"NamedCacheInFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, nameInTrue, true},
		{"NamedMapGreaterFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageGreaterThan11, false},
		{"NamedMapGreaterFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, nameInTrue, true},
		{"NamedCacheGreaterFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageGreaterThan11, false},
		{"NamedCacheGreaterFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageGreaterThan9, true},
		{"NamedMapGreaterEqualFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageGreaterEqual11, false},
		{"NamedMapGreaterEqualFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageGreaterEqual10, true},
		{"NamedCacheGreaterEqualFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageGreaterEqual11, false},
		{"NamedCacheGreaterEqualFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageGreaterEqual10, true},
		{"NamedMapLessFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageLess3, false},
		{"NamedMapLessFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageLess11, true},
		{"NamedCacheLessFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageLess3, false},
		{"NamedCacheLessFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageLess11, true},
		{"NamedMapLessEqualFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageLessEqual3, false},
		{"NamedMapLessEqualFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageLessEqual10, true},
		{"NamedCacheLessEqualFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageLessEqual3, false},
		{"NamedCacheLessEqualFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageLessEqual10, true},
		{"NamedMapInMelbourneFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, livesInMelbourne, false},
		{"NamedMapPerthFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, livesInPerth, true},
		{"NamedCacheMelbourneFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, livesInMelbourne, false},
		{"NamedCachePerthFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, livesInPerth, true},
		{"NamedMapLikeFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, nameLikeJo, false},
		{"NamedMapLikeFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, nameLikeTi, true},
		{"NamedCacheLikeFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, nameLikeJo, false},
		{"NamedCacheLikeFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, nameLikeTi, true},
		{"NamedMapLikeIgnoreFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, nameLikeJoIgnore, false},
		{"NamedMapLikeIgnoreFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, nameLikeTiIgnore, true},
		{"NamedCacheLikeIgnoreFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, nameLikeJoIgnore, false},
		{"NamedCacheLikeIgnoreFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, nameLikeTiIgnore, true},
		{"NamedMapLikeIgnoreChainedFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, cityLikeMel, false},
		{"NamedMapLikeIgnoreChainedFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, cityLikePer, true},
		{"NamedCacheLikeIgnoreChainedFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, cityLikeMel, false},
		{"NamedCacheLikeIgnoreChained Filter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, cityLikePer, true},
		{"NamedMapNotFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, nameNotTim, false},
		{"NamedMapNotFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, nameNotJohn, true},
		{"NamedCacheNotFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, nameNotTim, false},
		{"NamedCacheNotFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, nameNotJohn, true},
		{"NamedMapNotEqualFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageNot10, false},
		{"NamedMapNotEqualFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageNot11, true},
		{"NamedCacheNotEqualFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageNot10, false},
		{"NamedCacheNotEqualFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageNot11, true},
		{"NamedMapOrFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageIs11OrNameJohn, false},
		{"NamedMapOrFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageIs11OrNameTim, true},
		{"NamedCacheOrFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageIs11OrNameJohn, false},
		{"NamedCacheOrFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageIs11OrNameTim, true},
		{"NamedMapAndFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageIs10andNameJohn, false},
		{"NamedMapAndFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, ageIs10andNameTim, true},
		{"NamedCacheAndFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageIs10andNameJohn, false},
		{"NamedCacheAndFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, ageIs10andNameTim, true},
		{"NamedMapXorFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, xorFalse, false},
		{"NamedMapXorFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, xorTrue, true},
		{"NamedCacheXorFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, xorFalse, false},
		{"NamedCacheXorFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, xorTrue, true},
		{"NamedMapXor2Filter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, xor2False, false},
		{"NamedMapXor2Filter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, xor2True, true},
		{"NamedCacheXor2Filter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, xor2False, false},
		{"NamedCacheXor2Filter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, xor2True, true},
		{"NamedMapAnyFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, anyFalse, false},
		{"NamedMapAnyFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, anyTrue, true},
		{"NamedCacheAnyFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, anyFalse, false},
		{"NamedCacheAnyFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, anyTrue, true},
		{"NamedMapAllFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, allFalse, false},
		{"NamedMapAllFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, allTrue, true},
		{"NamedCacheAllFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, allFalse, false},
		{"NamedCacheAllFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, allTrue, true},
		{"NamedMapContainsAnyFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, containsAnyFalse, false},
		{"NamedMapContainsAnyFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, containsAnyTrue, true},
		{"NamedCacheContainsAnyFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, containsAnyFalse, false},
		{"NamedCacheContainsAnyFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, containsAnyTrue, true},
		{"NamedMapContainsAllFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, containsAllFalse, false},
		{"NamedMapContainsAllFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, containsAllTrue, true},
		{"NamedMapContainsFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, containsFalse, false},
		{"NamedMapContainsFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, containsTrue, true},
		{"NamedCacheContainsAllFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, containsAllFalse, false},
		{"NamedCacheContainsAllFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, containsAllTrue, true},
		{"NamedMapRegExpFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, regExpFalse, false},
		{"NamedMapRegExpFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, regExpTrue, true},
		{"NamedCacheRegExpFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, regExpFalse, false},
		{"NamedCacheRegExpFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, regExpTrue, true},
		{"NamedMapIsNilFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, isNilFalse, false},
		{"NamedMapIsNilFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, isNilTrue, true},
		{"NamedCacheIsNilFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, isNilFalse, false},
		{"NamedCacheIsNilFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, isNilTrue, true},
		{"NamedMapIsNotNilFilter1", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, isNotNilFalse, false},
		{"NamedMapIsNotNilFilter2", GetNamedMap[int, utils.Person](g, session, "filter"), RunTestFilter, isNotNilTrue, true},
		{"NamedCacheIsNotNilFilter1", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, isNotNilFalse, false},
		{"NamedCacheIsNotNilFilter2", GetNamedCache[int, utils.Person](g, session, "filter"), RunTestFilter, isNotNilTrue, true},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap, tc.filter, tc.shouldRemove)
		})
	}
}

// TestPresentFilter runs all tests for Present() filter
func TestPresentFilter(t *testing.T) {
	g := gomega.NewWithT(t)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, utils.Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, utils.Person])
	}{
		{"NamedMapRunTestPresentFilter", GetNamedMap[int, utils.Person](g, session, "present"), RunTestPresentFilter},
		{"NamedCacheRunTestPresentFilter", GetNamedCache[int, utils.Person](g, session, "present"), RunTestPresentFilter},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunTestFilter(t *testing.T, namedMap coherence.NamedMap[int, utils.Person], filter filters.Filter, shouldRemove bool) {
	var (
		g      = gomega.NewWithT(t)
		err    error
		person = utils.Person{ID: 1, Name: "Tim", Age: 10, Salary: 1000,
			Languages:   []string{"English", "French"},
			HomeAddress: utils.Address{Address1: "address1", Address2: "address2", City: "Perth", State: "WA", PostCode: 6000}}
		current *utils.Person
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	AssertSize(g, namedMap, 0)

	// add a new Person
	_, err = namedMap.Put(ctx, 1, person)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	AssertSize(g, namedMap, 1)

	// Attempt to remove the person using the supplied filter
	current, err = coherence.Invoke[int, utils.Person, utils.Person](ctx, namedMap, 1, processors.ConditionalRemove(filter, true))

	if shouldRemove {
		// The cache size should be zero
		g.Expect(err).NotTo(gomega.HaveOccurred())
		AssertSize(g, namedMap, 0)
	} else {
		// should not remove and should return the current value
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(current).To(gomega.Not(gomega.BeNil()))
		g.Expect(*current).To(gomega.Equal(person))
		AssertSize(g, namedMap, 1)
	}
}

func RunTestPresentFilter(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		oldValue *utils.Person
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	AssertSize(g, namedMap, 0)

	// Attempt to remove the person using the PresentFilter, this will not work as the entry is not there
	oldValue, err = coherence.Invoke[int, utils.Person, utils.Person](ctx, namedMap, 1, processors.ConditionalRemove(filters.Present(), true))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	addPerson(g, namedMap)

	// remove the person now which should succeed as it is there
	oldValue, err = coherence.Invoke[int, utils.Person, utils.Person](ctx, namedMap, 1, processors.ConditionalRemove(filters.Present(), true))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize(g, namedMap, 0)
}
