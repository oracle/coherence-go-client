/*
 * Copyright (c) 2025, Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package perf

import (
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/aggregators"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"testing"
)

var (
	filterIDBetween = filters.Between(idExtractor, 125_000, 150_000)
	filterCountryAU = filters.Equal(countryExtractor, "Australia")
	filterMultiple  = filters.Equal(countryExtractor, "Australia").And(filterIDBetween)
	countryInFilter = filters.In(countryExtractor, []string{"Mexico", "Australia"})
)

// TestStreamingPerformance tests streaming.
func TestStreamingPerformance(t *testing.T) {
	var iterations int64 = 10

	testCases := []struct {
		testName string
		test     func(t *testing.T, filter filters.Filter, iterations int64) *PerformanceResult
		filter   filters.Filter
		count    int64
	}{
		{"FilterBetween", RunTestFilterEntrySet, filterIDBetween, iterations},
		{"FilterEquals", RunTestFilterEntrySet, filterCountryAU, iterations},
		{"FilterMultiple", RunTestFilterEntrySet, filterMultiple, iterations},
		{"FilterAlways", RunTestFilterEntrySet, filters.Always(), iterations},
		{"FilterIn", RunTestFilterEntrySet, countryInFilter, iterations},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := tc.test(t, tc.filter, tc.count)
			mapResults[tc.testName] = result
		})
	}
}

// TestStreamingPerformance tests streaming.
func TestAggregatorPerformance(t *testing.T) {
	var iterations int64 = 10

	testCases := []struct {
		testName string
		test     func(t *testing.T, filter filters.Filter, iterations int64) *PerformanceResult
		filter   filters.Filter
		count    int64
	}{
		{"AggregatorCountAllFilter", RunCountAggregationTest, nil, iterations},
		{"AggregatorCountCountryFilter", RunCountAggregationTest, filterCountryAU, iterations},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := tc.test(t, tc.filter, tc.count)
			mapResults[tc.testName] = result
		})
	}
}

// TestKeyOperators tests key operations.
func TestKeyOperators(t *testing.T) {
	var iterations int64 = 100_000

	testCases := []struct {
		testName  string
		test      func(t *testing.T, operation string, iterations int64) *PerformanceResult
		operation string
		count     int64
	}{
		{"KeyGet", RunTestKeyOperation, "get", iterations},
		{"KeyPut", RunTestKeyOperation, "put", iterations},
		{"KeyContainsKey", RunTestKeyOperation, "containsKey", iterations},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := tc.test(t, tc.operation, tc.count)
			mapResults[tc.testName] = result
		})
	}
}

// RunTestFilterEntrySet runs tests against various filters.
func RunTestFilterEntrySet(t *testing.T, filter filters.Filter, count int64) *PerformanceResult {
	var (
		g = gomega.NewWithT(t)
		i int64
	)

	timer := newTestTimer(count)
	for i = 0; i < count; i++ {
		timer.Start()
		for ch := range config.Students.EntrySetFilter(ctx, filter) {
			g.Expect(ch.Err).To(gomega.BeNil())
			_ = ch.Value
		}
		timer.End()
	}

	return timer.Complete()
}

// RunCountAggregationTest runs tests against various aggregators.
func RunCountAggregationTest(t *testing.T, filter filters.Filter, count int64) *PerformanceResult {
	var (
		g    = gomega.NewWithT(t)
		i    int64
		fltr = filters.Always()
	)

	if filter != nil {
		fltr = filter
	}

	timer := newTestTimer(count)
	for i = 0; i < count; i++ {
		timer.Start()
		_, err := coherence.AggregateFilter[int, Student](ctx, config.Students, fltr, aggregators.Count())
		g.Expect(err).To(gomega.BeNil())
		timer.End()
	}

	return timer.Complete()
}

// RunTestKeyOperation runs key based tests.
func RunTestKeyOperation(t *testing.T, operation string, count int64) *PerformanceResult {
	g := gomega.NewWithT(t)

	size, err := config.Students.Size(ctx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	timer := newTestTimer(count)
	for i := 0; i < int(count); i++ {
		id := rnd.Intn(size) + 1
		timer.Start()
		if operation == "get" {
			_, err = config.Students.Get(ctx, id)
		} else if operation == "put" {
			_, err = config.Students.Put(ctx, id, getRandomStudent(id))
		} else if operation == "containsKey" {
			_, err = config.Students.ContainsKey(ctx, id)
		}

		g.Expect(err).To(gomega.BeNil())
		timer.End()
	}

	return timer.Complete()
}
