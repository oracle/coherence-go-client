/*
 * Copyright (c) 2023, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/aggregators"
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/test/utils"
	"math/big"
	"sort"
	"testing"
)

// TestAggregatorAgainstMapAndCache runs all aggregator tests against NamedMap and NamedCache
func TestAggregatorAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, utils.Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, utils.Person])
	}{
		{"NamedMapRunTestCount", utils.GetNamedMap[int, utils.Person](g, session, "aggregate-map"), RunTestCountAggregator},
		{"NamedCacheRunTestCount", utils.GetNamedCache[int, utils.Person](g, session, "aggregate-cache"), RunTestCountAggregator},
		{"NamedMapRunTestMin", utils.GetNamedMap[int, utils.Person](g, session, "aggregate-map"), RunTestMinAggregator},
		{"NamedCacheRunTestMin", utils.GetNamedCache[int, utils.Person](g, session, "aggregate-cache"), RunTestMinAggregator},
		{"NamedMapRunTestMax", utils.GetNamedMap[int, utils.Person](g, session, "aggregate-map"), RunTestMaxAggregator},
		{"NamedCacheRunTestMax", utils.GetNamedCache[int, utils.Person](g, session, "aggregate-cache"), RunTestMaxAggregator},
		{"NamedMapRunTestDistinct", utils.GetNamedMap[int, utils.Person](g, session, "aggregate-map"), RunTestDistinctAggregator},
		{"NamedCacheRunTestDistinct", utils.GetNamedCache[int, utils.Person](g, session, "aggregate-cache"), RunTestDistinctAggregator},
		{"NamedMapRunTestGroupBy", utils.GetNamedMap[int, utils.Person](g, session, "aggregate-map"), RunTestGroupByAggregator},
		{"NamedCacheRunTestGroupBy", utils.GetNamedCache[int, utils.Person](g, session, "aggregate-cache"), RunTestGroupByAggregator},
		{"NamedMapRunTestTopNAggregator", utils.GetNamedMap[int, utils.Person](g, session, "topn-map"), RunTestTopNAggregator},
		{"NamedCacheRunTestTopNAggregator", utils.GetNamedCache[int, utils.Person](g, session, "topn-cache"), RunTestTopNAggregator},
		{"NamedMapRunTestReducerAggregator", utils.GetNamedMap[int, utils.Person](g, session, "reducer-map"), RunTestReducerAggregator},
		{"NamedCacheRunTestReducerAggregator", utils.GetNamedCache[int, utils.Person](g, session, "reducer-cache"), RunTestReducerAggregator},
		{"NamedMapRunTestQueryRecorderAggregator", utils.GetNamedMap[int, utils.Person](g, session, "query-map"), RunTestQueryRecorderAggregator},
		{"NamedCacheRunTestQueryRecorderAggregator", utils.GetNamedCache[int, utils.Person](g, session, "query-cache"), RunTestQueryRecorderAggregator},
		{"NamedMapRunTestSum", utils.GetNamedMap[int, utils.Person](g, session, "aggregate-map"), RunTestSumAggregator},
		{"NamedCacheRunTestSum", utils.GetNamedCache[int, utils.Person](g, session, "aggregate-cache"), RunTestSumAggregator},
		{"NamedMapRunTestAverage", utils.GetNamedMap[int, utils.Person](g, session, "aggregate-map"), RunTestAverageAggregator},
		{"NamedCacheRunTestAverage", utils.GetNamedCache[int, utils.Person](g, session, "aggregate-cache"), RunTestAverageAggregator},
		{"NamedMapRunTestPriorityAggregator", utils.GetNamedMap[int, utils.Person](g, session, "priority-map"), RunTestPriorityAggregator},
		{"NamedCacheRunTestPriorityAggregator", utils.GetNamedCache[int, utils.Person](g, session, "priority-cache"), RunTestPriorityAggregator},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

var (
	personData = map[int]utils.Person{
		1: {ID: 1, Name: "Tim", Age: 10},
		2: {ID: 2, Name: "Andrew", Age: 20},
		3: {ID: 3, Name: "John", Age: 30},
		4: {ID: 4, Name: "Steve", Age: 40},
	}
	personCount = len(personData)

	personData2 = map[int]utils.Person{
		1: {ID: 1, Name: "Helen", Age: 40, Department: "IT", Salary: 10000},
		2: {ID: 2, Name: "Jasmine", Age: 20, Department: "HR", Salary: 12000},
		3: {ID: 3, Name: "John", Age: 45, Department: "HR", Salary: 13000},
		4: {ID: 4, Name: "Steve", Age: 43, Department: "Sales", Salary: 20000},
		5: {ID: 5, Name: "Irma", Age: 22, Department: "Sales", Salary: 21000},
		6: {ID: 6, Name: "Saul", Age: 45, Department: "Sales", Salary: 19000},
	}
	personCount2 = len(personData2)
)

func RunTestCountAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g     = gomega.NewWithT(t)
		err   error
		size  int
		count *int64
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount))

	// Count aggregator no keys or filter
	count, err = coherence.Aggregate(ctx, namedMap, aggregators.Count())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*count).To(gomega.Equal(int64(personCount)))

	// Count specific Keys 1 and 3
	count, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4}, aggregators.Count())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*count).To(gomega.Equal(int64(2)))

	// Count with Filter age > 19
	count, err = coherence.AggregateFilter(ctx, namedMap, filters.Greater(extractors.Extract[int]("age"), 19), aggregators.Count())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*count).To(gomega.Equal(int64(3)))
}

func RunTestMinAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g         = gomega.NewWithT(t)
		err       error
		size      int
		ageResult *int
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount))

	// Min aggregator no keys or filter
	ageResult, err = coherence.Aggregate(ctx, namedMap, aggregators.Min(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*ageResult).To(gomega.Equal(10))

	// Min specific Keys 3 and 4
	ageResult, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4}, aggregators.Min(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*ageResult).To(gomega.Equal(30))

	// Min with Filter age > 30
	ageResult, err = coherence.AggregateFilter(ctx, namedMap, filters.Greater(extractors.Extract[int]("age"), 30),
		aggregators.Min(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*ageResult).To(gomega.Equal(40))
}

func RunTestMaxAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g         = gomega.NewWithT(t)
		err       error
		size      int
		ageResult *int
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount))

	// Max aggregator no keys or filter
	ageResult, err = coherence.Aggregate(ctx, namedMap, aggregators.Max(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*ageResult).To(gomega.Equal(40))

	// Max specific Keys 3 and 4
	ageResult, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4}, aggregators.Max(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*ageResult).To(gomega.Equal(40))

	// Max with Filter age < 30
	ageResult, err = coherence.AggregateFilter(ctx, namedMap, filters.Less(extractors.Extract[int]("age"), 30), aggregators.Max(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*ageResult).To(gomega.Equal(20))
}

func RunTestSumAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g      = gomega.NewWithT(t)
		err    error
		size   int
		bigRat *big.Rat
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount))

	// Sum aggregator no keys or filter
	bigRat, err = coherence.Aggregate(ctx, namedMap, aggregators.Sum(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(bigRat.Num().Int64()).To(gomega.Equal(int64(100)))

	// Sum Keys 3 and 4
	bigRat, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4}, aggregators.Sum(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(bigRat.Num().Int64()).To(gomega.Equal(int64(70)))

	// Sum with Filter age < 30
	bigRat, err = coherence.AggregateFilter(ctx, namedMap, filters.Less(extractors.Extract[int]("age"), 30),
		aggregators.Sum(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(bigRat.Num().Int64()).To(gomega.Equal(int64(30)))
}

func RunTestAverageAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g      = gomega.NewWithT(t)
		err    error
		size   int
		bigRat *big.Rat
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount))

	// Average aggregator no keys or filter
	bigRat, err = coherence.Aggregate(ctx, namedMap, aggregators.Average(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(bigRat.Num().Int64()).To(gomega.Equal(int64(25)))

	// Average for Keys 3 and 4
	bigRat, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4}, aggregators.Average(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(bigRat.Num().Int64()).To(gomega.Equal(int64(35)))

	// Average with Filter age < 30
	bigRat, err = coherence.AggregateFilter(ctx, namedMap, filters.Less(extractors.Extract[int]("age"), 30),
		aggregators.Average(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(bigRat.Num().Int64()).To(gomega.Equal(int64(15)))
}

func RunTestDistinctAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g         = gomega.NewWithT(t)
		err       error
		size      int
		ageResult *[]int
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount))

	// Distinct aggregator no keys or filter
	ageResult, err = coherence.Aggregate(ctx, namedMap, aggregators.Distinct(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	sort.Ints(*ageResult)
	g.Expect(*ageResult).To(gomega.Equal([]int{10, 20, 30, 40}))

	// Distinct specific Keys 3 and 4
	ageResult, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4}, aggregators.Distinct(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	sort.Ints(*ageResult)
	g.Expect(*ageResult).To(gomega.Equal([]int{30, 40}))

	// Distinct with Filter age < 30
	ageResult, err = coherence.AggregateFilter(ctx, namedMap,
		filters.Less(extractors.Extract[int]("age"), 30), aggregators.Distinct(extractors.Extract[int]("age")))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	sort.Ints(*ageResult)
	g.Expect(*ageResult).To(gomega.Equal([]int{10, 20}))
}

func RunTestGroupByAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g            = gomega.NewWithT(t)
		err          error
		size         int
		salaryResult *aggregators.AggregationResult[string, float32]
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount2))

	salaryResult, err = coherence.Aggregate(ctx, namedMap,
		aggregators.GroupBy(extractors.Extract[string]("department"), aggregators.Max(extractors.Extract[float32]("salary"))))

	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(salaryResult.Entries)).To(gomega.Equal(3))
}

func RunTestTopNAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g            = gomega.NewWithT(t)
		err          error
		size         int
		salaryResult *[]utils.Person
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount2))

	// Top 3 people by salary across all entries (descending)
	salaryResult, err = coherence.Aggregate[int, utils.Person, []utils.Person](ctx, namedMap,
		aggregators.TopN[float32, utils.Person](extractors.Extract[float32]("salary"), false, 3))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*salaryResult)).To(gomega.Equal(3))

	g.Expect((*salaryResult)[0].Salary).To(gomega.Equal(personData2[5].Salary))
	g.Expect((*salaryResult)[1].Salary).To(gomega.Equal(personData2[4].Salary))
	g.Expect((*salaryResult)[2].Salary).To(gomega.Equal(personData2[6].Salary))

	// Bottom 3 people by salary across all entries
	salaryResult, err = coherence.Aggregate[int, utils.Person, []utils.Person](ctx, namedMap,
		aggregators.TopN[float32, utils.Person](extractors.Extract[float32]("salary"), true, 3))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*salaryResult)).To(gomega.Equal(3))

	g.Expect((*salaryResult)[0].Salary).To(gomega.Equal(personData2[1].Salary))
	g.Expect((*salaryResult)[1].Salary).To(gomega.Equal(personData2[2].Salary))
	g.Expect((*salaryResult)[2].Salary).To(gomega.Equal(personData2[3].Salary))

	// Top 2 people by salary using keys
	salaryResult, err = coherence.AggregateKeys[int, utils.Person, []utils.Person](ctx, namedMap, []int{1, 2, 3},
		aggregators.TopN[float32, utils.Person](extractors.Extract[float32]("salary"), false, 2))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*salaryResult)).To(gomega.Equal(2))
	g.Expect((*salaryResult)[0].Salary).To(gomega.Equal(personData2[3].Salary))
	g.Expect((*salaryResult)[1].Salary).To(gomega.Equal(personData2[2].Salary))

	// Bottom 2 people by salary using keys
	salaryResult, err = coherence.AggregateKeys[int, utils.Person, []utils.Person](ctx, namedMap, []int{1, 2, 3},
		aggregators.TopN[float32, utils.Person](extractors.Extract[float32]("salary"), true, 2))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*salaryResult)).To(gomega.Equal(2))

	g.Expect((*salaryResult)[0].Salary).To(gomega.Equal(personData2[1].Salary))
	g.Expect((*salaryResult)[1].Salary).To(gomega.Equal(personData2[2].Salary))

	// top 2 people by salary using filter
	salaryResult, err = coherence.AggregateFilter[int, utils.Person, []utils.Person](ctx, namedMap, filters.Greater(extractors.Extract[int]("age"), 40),
		aggregators.TopN[float32, utils.Person](extractors.Extract[float32]("salary"), false, 2))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*salaryResult)).To(gomega.Equal(2))

	g.Expect((*salaryResult)[0].Salary).To(gomega.Equal(personData2[4].Salary))
	g.Expect((*salaryResult)[1].Salary).To(gomega.Equal(personData2[6].Salary))

	// bottom 2 people by salary using filter
	salaryResult, err = coherence.AggregateFilter[int, utils.Person, []utils.Person](ctx, namedMap, filters.Greater(extractors.Extract[int]("age"), 40),
		aggregators.TopN[float32, utils.Person](extractors.Extract[float32]("salary"), true, 2))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*salaryResult)).To(gomega.Equal(2))

	g.Expect((*salaryResult)[0].Salary).To(gomega.Equal(personData2[3].Salary))
	g.Expect((*salaryResult)[1].Salary).To(gomega.Equal(personData2[6].Salary))
}

func RunTestReducerAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g            = gomega.NewWithT(t)
		err          error
		size         int
		result       *aggregators.ReducerResult[int]
		resultValues []any
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount2))

	reducer := aggregators.Reducer[int](extractors.Multi("name,age"))

	// reduce all entries
	result, err = coherence.Aggregate(ctx, namedMap, reducer)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(result.Entries)).To(gomega.Equal(size))

	// reduce all entries using a set of keys
	result, err = coherence.AggregateKeys(ctx, namedMap, []int{1, 2, 3}, reducer)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(result.Entries)).To(gomega.Equal(3))

	// reduce all entries using a filter
	result, err = coherence.AggregateFilter(ctx, namedMap, filters.Greater(extractors.Extract[int]("age"), 40), reducer)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(result.Entries)).To(gomega.Equal(3))

	// reduce all entries using a single key to check results
	result, err = coherence.AggregateKeys(ctx, namedMap, []int{1}, reducer)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(result.Entries)).To(gomega.Equal(1))

	resultValues = result.Entries[0].Value
	g.Expect(len(resultValues)).To(gomega.Equal(2))
	g.Expect(resultValues[0]).To(gomega.Equal("Helen"))
	g.Expect(resultValues[1]).To(gomega.Equal(float64(40)))
}

func RunTestQueryRecorderAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g          = gomega.NewWithT(t)
		err        error
		size       int
		jsonResult *map[string]interface{}
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount2))

	// Run an explain query
	jsonResult, err = coherence.AggregateFilter[int, utils.Person, map[string]interface{}](ctx, namedMap, filters.Always(),
		aggregators.QueryRecorder(aggregators.Explain))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*jsonResult)).Should(gomega.BeNumerically(">", 0))
	g.Expect((*jsonResult)["results"]).To(gomega.Not(gomega.BeNil()))
	g.Expect((*jsonResult)["type"]).To(gomega.Not(gomega.BeNil()))

	// Run a trace query
	jsonResult, err = coherence.AggregateFilter[int, utils.Person, map[string]interface{}](ctx, namedMap, filters.Always(),
		aggregators.QueryRecorder(aggregators.Trace))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*jsonResult)).Should(gomega.BeNumerically(">", 0))
	g.Expect((*jsonResult)["results"]).To(gomega.Not(gomega.BeNil()))
	g.Expect((*jsonResult)["type"]).To(gomega.Not(gomega.BeNil()))

	// Run a trace query using different filter
	testFilter := filters.Equal(extractors.Extract[int]("age"), 11).Or(filters.Equal(extractors.Extract[string]("name"), "John"))
	jsonResult, err = coherence.AggregateFilter[int, utils.Person, map[string]interface{}](ctx, namedMap, testFilter, aggregators.QueryRecorder(aggregators.Trace))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(len(*jsonResult)).Should(gomega.BeNumerically(">", 0))
	g.Expect((*jsonResult)["results"]).To(gomega.Not(gomega.BeNil()))
	g.Expect((*jsonResult)["type"]).To(gomega.Not(gomega.BeNil()))
}

func RunTestPriorityAggregator(t *testing.T, namedMap coherence.NamedMap[int, utils.Person]) {
	var (
		g         = gomega.NewWithT(t)
		err       error
		size      int
		ageResult *[]int
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// populate data
	err = namedMap.PutAll(ctx, personData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err = namedMap.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(personCount))

	agg := aggregators.Distinct(extractors.Extract[int]("age"))

	// Priority aggregator no keys or filter
	ageResult, err = coherence.Aggregate(ctx, namedMap, aggregators.Priority(aggregators.TimeoutDefault, aggregators.TimeoutDefault, 0, agg))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	sort.Ints(*ageResult)
	g.Expect(*ageResult).To(gomega.Equal([]int{10, 20, 30, 40}))

	// Max specific Keys 3 and 4
	ageResult, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4}, aggregators.Priority(aggregators.TimeoutDefault, aggregators.TimeoutDefault, 0, agg))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	sort.Ints(*ageResult)
	g.Expect(*ageResult).To(gomega.Equal([]int{30, 40}))

	// Max with Filter age < 30
	ageResult, err = coherence.AggregateFilter(ctx, namedMap,
		filters.Less(extractors.Extract[int]("age"), 30), aggregators.Priority(aggregators.TimeoutDefault, aggregators.TimeoutDefault, 0, agg))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	sort.Ints(*ageResult)
	g.Expect(*ageResult).To(gomega.Equal([]int{10, 20}))
}
