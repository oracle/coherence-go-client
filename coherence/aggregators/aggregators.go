/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package aggregators

import (
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
	"math/big"
)

const (
	aggregatorBasePackage = "aggregator."

	compositeAggregatorType = aggregatorBasePackage + "compositeAggregator"
	averageAggregatorType   = aggregatorBasePackage + "BigDecimalAverage"
	maxAggregatorType       = aggregatorBasePackage + "ComparableMax"
	minAggregatorType       = aggregatorBasePackage + "ComparableMin"
	sumAggregatorType       = aggregatorBasePackage + "BigDecimalSum"
	countAggregatorType     = aggregatorBasePackage + "Count"
	distinctAggregatorType  = aggregatorBasePackage + "DistinctValues"
	reducerAggregatorType   = aggregatorBasePackage + "ReducerAggregator"
	groupAggregatorType     = aggregatorBasePackage + "GroupAggregator"
	queryAggregatorType     = aggregatorBasePackage + "QueryRecorder"
	topAggregatorType       = aggregatorBasePackage + "TopNAggregator"
	priorityAggregatorType  = aggregatorBasePackage + "PriorityAggregator"

	// Explain produces a QueryRecord object that contains an estimated cost  of the query execution.
	Explain = "EXPLAIN"

	// Trace produces a QueryRecord object that contains the actual cost of the query execution.
	Trace = "TRACE"

	// TimoutNone indicate that this task or request can run indefinitely.
	TimoutNone = -1

	// TimeoutDefault indicates that the corresponding service's default timeout value should be used.
	TimeoutDefault = 0
)

// Aggregator acts as a common interface for all Aggregators.
type Aggregator[R any] interface {
	// AndThen performs an additional aggregation step.
	AndThen(next Aggregator[R]) Aggregator[R]
}

// groupByAggregator is an internal class (exported only for serialization purpose).
type groupByAggregator[G, T any] interface { // nolint
	// When applies a Filter to further reduce the aggregation results.
	When(filter filters.Filter) groupByAggregator[G, T]
}

// abstractAggregator is the base class for all Aggregators.
// This is an internal class (exported only for serialization purpose).
type abstractAggregator[R any] struct {
	Type     string `json:"@class,omitempty"`
	delegate Aggregator[R]
}

func newAbstractAggregator[R any](typeName string, delegate Aggregator[R]) *abstractAggregator[R] {
	return &abstractAggregator[R]{Type: typeName, delegate: delegate}
}

// AndThen combines this aggregator with the specified aggregator.
func (aa *abstractAggregator[R]) AndThen(next Aggregator[R]) Aggregator[R] {
	return newCompositeAggregator[R](aa.delegate, next)
}

// AndThen combines this aggregator with the specified aggregator.
func (aa *averageAggregator[E]) AndThen(next Aggregator[big.Rat]) Aggregator[big.Rat] {
	return newCompositeAggregator[big.Rat](aa.delegate, next)
}

type abstractAggregatorWithExtractor[E, R any] struct {
	*abstractAggregator[R]
	Extractor *extractors.ValueExtractor[any, E] `json:"extractor,omitempty"`
}

type distinctAggregatorWithExtractor[E, R any] struct {
	*abstractAggregator[[]R]
	Extractor *extractors.ValueExtractor[E, R] `json:"extractor,omitempty"`
}

type reducerAggregatorWithExtractor[K comparable] struct {
	*abstractAggregator[ReducerResult[K]]
	Extractor *extractors.ValueExtractor[any, any] `json:"extractor,omitempty"`
}

func newDistinctAggregatorWithExtractor[E, R any](typeName string, extractor extractors.ValueExtractor[E, R], delegate Aggregator[[]R]) *distinctAggregatorWithExtractor[E, R] {
	aawe := &distinctAggregatorWithExtractor[E, R]{}
	aawe.abstractAggregator = newAbstractAggregator[[]R](typeName, delegate)
	aawe.Extractor = &extractor
	return aawe
}

type averageAggregator[E any] struct {
	*abstractAggregatorWithExtractor[E, big.Rat]
}

type compositeAggregator[R any] struct {
	*abstractAggregator[R]
	Aggregators []Aggregator[R] `json:"aggregators,omitempty"`
}

type countAggregator struct {
	*abstractAggregator[int64]
}

type distinctAggregator[E, R any] struct {
	*distinctAggregatorWithExtractor[E, R]
}

type reducerAggregator[K comparable] struct {
	*reducerAggregatorWithExtractor[K]
}

// EnumType defines a type for a QueryRecorder.
// This is an internal class (exported only for serialization purpose).
type EnumType struct {
	Enum string `json:"enum"`
}

type queryRecorderAggregator[R any] struct {
	*abstractAggregator[R]
	ExplainType EnumType `json:"type"`
}

type priorityAggregator[R any] struct {
	*abstractAggregator[R]
	Aggregator         Aggregator[R] `json:"aggregator"`
	ExecutionTimeout   int64         `json:"_executionTimeout"`
	RequestTimeout     int64         `json:"_requestTimeout"`
	SchedulingPriority int64         `json:"_schedulingPriority"`
}

type groupByAggregatorImpl[G comparable, T any] struct {
	*abstractAggregatorWithExtractor[G, AggregationResult[G, T]]
	Agg  Aggregator[T]  `json:"aggregator,omitempty"`
	Fltr filters.Filter `json:"filter,omitempty"`
}

type maxAggregator[E processors.Number, R processors.Number] struct {
	*abstractAggregatorWithExtractor[E, R]
}

type sumAggregator[E processors.Number, R big.Rat] struct {
	*abstractAggregatorWithExtractor[E, R]
}

type minAggregator[E processors.Number, R processors.Number] struct {
	*abstractAggregatorWithExtractor[E, R]
}

// Average calculates an average for values of any numeric type extracted from a set
// of entries in a NamedMap or NamedCache. If the set of entries is empty, a nil value is returned.
// The type parameter is E = type of the value to average.
func Average[E processors.Number](extractor extractors.ValueExtractor[any, E]) Aggregator[big.Rat] {
	aa := &averageAggregator[E]{}
	aa.abstractAggregatorWithExtractor = newAbstractAggregatorWithExtractor[E, big.Rat](averageAggregatorType, extractor, aa)
	return aa
}

func newCompositeAggregator[R any](aggregators ...Aggregator[R]) *compositeAggregator[R] {
	compAggre := &compositeAggregator[R]{Aggregators: []Aggregator[R]{}}
	compAggre.abstractAggregator = newAbstractAggregator[R](compositeAggregatorType, compAggre)
	compAggre.Aggregators = append(compAggre.Aggregators, aggregators...)
	return compAggre
}

// AndThen performs an additional aggregation step.
func (aa *compositeAggregator[R]) AndThen(next Aggregator[R]) Aggregator[R] {
	aa.Aggregators = append(aa.Aggregators, next)
	return aa
}

// Count calculates the number of values in an entry set.
func Count() Aggregator[int64] {
	ca := &countAggregator{}
	ca.abstractAggregator = newAbstractAggregator[int64](countAggregatorType, ca)
	return ca
}

// Distinct returns an aggregator to get the set of unique values extracted from a set of entries in a NamedMap or NamedCache.
// If the set of entries is empty, an empty array is returned. The type parameters is R = the type of the result.
// The type parameter is R = type of the distinct value.
func Distinct[R any](extractor extractors.ValueExtractor[any, R]) Aggregator[[]R] {
	aa := &distinctAggregator[any, R]{}
	aa.distinctAggregatorWithExtractor = newDistinctAggregatorWithExtractor[any, R](distinctAggregatorType, extractor, aa)
	return aa
}

func newAbstractAggregatorWithExtractor[E, R any](typeName string, extractor extractors.ValueExtractor[any, E], delegate Aggregator[R]) *abstractAggregatorWithExtractor[E, R] {
	aawe := &abstractAggregatorWithExtractor[E, R]{}
	aawe.abstractAggregator = newAbstractAggregator[R](typeName, delegate)
	aawe.Extractor = &extractor
	return aawe
}

// Reducer returns an aggregator to return a portion of values from a NamedMap or NamedCache based on an extractor.
// If the set of entries is empty, an empty map is returned. The type parameters is K = the type of the key.
// The type parameter is K = type of the key.
func Reducer[K comparable](extractor extractors.ValueExtractor[any, any]) Aggregator[ReducerResult[K]] {
	aawe := &reducerAggregatorWithExtractor[K]{}
	aa := &reducerAggregator[K]{}
	aawe.abstractAggregator = &abstractAggregator[ReducerResult[K]]{Type: reducerAggregatorType, delegate: aa}
	aawe.Extractor = &extractor
	aa.reducerAggregatorWithExtractor = aawe

	return aa
}

// QueryRecorder returns and aggregator to return an estimated or actual cost of the query execution for a given filter.
// The result is a JSON struct map[string]interface{}.
func QueryRecorder(explainType string) Aggregator[map[string]interface{}] {
	aa := &abstractAggregator[map[string]interface{}]{Type: queryAggregatorType}
	query := &queryRecorderAggregator[map[string]interface{}]{ExplainType: EnumType{explainType}, abstractAggregator: aa}
	aa.delegate = query
	return query
}

// Priority returns an aggregator to explicitly control the scheduling priority
// and timeouts for execution of Aggregators. You can use Timeout_None or TimeoutDefault for execution
// request timeout values. The type parameter is R = the type of the result.
func Priority[R any](executionTimeout, requestTimeout, schedulingPriority int64, aggregator Aggregator[R]) Aggregator[R] {
	pa := &priorityAggregator[R]{Aggregator: aggregator, ExecutionTimeout: executionTimeout,
		RequestTimeout: requestTimeout, SchedulingPriority: schedulingPriority}
	pa.abstractAggregator = newAbstractAggregator[R](priorityAggregatorType, pa)
	return pa
}

type topNAggregator[E, R any] struct {
	*abstractTopNAggregatorWithExtractor[R, []R]
	Comparator *extractors.SafeComparator[E] `json:"comparator,omitempty"`
	Results    int                           `json:"results"`
}

type abstractTopNAggregatorWithExtractor[E, R any] struct {
	*abstractAggregator[R]
	Extractor *extractors.ValueExtractor[any, E] `json:"extractor,omitempty"`
}

// The TopN aggregates the top N extracted values into an array.
// The extracted values must not be null, but do not need to be unique.
// The type parameters are E = the type of the extracted value and R = the type of the result.
func TopN[E, R any](extractor extractors.ValueExtractor[any, E], ascending bool, results int) Aggregator[[]R] {
	aawe := &abstractTopNAggregatorWithExtractor[R, []R]{}
	topN := &topNAggregator[E, R]{Results: results, abstractTopNAggregatorWithExtractor: aawe,
		Comparator: extractors.NewSafeComparator[E](extractor, ascending)}
	topN.abstractAggregator = newAbstractAggregator[[]R](topAggregatorType, topN)
	ue := extractors.Identity[R]()
	topN.Extractor = &ue

	return topN
}

// GroupBy provides an ability to split a subset of entries in a
// NamedMap or NamedCache into a collection of non-intersecting subsets and then
// aggregate them separately and independently. The splitting (grouping) is
// performed using the results of the underlying property in such a way
// that two entries will belong to the same group if and only if the result of
// the corresponding property call produces the same value or tuple (list of values).
//
// After the entries are split into the groups, the underlying aggregator is
// applied separately to each group. The result of the aggregation by the
// GroupAggregator is a Map that has distinct values (or tuples) as keys and
// results of the individual aggregation as values. Additionally, those results
// could be further reduced using an optional.
// The type parameters are G = type to group by value, T = type of the aggregator result.
//
// For example, the following example returns the maximum salary by department.
//
//	salaryResult, err = coherence.Aggregate(ctx, namedMap,
//	    aggregators.GroupBy(extractors.Extract[string]("department"), aggregators.Max(extractors.Extract[float32]("salary"))))
func GroupBy[G comparable, T any](extractor extractors.ValueExtractor[any, G], agg Aggregator[T]) Aggregator[AggregationResult[G, T]] {
	aa := &groupByAggregatorImpl[G, T]{Agg: agg}
	aa.abstractAggregatorWithExtractor = newAbstractAggregatorWithExtractor[G, AggregationResult[G, T]](groupAggregatorType, extractor, aa)
	return aa
}

// When applies a Filter to further reduce the aggregation results.
func (aa *groupByAggregatorImpl[G, T]) When(fltr filters.Filter) Aggregator[AggregationResult[G, T]] {
	aa.Fltr = fltr
	return aa
}

func (aa *groupByAggregatorImpl[G, T]) AndThen(next Aggregator[AggregationResult[G, T]]) Aggregator[AggregationResult[G, T]] {
	return newCompositeAggregator[AggregationResult[G, T]](aa.delegate, next)
}

// Max calculates a maximum of numeric values of any numeric type extracted from a set
// of entries in a NamedMap or NamedCache. If the set of entries is empty, a nil value is returned.
// The type parameter is E = type to extract.
func Max[E processors.Number](extractor extractors.ValueExtractor[any, E]) Aggregator[E] {
	aa := &maxAggregator[E, E]{}
	aa.abstractAggregatorWithExtractor = newAbstractAggregatorWithExtractor[E, E](maxAggregatorType, extractor, aa)
	return aa
}

// Min calculates a minimum of numeric values of any numeric type extracted from a set
// of entries in a NamedMap or NamedCache. If the set of entries is empty, a nil value is returned.
// The type parameter is E = type to extract.
func Min[E processors.Number](extractor extractors.ValueExtractor[any, E]) Aggregator[E] {
	aa := &minAggregator[E, E]{}
	aa.abstractAggregatorWithExtractor = newAbstractAggregatorWithExtractor[E, E](minAggregatorType, extractor, aa)
	return aa
}

// Sum calculates a sum of numeric values of any numeric type extracted from a set
// of entries in a NamedMap or NamedCache. If the set of entries is empty, a nil value is returned.
// The return type of the aggregator is a big.Rat and must be converted to the required type
// before use. The type parameter is E = type to extract.
func Sum[E processors.Number](extractor extractors.ValueExtractor[any, E]) Aggregator[big.Rat] {
	aa := &sumAggregator[E, big.Rat]{}
	aa.abstractAggregatorWithExtractor = newAbstractAggregatorWithExtractor[E, big.Rat](sumAggregatorType, extractor, aa)
	return aa
}

// ReducerResult wraps a result which returns a map of slice of entries.
// The type parameter is K = the type of the key.
type ReducerResult[K comparable] struct {
	Entries []Entry[K, []any] `json:"entries"`
}

// AggregationResult wraps a result which returns a slice of entries.
// The type parameters are K = type of the key and V= type of the value.
type AggregationResult[K comparable, V any] struct {
	Entries []Entry[K, V] `json:"entries"`
}

// Entry wraps a Key and a Value.
// The type parameters are K = type of the key and V= type of the value.
type Entry[K comparable, V any] struct {
	Type  string `json:"@type"`
	Key   K      `json:"key"`
	Value V      `json:"value"`
}
