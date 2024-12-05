/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package filters

import (
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
)

const (
	filterPackage = "util.filter."

	allFilterType           = filterPackage + "AllFilter"
	alwaysFilterType        = filterPackage + "AlwaysFilter"
	andFilterType           = filterPackage + "AndFilter"
	anyFilterType           = filterPackage + "AnyFilter"
	betweenFilterType       = filterPackage + "BetweenFilter"
	containsAllFilterType   = filterPackage + "ContainsAllFilter"
	containsAnyFilterType   = filterPackage + "ContainsAnyFilter"
	containsFilterType      = filterPackage + "ContainsFilter"
	equalsFilterType        = filterPackage + "EqualsFilter"
	greaterEqualsFilterType = filterPackage + "GreaterEqualsFilter"
	greaterFilterType       = filterPackage + "GreaterFilter"
	inFilterType            = filterPackage + "InFilter"
	isNilFilterType         = filterPackage + "IsNullFilter"
	isNotNilFilterType      = filterPackage + "IsNotNullFilter"
	keyAssociatedFilterType = filterPackage + "KeyAssociatedFilter"
	lessEqualsFilterType    = filterPackage + "LessEqualsFilter"
	lessFilterType          = filterPackage + "LessFilter"
	likeFilterType          = filterPackage + "LikeFilter"
	mapEventFilterType      = filterPackage + "MapEventFilter"
	neverFilterType         = filterPackage + "NeverFilter"
	notEqualsFilterType     = filterPackage + "NotEqualsFilter"
	notFilterType           = filterPackage + "NotFilter"
	orFilterType            = filterPackage + "OrFilter"
	presentFilterType       = filterPackage + "PresentFilter"
	regexFilterType         = filterPackage + "RegexFilter"
	xorFilterType           = filterPackage + "XorFilter"
)

// Filter interface defines the common operations on all Filters.
type Filter interface {
	// And returns a composed filter that represents a short-circuiting logical
	// AND of this filter and another.  When evaluating the composed
	// filter, if this filter is false, then the other
	// filter is not evaluated.
	// The returned filter can be used for further composition.
	And(other Filter) Filter

	// Or returns a composed predicate that represents a short-circuiting logical
	// OR of this predicate and another.  When evaluating the composed
	// predicate, if this predicate is true, then the other
	// predicate is not evaluated.
	// The returned filter can be used for further composition.
	Or(other Filter) Filter

	// Xor Returns a composed predicate that represents a logical XOR of this
	// predicate and another.
	// The returned filter can be used for further composition.
	Xor(other Filter) Filter

	// AssociatedWith returns a key associated filter that limits the scope of this filter to the specified key.
	// The returned filter can be used for further composition.
	AssociatedWith(key any) Filter
}

// Between creates a filter that checks if the property value lies between 'to' and 'from' which
// always evaluates to false.
func Between[V comparable](extractor extractors.ValueExtractor[any, V], from, to V) Filter {
	return newBetweenFilter(extractor, from, to)
}

// Contains creates a Filter that tests a collection or array value returned from
// a method invocation based on the given property for containment of a
// given value.
func Contains[V any](extractor extractors.ValueExtractor[any, V], value V) Filter {
	return newContainsFilter[V](extractor, value)
}

// ContainsAll creates a filter that tests a collection or array value returned from
// a method invocation based on the given property for containment of
// all the given values.
func ContainsAll[V any](extractor extractors.ValueExtractor[any, V], values ...V) Filter {
	return newContainsAllFilter[V](extractor, values)
}

// ContainsAny creates a filter that tests a collection or array value returned from
// a method invocation based on the given property for containment of
// the given values.
func ContainsAny[V any](extractor extractors.ValueExtractor[any, V], values ...V) Filter {
	return newContainsAnyFilter[V](extractor, values...)
}

// Equal creates a Filter that compares the result of a method invocation with a value
// for equality.
func Equal[V any](extractor extractors.ValueExtractor[any, V], value V) Filter {
	return newEqualsFilter[V](extractor, value)
}

// Greater creates a Filter that checks if the result of a method invocation is greater
// than the specified value. If either result of a method invocation or a value to
// compare are equal to nil, the evaluation yields false.
func Greater[V any](extractor extractors.ValueExtractor[any, V], value V) Filter {
	return newGreaterFilter[V](extractor, value)
}

// GreaterEqual creates a Filter that checks if the result of a method invocation is greater
// or equal to the specified value. If either result of a method invocation or a value to
// compare are equal to nil, the evaluation yields false.
func GreaterEqual[V any](extractor extractors.ValueExtractor[any, V], value V) Filter {
	return newGreaterEqualsFilter[V](extractor, value)
}

// In creates a Filter which checks whether the result of a method invocation based on
// the given property belongs to a predefined set of values.
func In[V any](extractor extractors.ValueExtractor[any, V], values []V) Filter {
	return newInFilter[V](extractor, values)
}

// Like creates a filter compares the result of a method invocation based on the provided
// property with a value for pattern match. A pattern can include regular characters
// and wildcard characters '_' and '%'.
//
// During pattern matching, regular characters must exactly match the
// characters in an evaluated string. Wildcard character '_' (underscore) can
// be matched with any single character, and wildcard character '%' can be
// matched with any string fragment of zero or more characters.
func Like(extractor extractors.ValueExtractor[any, string], pattern string, ignoreCase bool) Filter {
	return newLikeFilter(extractor, pattern, ignoreCase)
}

// IsNil creates a Filter which compares the result of a method invocation with nil.
func IsNil[V any](extractor extractors.ValueExtractor[any, V]) Filter {
	return newIsNilFilter(extractor)
}

// IsNotNil creates a Filter which tests the result of a method invocation for inequality to nil.
func IsNotNil[V any](extractor extractors.ValueExtractor[any, V]) Filter {
	return newIsNotNilFilter(extractor)
}

// Less creates a Filter compares the result of a method invocation with a value for
// "Less" condition. In a case when either result of a method
// invocation or a value to compare are equal to nil, the evaluation
// yields false.
func Less[V comparable](extractor extractors.ValueExtractor[any, V], value V) Filter {
	return newLessFilter[V](extractor, value)
}

// LessEqual creates a Filter compares the result of a method invocation with a value for
// "Less or Equals" condition. In a case when either result of a method
// invocation or a value to compare are equal to nil, the evaluation
// yields false.
func LessEqual[V comparable](extractor extractors.ValueExtractor[any, V], value V) Filter {
	return newLessEqualsFilter[V](extractor, value)
}

// Never creates a Filter always evaluates to false.
func Never() Filter {
	return newNeverFilter()
}

// Not returns a filter that negates the results of the passed filter.
func Not(filter Filter) Filter {
	return newNotFilter(filter)
}

// NotEqual creates a Filter compares the result of a method invocation with a value for inequality.
func NotEqual[V any](extractor extractors.ValueExtractor[any, V], value V) Filter {
	return newNotEqualsFilter[V](extractor, value)
}

// Or creates a Filter evaluates the logical 'and' of the the two filters.
func Or(left Filter, right Filter) Filter {
	return newOrFilter(left, right)
}

// Present creates a Filter returns true for entries that currently exist in a NamedMap.
// This Filter is intended to be used solely in combination with a ConditionalProcessor.
func Present() Filter {
	return newPresentFilter()
}

// Regex creates a Filter that uses the regular expression pattern match defined by the
// Java's String.matches(String) contract.
func Regex(extractor extractors.ValueExtractor[any, string], pattern string) Filter {
	return newRegExFilter[any, string](extractor, pattern)
}

// Xor creates a  Filter evaluates the logical 'xor' of the two filters.
func Xor(left Filter, right Filter) Filter {
	return newXorFilter(left, right)
}

type allFilter struct {
	*arrayOfFilters
}

// All returns a Filter that is the logical "and" of all the specified filters.
func All(filters ...Filter) Filter {
	af := &allFilter{}
	af.arrayOfFilters = newArrayOfFiltersHolder(allFilterType, filters, af)

	return af
}

type alwaysFilter struct {
	*arrayOfFilters
}

func (af alwaysFilter) String() string {
	return "AlwaysFilter"
}

// Always returns a Filter that always evaluates to true.
func Always() Filter {
	af := &alwaysFilter{}
	af.arrayOfFilters = newArrayOfFiltersHolder(alwaysFilterType, []Filter{}, af)

	return af
}

type neverFilter struct {
	*arrayOfFilters
}

func newNeverFilter() *neverFilter {
	nf := &neverFilter{}
	nf.arrayOfFilters = newArrayOfFiltersHolder(neverFilterType, []Filter{}, nf)

	return nf
}

// And filter evaluates the logical 'and' of the two filters.
// When evaluating the composed filter, if the 'left' filter is false, then the other
// filter is not evaluated. The returned filter can be used for further composition.
func And(left Filter, right Filter) Filter {
	af := &allFilter{}
	af.arrayOfFilters = newArrayOfFiltersHolder(andFilterType, []Filter{left, right}, af)

	return af
}

type anyFilter struct {
	*arrayOfFilters
}

// Any Filter returns the logical "or" of a filter array.
// When evaluating the composed filter, if any of the specified filters
// evaluates to true, then the other filters are not evaluated.
// The returned filter can be used for further composition.
func Any(filters ...Filter) Filter {
	af := &anyFilter{}
	af.arrayOfFilters = newArrayOfFiltersHolder(anyFilterType, filters, af)

	return af
}

// ArrayOfFilters represents a base structure for an array of Filters.
type arrayOfFilters struct {
	Type     string   `json:"@class,omitempty"`
	Filters  []Filter `json:"filters,omitempty"`
	delegate Filter
}

func newArrayOfFiltersHolder(typeName string, filters []Filter, delegate Filter) *arrayOfFilters {
	af := &arrayOfFilters{
		Type:     typeName,
		Filters:  filters,
		delegate: delegate,
	}

	return af
}

// And returns a composed filter that represents a short-circuiting logical
// AND of this filter and another.  When evaluating the composed filter, if
// this filter is false, then the other filter is not evaluated.
func (af *arrayOfFilters) And(other Filter) Filter {
	return And(af.delegate, other)
}

// Or returns a composed predicate that represents a short-circuiting logical
// OR of this predicate and another.  When evaluating the composed predicate, if
// this predicate is true, then the other predicate is not evaluated.
func (af *arrayOfFilters) Or(other Filter) Filter {
	return Or(af.delegate, other)
}

// Xor returns a composed predicate that represents a logical XOR of this
// predicate and the other predicate.
func (af *arrayOfFilters) Xor(other Filter) Filter {
	return newXorFilter(af.delegate, other)
}

// AssociatedWith returns a key associated filter based on this filter and a specified key.
func (af *arrayOfFilters) AssociatedWith(key any) Filter {
	return newKeyAssociatedFilter(af.delegate, key)
}

type xorFilter struct {
	*arrayOfFilters
}

func newXorFilter(left Filter, right Filter) *xorFilter {
	af := &xorFilter{}
	af.arrayOfFilters = newArrayOfFiltersHolder(xorFilterType, []Filter{left, right}, af)

	return af
}

type containsAnyFilter[V any] struct {
	*comparisonFilterValues[V]
}

// newEqualsFilter extracts a value for a Filter.
func newContainsAnyFilter[V any](extractor extractors.ValueExtractor[any, V], values ...V) *containsAnyFilter[V] {
	ef := &containsAnyFilter[V]{}
	ef.comparisonFilterValues = newComparisonFilterValues[V](containsAnyFilterType, extractor, values, ef)

	return ef
}

type containsFilter[V any] struct {
	*comparisonFilter[V]
}

// newContainsFilter returns a filter using contains
func newContainsFilter[V any](extractor extractors.ValueExtractor[any, V], value V) *containsFilter[V] {
	ef := &containsFilter[V]{}
	ef.comparisonFilter = newComparisonFilter[V](containsFilterType, extractor, value, ef)

	return ef
}

// AssociatedWith returns a key associated filter based on this filter and a specified key.
func (ef *extractorFilter[T, E]) AssociatedWith(key any) Filter {
	return newKeyAssociatedFilter(ef.delegate, key)
}

func newKeyAssociatedFilter[K any](fltr Filter, key K) *keyAssociatedFilter[K] {
	kaf := &keyAssociatedFilter[K]{HostKey: key}
	kaf.singleFilterHolder = newSingleFilterHolder(keyAssociatedFilterType, fltr, kaf)

	return kaf
}

type betweenFilter[V comparable] struct {
	*arrayOfFilters
	PreserveOrder bool `json:"preserveOrder"`
}

func newBetweenFilter[V comparable](extractor extractors.ValueExtractor[any, V], from, to V) Filter {
	bf := &betweenFilter[V]{}
	left := newGreaterEqualsFilter[V](extractor, from)
	right := newLessEqualsFilter[V](extractor, to)
	bf.arrayOfFilters = newArrayOfFiltersHolder(betweenFilterType, []Filter{left, right}, bf)

	return bf
}

type comparisonFilter[V any] struct {
	*extractorFilter[any, V]
	Value    V `json:"value"`
	delegate Filter
}

type comparisonFilterNil[V any] struct {
	*extractorFilter[any, V]
	Value    V `json:"value,omitempty"`
	delegate Filter
}

type comparisonFilterValues[V any] struct {
	*extractorFilter[any, V]
	Value    []V `json:"value"`
	delegate Filter
}

func newComparisonFilter[V any](typeName string, extractor extractors.ValueExtractor[any, V], value V, delegate Filter) *comparisonFilter[V] {
	compFilter := &comparisonFilter[V]{Value: value, delegate: delegate}
	compFilter.extractorFilter = newExtractorFilter[any, V](typeName, extractor, delegate)
	return compFilter
}

func newComparisonFilterNil[V any](typeName string, extractor extractors.ValueExtractor[any, V], value V, delegate Filter) *comparisonFilterNil[V] {
	compFilter := &comparisonFilterNil[V]{Value: value, delegate: delegate}
	compFilter.extractorFilter = newExtractorFilter[any, V](typeName, extractor, delegate)
	return compFilter
}

func newComparisonFilterValues[V any](typeName string, extractor extractors.ValueExtractor[any, V], values []V, delegate Filter) *comparisonFilterValues[V] {
	compFilter := &comparisonFilterValues[V]{Value: values, delegate: delegate}
	compFilter.extractorFilter = newExtractorFilter[any, V](typeName, extractor, delegate)
	return compFilter
}

type containsAllFilter[V any] struct {
	*comparisonFilterValues[V]
}

func newContainsAllFilter[V any](extractor extractors.ValueExtractor[any, V], values []V) *containsAllFilter[V] {
	ef := &containsAllFilter[V]{}
	ef.comparisonFilterValues = newComparisonFilterValues[V](containsAllFilterType, extractor, values, ef)

	return ef
}

type equalsFilter[V any] struct {
	*comparisonFilter[V]
}

// newEqualsFilter creates a filter that returns `true` if the specified property's value
// matches the specified value.
func newEqualsFilter[V any](extractor extractors.ValueExtractor[any, V], value V) *equalsFilter[V] {
	ef := &equalsFilter[V]{}
	ef.comparisonFilter = newComparisonFilter[V](equalsFilterType, extractor, value, ef)

	return ef
}

type extractorFilter[T, E any] struct {
	Type      string                          `json:"@class,omitempty"`
	Extractor extractors.ValueExtractor[T, E] `json:"extractor,omitempty"`
	delegate  Filter
}

// newExtractorFilter creates a new ExtractorFilter whose Evaluate() method delegates to another filter.
func newExtractorFilter[T, E any](typeName string, extractor extractors.ValueExtractor[T, E], delegate Filter) *extractorFilter[T, E] {
	return &extractorFilter[T, E]{Type: typeName, Extractor: extractor, delegate: delegate}
}

// And returns a composed filter that represents a short-circuiting logical
// AND of this filter and another.  When evaluating the composed filter, if
// this filter is false, then the other filter is not evaluated.
func (ef *extractorFilter[T, E]) And(other Filter) Filter {
	return And(ef.delegate, other)
}

// Or returns a composed predicate that represents a short-circuiting logical
// OR of this predicate and another.  When evaluating the composed predicate, if
// this predicate is true, then the other predicate is not evaluated.
func (ef *extractorFilter[T, E]) Or(other Filter) Filter {
	return newOrFilter(ef.delegate, other)
}

// Xor returns a composed predicate that represents a logical XOR of this
// predicate and the other predicate.
func (ef *extractorFilter[T, E]) Xor(other Filter) Filter {
	return newXorFilter(ef.delegate, other)
}

type greaterEqualsFilter[V any] struct {
	*comparisonFilter[V]
}

// newGreaterEqualsFilter extracts a value for a Filter.
func newGreaterEqualsFilter[V any](extractor extractors.ValueExtractor[any, V], value V) *greaterEqualsFilter[V] {
	gef := &greaterEqualsFilter[V]{}
	gef.comparisonFilter = newComparisonFilter[V](greaterEqualsFilterType, extractor, value, gef)

	return gef
}

type greaterFilter[V any] struct {
	*comparisonFilter[V]
}

// newGreaterFilter extracts a value for a Filter.
func newGreaterFilter[V any](extractor extractors.ValueExtractor[any, V], value V) *greaterFilter[V] {
	gf := &greaterFilter[V]{}
	gf.comparisonFilter = newComparisonFilter(greaterFilterType, extractor, value, gf)

	return gf
}

type inFilter[V any] struct {
	*comparisonFilterValues[V]
}

func newInFilter[V any](extractor extractors.ValueExtractor[any, V], values []V) *inFilter[V] {
	ef := &inFilter[V]{}
	ef.comparisonFilterValues = newComparisonFilterValues[V](inFilterType, extractor, values, ef)

	return ef
}

type isNilFilter[V any] struct {
	*comparisonFilterNil[V]
}

func newIsNilFilter[V any](extractor extractors.ValueExtractor[any, V]) *isNilFilter[V] {
	var zeroValue V
	nf := &isNilFilter[V]{}
	nf.comparisonFilterNil = newComparisonFilterNil[V](isNilFilterType, extractor, zeroValue, nf)

	return nf
}

type isNotNilFilter[V any] struct {
	*comparisonFilterNil[V]
}

// newIsNotNilFilter extracts a value for a Filter.
func newIsNotNilFilter[V any](extractor extractors.ValueExtractor[any, V]) *isNotNilFilter[V] {
	var zeroValue V
	nf := &isNotNilFilter[V]{}
	nf.comparisonFilterNil = newComparisonFilterNil[V](isNotNilFilterType, extractor, zeroValue, nf)

	return nf
}

type keyAssociatedFilter[K any] struct {
	*singleFilterHolder
	HostKey K `json:"hostKey,omitempty"`
}

type lessEqualsFilter[V comparable] struct {
	*comparisonFilter[V]
}

// newEqualsFilter extracts a value for a Filter.
func newLessEqualsFilter[V comparable](extractor extractors.ValueExtractor[any, V], value V) *lessEqualsFilter[V] {
	leq := &lessEqualsFilter[V]{}
	leq.comparisonFilter = newComparisonFilter[V](lessEqualsFilterType, extractor, value, leq)

	return leq
}

type lessFilter[V comparable] struct {
	*comparisonFilter[V]
}

// newLessFilter extracts a value for a Filter.
func newLessFilter[V comparable](extractor extractors.ValueExtractor[any, V], value V) *lessFilter[V] {
	lf := &lessFilter[V]{}
	lf.comparisonFilter = newComparisonFilter[V](lessFilterType, extractor, value, lf)

	return lf
}

type likeFilter[V string] struct {
	*comparisonFilter[V]
	IgnoreCase bool `json:"ignoreCase"`
}

// newEqualsFilter extracts a value for a Filter.
func newLikeFilter[V string](extractor extractors.ValueExtractor[any, V], pattern V, ignoreCase bool) *likeFilter[V] {
	ef := &likeFilter[V]{IgnoreCase: ignoreCase}
	ef.comparisonFilter = newComparisonFilter[V](likeFilterType, extractor, pattern, ef)

	return ef
}

type MapEventMask int

const (
	// MaskInserted indicates that insert events should be evaluated.
	// The event will be fired if there is no filter specified or if the
	// filter evaluates to true for a new value.
	MaskInserted MapEventMask = 0x0001

	// MaskUpdated indicates that update events should be evaluated.
	// The event will be fired if there is no filter specified or the
	// filter evaluates to true when applied to either old or new value.
	MaskUpdated MapEventMask = 0x0002

	// MaskDeleted indicates that delete events should be evaluated.
	// The event will be fired if there is no filter specified or the
	// filter evaluates to true for an old value.
	MaskDeleted MapEventMask = 0x0004

	// MaskAll indicates that all events should be evaluated.
	MaskAll MapEventMask = MaskInserted | MaskUpdated | MaskDeleted
)

func (m MapEventMask) String() string {
	switch m {
	case MaskAll:
		return "ALL"
	case MaskUpdated:
		return "UPDATED"
	case MaskDeleted:
		return "DELETED"
	case MaskInserted:
		return "INSERTED"
	default:
		return "UNKNOWN"
	}
}

type MapEventFilter struct {
	*singleFilterHolder
	Mask MapEventMask `json:"mask"`
}

func (mef MapEventFilter) String() string {
	return fmt.Sprintf("MapEventFilter{mask=%v, type=%v, filter=%v}",
		mef.Mask, mef.singleFilterHolder.Type, mef.singleFilterHolder.Filter)
}

func NewEventFilter(mask MapEventMask, filter Filter) *MapEventFilter {
	af := &MapEventFilter{}
	filterLocal := filter
	if filterLocal == nil {
		filterLocal = Always()
	}
	af.singleFilterHolder = newSingleFilterHolder(mapEventFilterType, filterLocal, af)
	af.Mask = mask
	return af
}

func NewEventFilterFromMask(mask MapEventMask) *MapEventFilter {
	filter := Always()
	return NewEventFilter(mask, filter)
}

func NewEventFilterFromFilter(filter Filter) *MapEventFilter {
	return NewEventFilter(MaskAll, filter)
}

type notEqualsFilter[V any] struct {
	*comparisonFilter[V]
}

// newNotEqualsFilter extracts a value for a Filter.
func newNotEqualsFilter[V any](extractor extractors.ValueExtractor[any, V], value V) *notEqualsFilter[V] {
	neq := &notEqualsFilter[V]{}
	neq.comparisonFilter = newComparisonFilter[V](notEqualsFilterType, extractor, value, neq)

	return neq
}

type notFilter struct {
	*singleFilterHolder
}

func newNotFilter(filter Filter) *notFilter {
	nf := &notFilter{}
	nf.singleFilterHolder = newSingleFilterHolder(notFilterType, filter, nf)
	return nf
}

type orFilter struct {
	*arrayOfFilters
}

func newOrFilter(left Filter, right Filter) *orFilter {
	of := &orFilter{}
	of.arrayOfFilters = newArrayOfFiltersHolder(orFilterType, []Filter{left, right}, of)

	return of
}

type presentFilter struct {
	*singleFilterHolder
}

func newPresentFilter() *presentFilter {
	pf := &presentFilter{}
	pf.singleFilterHolder = newSingleFilterHolder(presentFilterType, nil, pf)

	return pf
}

type regExFilter[T any, E string] struct {
	*extractorFilter[T, E]
	Value E `json:"value"`
}

// newRegExFilter extracts a regular expression for a Filter.
func newRegExFilter[T any, E string](extractor extractors.ValueExtractor[T, E], pattern E) *regExFilter[T, E] {
	filter := &regExFilter[T, E]{Value: pattern}
	filter.extractorFilter = newExtractorFilter[T, E](regexFilterType, extractor, filter)

	return filter
}

type singleFilterHolder struct {
	Type     string `json:"@class,omitempty"`
	Filter   Filter `json:"filter,omitempty"`
	delegate Filter
}

func newSingleFilterHolder(typeName string, filter Filter, delegate Filter) *singleFilterHolder {
	af := &singleFilterHolder{
		Type:     typeName,
		Filter:   filter,
		delegate: delegate,
	}

	return af
}

// And returns a composed filter that represents a short-circuiting logical
// AND of this filter and another.  When evaluating the composed filter, if
// this filter is false, then the other filter is not evaluated.
func (sf *singleFilterHolder) And(other Filter) Filter {
	return And(sf.delegate, other)
}

// Or returns a composed predicate that represents a short-circuiting logical
// OR of this predicate and another.  When evaluating the composed predicate, if
// this predicate is true, then the other predicate is not evaluated.
func (sf *singleFilterHolder) Or(other Filter) Filter {
	return Or(sf.delegate, other)
}

// Xor returns a composed predicate that represents a logical XOR of this
// predicate and the other predicate.
func (sf *singleFilterHolder) Xor(other Filter) Filter {
	return newXorFilter(sf.delegate, other)
}

// AssociatedWith returns a key associated filter based on this filter and a specified key.
func (sf *singleFilterHolder) AssociatedWith(key any) Filter {
	return newKeyAssociatedFilter(sf.delegate, key)
}
