/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package extractors

import (
	"strings"
)

const (
	comparatorsBasePackage = "util.comparator."
	extractorPackage       = "extractor."

	safeComparatorType      = comparatorsBasePackage + "SafeComparator"
	inverseComparatorType   = comparatorsBasePackage + "InverseComparator"
	extractorComparatorType = comparatorsBasePackage + "ExtractorComparator"
	entryComparatorType     = comparatorsBasePackage + "EntryComparator"

	universalExtractorType = extractorPackage + "UniversalExtractor"
	chainedExtractorType   = extractorPackage + "ChainedExtractor"
	identityExtractorType  = extractorPackage + "IdentityExtractor"
	multiExtractorType     = extractorPackage + "MultiExtractor"
)

// EntryComparatorStyle defines the style of an [EntryComparator].
type EntryComparatorStyle int

var (
	StyleAuto  EntryComparatorStyle
	StyleValue EntryComparatorStyle = 1
	StyleKey   EntryComparatorStyle = 2
	StyleEntry EntryComparatorStyle = 3
)

// ValueExtractor extracts a value from a given object.
// The type parameters are T = the type of the value to extract from
// and E = the type of value that will be extracted.
type ValueExtractor[T, E any] interface {
	// Extract a value from the object.
	Extract(obj T) (E, error)
}

// Comparator allows sorting of result sets.
type Comparator[T any] interface {
	Compare(obj T, comparator T) (int, error)
}

type abstractExtractor struct {
	Type string `json:"@class,omitempty"`
	Name string `json:"name,omitempty"`
}

// SafeComparator is an internal type (exported only for serialization purpose).
type SafeComparator[E any] struct {
	Type       string                  `json:"@class,omitempty"`
	Comparator *extractorComparator[E] `json:"comparator,omitempty"`
}

// Compare compares two values for sorting. Only used on the server.
// This is an internal type (exported only for serialization purpose).
func (sc *SafeComparator[T]) Compare(_ T, _ T) (int, error) {
	return 0, nil
}

type extractorComparator[E any] struct {
	Type      string                  `json:"@class,omitempty"`
	Extractor *ValueExtractor[any, E] `json:"extractor"`
}

// Compare is an internal type (exported only for serialization purpose).
func (ec *extractorComparator[T]) Compare(_ T, _ T) (int, error) {
	return 0, nil
}

type entryComparator[E any] struct {
	Type       string               `json:"@class,omitempty"`
	Style      EntryComparatorStyle `json:"style"`
	Comparator *Comparator[E]       `json:"comparator"`
}

// Compare is an internal type (exported only for serialization purpose).
func (ec *entryComparator[T]) Compare(_ T, _ T) (int, error) {
	return 0, nil
}

// EntryComparator returns an comparators used to compare map entries. Depending on the
// comparison style this comparator will compare entries', values or keys.
func EntryComparator[E any](comparator Comparator[E], style EntryComparatorStyle) Comparator[E] {
	ec := &entryComparator[E]{Type: entryComparatorType, Comparator: &comparator, Style: style}
	return ec
}

// NewSafeComparator returns a new safe Comparator.
// The type parameter is E = the type of value that will be extracted.
func NewSafeComparator[E any](extractor ValueExtractor[any, E], ascending bool) *SafeComparator[E] {
	comparatorType := inverseComparatorType
	if !ascending {
		comparatorType = safeComparatorType
	}
	ec := &extractorComparator[E]{Type: extractorComparatorType, Extractor: &extractor}
	return &SafeComparator[E]{Type: comparatorType, Comparator: ec}
}

// ExtractorComparator returns a [Comparator] which will compare the extracted value and sort the results
// based upon the value of the ascending parameter.
func ExtractorComparator[E any](extractor ValueExtractor[any, E], ascending bool) Comparator[E] {
	comparatorType := safeComparatorType
	if !ascending {
		comparatorType = inverseComparatorType
	}
	ec := &extractorComparator[E]{Type: extractorComparatorType, Extractor: &extractor}
	return &SafeComparator[E]{Type: comparatorType, Comparator: ec}
}

type universalExtractor[T, E any] struct {
	abstractExtractor
}

func newUniversalExtractor[T, E any](name string) *universalExtractor[T, E] {
	ue := &universalExtractor[T, E]{
		abstractExtractor: abstractExtractor{
			Type: universalExtractorType,
			Name: name,
		},
	}

	return ue
}

// Extract a value from the object.
func (ue *universalExtractor[T, E]) Extract(_ T) (E, error) {
	var zeroValue E
	return zeroValue, nil
}

type chainedExtractor[T, E any] struct {
	abstractExtractor
	Extractors []*universalExtractor[T, E] `json:"extractors,omitempty"`
}

type compositeExtractor[T, E any] struct {
	abstractExtractor
	Extractors []*ValueExtractor[T, E] `json:"extractors,omitempty"`
}

// Multi returns a value extractor that extracts multiple comma separated property values.
// Commonly used by aggregators such as the Reducer Aggregator. For example the following will
// create an aggregator that will return an array of map[K][]any with the key and the name and
// age attributes in a slice.
//
//	reducer := aggregators.Reducer[int](extractors.Multi("name,age"))
func Multi(properties string) ValueExtractor[any, any] {
	var (
		fields     = strings.Split(properties, ",")
		extractors = make([]*ValueExtractor[any, any], 0)
	)

	for _, field := range fields {
		e := Extract[any](field)
		extractors = append(extractors, &e)
	}
	ue := &compositeExtractor[any, any]{
		abstractExtractor: abstractExtractor{
			Type: multiExtractorType,
		},
		Extractors: extractors,
	}

	return ue
}

// Chained creates a ChainedExtractor from dot delimited properties.
// The type parameters are T = the type of the value to extract from
// and E = the type of value that will be extracted.
func Chained[T, E any](property string) ValueExtractor[T, E] {
	var (
		fields     = strings.Split(property, ".")
		extractors = make([]*universalExtractor[T, E], 0)
	)

	for _, field := range fields {
		extractors = append(extractors, newUniversalExtractor[T, E](field))
	}
	ue := &chainedExtractor[T, E]{
		abstractExtractor: abstractExtractor{
			Type: chainedExtractorType,
		},
		Extractors: extractors,
	}

	return ue
}

// Extract a value from the object.
// This is an internal type (exported only for serialization purpose).
func (ue *chainedExtractor[T, E]) Extract(_ T) (E, error) {
	var zeroValue E
	return zeroValue, nil
}

// Compare is an internal type (exported only for serialization purpose).
func (ue *chainedExtractor[T, E]) Compare(_ T, _ T) (int, error) {
	return 0, nil
}

// Extract a value from the object.
// This is an internal type (exported only for serialization purpose).
func (ue *compositeExtractor[T, E]) Extract(_ T) (E, error) {
	var zeroValue E
	return zeroValue, nil
}

// Compare is an internal type (exported only for serialization purpose).
func (ue *compositeExtractor[T, E]) Compare(_ T, _ T) (int, error) {
	return 0, nil
}

// Extract creates a ValueExtractor from an entry's value. If the property
// contains a "." (period), then a chained extractor is created otherwise a
// UniversalExtractor is created. The type parameter is E = type of extracted value.
func Extract[E any](property string) (extractor ValueExtractor[any, E]) {
	// check for '.' in the property and if so it should be a ChainedExtractor
	if strings.Contains(property, ".") {
		return Chained[any, E](property)
	}
	return newUniversalExtractor[any, E](property)
}

// Universal returns a UniversalExtractor.
func Universal[E any](property string) ValueExtractor[any, E] {
	return newUniversalExtractor[any, E](property)
}

type identityExtractor[T, E any] struct {
	abstractExtractor
}

func (ue *identityExtractor[T, E]) Extract(_ T) (E, error) {
	var zeroValue E
	return zeroValue, nil
}

// Compare is an internal type (exported only for serialization purpose).
func (ue *identityExtractor[T, E]) Compare(_ T, _ T) (int, error) {
	return 0, nil
}

// Identity returns a ValueExtractor that extracts the objects identity or key.
func Identity[V any]() ValueExtractor[any, V] {
	ie := &identityExtractor[any, V]{
		abstractExtractor: abstractExtractor{
			Type: identityExtractorType,
		},
	}
	return ie
}
