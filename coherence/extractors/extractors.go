/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package extractors

import (
	"strings"
)

const (
	comparatorsBasePackage = "comparator."
	extractorPackage       = "extractor."

	safeComparatorType      = comparatorsBasePackage + "SafeComparator"
	inverseComparatorType   = comparatorsBasePackage + "InverseComparator"
	extractorComparatorType = comparatorsBasePackage + "ExtractorComparator"

	universalExtractorType = extractorPackage + "UniversalExtractor"
	chainedExtractorType   = extractorPackage + "ChainedExtractor"
	identityExtractorType  = extractorPackage + "IdentityExtractor"
	multiExtractorType     = extractorPackage + "MultiExtractor"
	queueKeyExtractorType  = "internal.net.queue.extractor.QueueKeyExtractor"
)

// ValueExtractor extracts a value from a given object.
// The type parameters are T = the type of the value to extract from
// and E = the type of value that will be extracted.
type ValueExtractor[T, E any] interface {
	// Extract a value from the object.
	Extract(obj T) (E, error)
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

type extractorComparator[E any] struct {
	Type      string                  `json:"@class,omitempty"`
	Extractor *ValueExtractor[any, E] `json:"extractor"`
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

// Extract a value from the object.
// This is an internal type (exported only for serialization purpose).
func (ue *compositeExtractor[T, E]) Extract(_ T) (E, error) {
	var zeroValue E
	return zeroValue, nil
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

type identityExtractor[T, E any] struct {
	abstractExtractor
}

func (ue *identityExtractor[T, E]) Extract(_ T) (E, error) {
	var zeroValue E
	return zeroValue, nil
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

type queueKeyExtractor[T, E any] struct {
	abstractExtractor
}

func (ue *queueKeyExtractor[T, E]) Extract(_ T) (E, error) {
	var zeroValue E
	return zeroValue, nil
}

func QueueKeyExtractor[V any]() ValueExtractor[any, V] {
	ie := &queueKeyExtractor[any, V]{
		abstractExtractor: abstractExtractor{
			Type: queueKeyExtractorType,
		},
	}
	return ie
}
