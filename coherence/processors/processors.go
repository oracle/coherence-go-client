/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package processors

import (
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"math/big"
)

const (
	processorPrefix = "processor."
	extractorPrefix = "extractor."

	compositeProcessorType         = processorPrefix + "CompositeProcessor"
	conditionalProcessorType       = processorPrefix + "ConditionalProcessor"
	conditionalPutProcessorType    = processorPrefix + "ConditionalPut"
	conditionalPutAllProcessorType = processorPrefix + "ConditionalPutAll"
	conditionalRemoveProcessorType = processorPrefix + "ConditionalRemove"
	extractorProcessorType         = processorPrefix + "ExtractorProcessor"
	incrementProcessorType         = processorPrefix + "NumberIncrementor"
	methodInvocationProcessorType  = processorPrefix + "MethodInvocationProcessor"
	multiplierProcessorType        = processorPrefix + "NumberMultiplier"
	preloadProcessorType           = processorPrefix + "PreloadRequest"
	touchProcessorType             = processorPrefix + "TouchProcessor"
	updateProcessorType            = processorPrefix + "UpdaterProcessor"
	versionedPutProcessorType      = processorPrefix + "VersionedPut"
	versionedPutAllProcessorType   = processorPrefix + "VersionedPutAll"
	compositeUpdaterType           = extractorPrefix + "CompositeUpdater"
	universalUpdaterType           = extractorPrefix + "UniversalUpdater"
)

// Processor interface allows composition of Processors. An instance of a Processor
// should be created using the various factory methods.
type Processor interface {
	// AndThen creates a Processor that executes the current Processor followed by the specified 'next' Processor.
	AndThen(next Processor) Processor

	// When creates a Processor that executes only if the specified Filter passes. If the underlying filter
	// expects to evaluate existent entries only it should be combined with a filter to test for presence
	// like and(present).
	When(filter filters.Filter) Processor
}

// Number represents a type that can be incremented or multiplied
type Number interface {
	~float32 | ~float64 | ~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~complex64 | ~complex128 | big.Rat | big.Int
}

// ConditionalPut puts the value if the filter returns true. While the conditional insert processing
// could be implemented via direct key-based QueryMap operations, this method is more efficient and
// enforces concurrency control without explicit locking.
func ConditionalPut[V any](filter filters.Filter, value V) Processor {
	return newConditionalPutProcessor[V](filter, value)
}

// ConditionalPutAll inserts the specified values if the filter evaluates to true. While the conditional
// insert processing could be implemented via direct key-based QueryMap operations, this is more efficient
// and enforces concurrency control without explicit locking.
func ConditionalPutAll[K comparable, V any](filter filters.Filter, entries map[K]V) Processor {
	return newConditionalPutAllProcessor[K, V](filter, entries)
}

// ConditionalRemove removes the values if the filter evaluates to true.  While the conditional remove
// could be implemented via direct key-based QueryMap operations, this is more efficient and
// enforces concurrency control without explicit locking. If returnCurrent is set to true and
// the remove does not occur, then the current value will be returned.
func ConditionalRemove(filter filters.Filter, returnCurrent ...bool) Processor {
	return newConditionalRemoveProcessor(filter, returnCurrent...)
}

// Extractor creates a processor to extract the specified property from an entry's value. If the property
// contains a "." (period), then a chained extractor is created.
func Extractor[E any](property string) Processor {
	return newExtractorProcessor[E](property)
}

// InvokeAccessor invokes an accessor method on an entry. The specified method will
// be invoked with the specified arguments. It returns a Processor that can be used
// for further composition.
func InvokeAccessor(method string, args ...interface{}) Processor {
	return newMethodInvocationProcessor(method, false, args)
}

// InvokeMutator invokes a mutator method. The specified method will
// be invoked with the specified arguments. It returns a Processor that can be used
// for further composition.
func InvokeMutator(method string, args ...interface{}) Processor {
	return newMethodInvocationProcessor(method, true, args)
}

// Increment creates an Increment Processor that increments the numeric value of the
// specified property by the specified value. If postInc is true then
// return the value as it was before it was incremented, or if false return the
// value as it is after it is incremented.
func Increment[I Number](property string, value I, postInc ...bool) Processor {
	return newNumberIncrementor[I](property, value, postInc...)
}

// Multiply creates a Multiply Processor that multiplies the numeric value of the
// specified property by the specified value.  If postInc is true then
// return the value as it was before it was incremented, or if false return the
// value as it is after it is incremented.
func Multiply[I Number](property string, value I, postInc ...bool) Processor {
	return newNumberMultiplier[I](property, value, postInc...)
}

// Preload loads an entry into a NamedMap. This processor provides a means to "pre-load"
// an entry or a collection of entries into the cache without incurring the cost of
// sending the value(s) over the network. If the corresponding entry (or entries) already
// exists in the map, or if the map does not have a loader, then invoking this Processor
// has no effect.
func Preload() Processor {
	return newPreloadProcessor()
}

// Touch touches an entry (if present) in order to trigger interceptor re-evaluation
// and possibly increment expiry time.
func Touch() Processor {
	return newTouchProcessor()
}

// Update modifies an entry's specified property with the specified value.
// The processor will return a bool indicating if the entry to be updated was present.
func Update[V any](property string, value V) Processor {
	return newUpdaterProcessor[V](property, value)
}

// VersionedPut inserts the specified value if the version specified by the new
// value matches the version of the current value.  If insertion occurs
// the version of the entry within the map will be incremented.
func VersionedPut[V any](value V, canInsert, returnCurrent bool) Processor {
	return newVersionedPutProcessor[V](value, canInsert, returnCurrent)
}

// VersionedPutAll inserts the specified value if the version specified by the new
// value matches the version of the current value.  If insertion occurs
// the version of the entry within the map will be incremented.
func VersionedPutAll[K comparable, V any](entries map[K]V, canInsert, returnCurrent bool) Processor {
	return newVersionedPutAllProcessor[K, V](entries, canInsert, returnCurrent)
}

type abstractProcessor struct {
	Type     string    `json:"@class,omitempty"`
	delegate Processor // delegate processor whose type name is set in the Type
}

func newAbstractProcessor(typeName string, delegate Processor) *abstractProcessor {
	return &abstractProcessor{
		Type:     typeName,
		delegate: delegate,
	}
}

// AndThen creates a Processor that executes the current Processor followed by the specified Processor.
func (ap *abstractProcessor) AndThen(next Processor) Processor {
	return newCompositeProcessor(ap.delegate, next)
}

// When creates a Processor that executes only if the specified Filter passes.
func (ap *abstractProcessor) When(filter filters.Filter) Processor {
	return newConditionalProcessor(filter, ap.delegate)
}

type compositeProcessor struct {
	*abstractProcessor
	Processors []Processor `json:"processors,omitempty"`
}

func newCompositeProcessor(left, right Processor) *compositeProcessor {
	cp := &compositeProcessor{}
	cp.abstractProcessor = newAbstractProcessor(compositeProcessorType, cp)
	cp.Processors = []Processor{left, right}

	return cp
}

// AndThen creates a Processor that executes the current Processor followed by the specified Processor.
func (ap *compositeProcessor) AndThen(next Processor) Processor {
	ap.Processors = append(ap.Processors, next)
	return ap
}

type conditionalProcessor struct {
	*abstractProcessor
	Filter    filters.Filter `json:"filter,omitempty"`
	Processor Processor      `json:"processor,omitempty"`
}

func newConditionalProcessor(filter filters.Filter, proc Processor) *conditionalProcessor {
	cp := &conditionalProcessor{Filter: filter, Processor: proc}
	cp.abstractProcessor = newAbstractProcessor(conditionalProcessorType, cp)

	return cp
}

type conditionalPutAllProcessor[K comparable, V any] struct {
	*abstractProcessor
	Filter     filters.Filter      `json:"filter,omitempty"`
	Entries    putAllEntries[K, V] `json:"entries,omitempty"`
	RetCurrent bool                `json:"return,omitempty"`
}

type putAllEntry[K comparable, V any] struct {
	Key   K `json:"key,omitempty"`
	Value V `json:"value,omitempty"`
}

type putAllEntries[K comparable, V any] struct {
	Entries []*putAllEntry[K, V] `json:"entries,omitempty"`
}

func newConditionalPutAllProcessor[K comparable, V any](filter filters.Filter, entries map[K]V, returnCurrent ...bool) *conditionalPutAllProcessor[K, V] {
	retCurrent := false
	if len(returnCurrent) > 0 {
		retCurrent = returnCurrent[0]
	}
	e := make([]*putAllEntry[K, V], len(entries))
	counter := 0
	for k, v := range entries {
		e[counter] = &putAllEntry[K, V]{Key: k, Value: v}
		counter++
	}

	cp := &conditionalPutAllProcessor[K, V]{Filter: filter, Entries: putAllEntries[K, V]{Entries: e}, RetCurrent: retCurrent}
	cp.abstractProcessor = newAbstractProcessor(conditionalPutAllProcessorType, cp)

	return cp
}

type conditionalPutProcessor[V any] struct {
	*abstractProcessor
	Filter     filters.Filter `json:"filter,omitempty"`
	Value      V              `json:"value,omitempty"`
	RetCurrent bool           `json:"return,omitempty"`
}

func newConditionalPutProcessor[V any](filter filters.Filter, value V, returnCurrent ...bool) *conditionalPutProcessor[V] {
	retCurrent := false
	if len(returnCurrent) > 0 {
		retCurrent = returnCurrent[0]
	}
	cp := &conditionalPutProcessor[V]{Filter: filter, Value: value, RetCurrent: retCurrent}
	cp.abstractProcessor = newAbstractProcessor(conditionalPutProcessorType, cp)

	return cp
}

// ReturnCurrent marks if this processor should return current value or not.
func (cp *conditionalPutProcessor[V]) ReturnCurrent() Processor {
	cp.RetCurrent = true
	return cp
}

type conditionalRemoveProcessor struct {
	*abstractProcessor
	Filter     filters.Filter `json:"filter,omitempty"`
	RetCurrent bool           `json:"return,omitempty"`
}

func newConditionalRemoveProcessor(filter filters.Filter, returnCurrent ...bool) *conditionalRemoveProcessor {
	retCurrent := false
	if len(returnCurrent) > 0 {
		retCurrent = returnCurrent[0]
	}
	cp := &conditionalRemoveProcessor{Filter: filter, RetCurrent: retCurrent}
	cp.abstractProcessor = newAbstractProcessor(conditionalRemoveProcessorType, cp)

	return cp
}

// ReturnCurrent marks if this processor should return current value or not.
func (cp *conditionalRemoveProcessor) ReturnCurrent(returnCurrent bool) Processor {
	cp.RetCurrent = returnCurrent
	return cp
}

type extractorProcessor[E any] struct {
	*abstractProcessor
	Extractor extractors.ValueExtractor[any, E] `json:"extractor,omitempty"`
	Name      string                            `json:"name,omitempty"`
	Params    []interface{}                     `json:"params,omitempty"`
	Target    int                               `json:"target,omitempty"` //1 for Key extractor, 0 for value extractor
}

func newExtractorProcessor[E any](property string) *extractorProcessor[E] {
	ep := &extractorProcessor[E]{Name: property, Extractor: extractors.Extract[E](property)}
	ep.abstractProcessor = newAbstractProcessor(extractorProcessorType, ep)

	return ep
}

type methodInvocationProcessor struct {
	*abstractProcessor
	MethodName string        `json:"methodName,omitempty"`
	IsMutator  bool          `json:"mutator,omitempty"`
	Args       []interface{} `json:"args"`
}

func newMethodInvocationProcessor(property string, isMutator bool, args []interface{}) *methodInvocationProcessor {
	if args == nil {
		args = make([]interface{}, 0)
	}
	ep := &methodInvocationProcessor{
		MethodName: property,
		IsMutator:  isMutator,
		Args:       args,
	}
	ep.abstractProcessor = newAbstractProcessor(methodInvocationProcessorType, ep)

	return ep
}

type numberIncrementor[I Number] struct {
	*abstractProcessor
	Manipulator   *compositeIncrementor[I] `json:"manipulator,omitempty"`
	IncBy         I                        `json:"increment,omitempty"`
	ReturnPostInc bool                     `json:"postInc,omitempty"`
}

func newNumberIncrementor[I Number](propertyName string, incBy I, postInc ...bool) *numberIncrementor[I] {
	postIncrement := false
	if len(postInc) > 0 {
		postIncrement = postInc[0]
	}
	ni := &numberIncrementor[I]{IncBy: incBy, ReturnPostInc: postIncrement}
	ni.abstractProcessor = newAbstractProcessor(incrementProcessorType, ni)
	ni.Manipulator = newCompositeIncrementor[I](propertyName)
	return ni
}

type numberMultiplier[I Number] struct {
	*abstractProcessor
	Manipulator     *compositeIncrementor[I] `json:"manipulator,omitempty"`
	MultiplyBy      I                        `json:"multiplier,omitempty"`
	ReturnPostValue bool                     `json:"postMultiplication"`
}

func newNumberMultiplier[I Number](propertyName string, multiplyBy I, postInc ...bool) *numberMultiplier[I] {
	postIncrement := false
	if len(postInc) > 0 {
		postIncrement = postInc[0]
	}
	ni := &numberMultiplier[I]{MultiplyBy: multiplyBy, ReturnPostValue: postIncrement}
	ni.abstractProcessor = newAbstractProcessor(multiplierProcessorType, ni)
	ni.Manipulator = newCompositeIncrementor[I](propertyName)

	return ni
}

type preloadProcessor struct {
	*abstractProcessor
}

func newPreloadProcessor() *preloadProcessor {
	cp := &preloadProcessor{}
	cp.abstractProcessor = newAbstractProcessor(preloadProcessorType, cp)

	return cp
}

type touchProcessor struct {
	*abstractProcessor
}

func newTouchProcessor() *touchProcessor {
	tp := &touchProcessor{}
	tp.abstractProcessor = newAbstractProcessor(touchProcessorType, tp)

	return tp
}

type updaterProcessor[V any] struct {
	*abstractProcessor
	Updater *compositeUpdater `json:"updater,omitempty"`
	Value   V                 `json:"value"`
}

func newUpdaterProcessor[V any](propertyName string, value V) *updaterProcessor[V] {
	up := &updaterProcessor[V]{Value: value}
	up.abstractProcessor = newAbstractProcessor(updateProcessorType, up)
	ie := extractors.Identity[any]()
	up.Updater = &compositeUpdater{
		Type:      compositeUpdaterType,
		Extractor: &ie,
		Updater:   newUniversalUpdater(propertyName),
	}

	return up
}

type compositeUpdater struct {
	Type      string                               `json:"@class,omitempty"`
	Extractor *extractors.ValueExtractor[any, any] `json:"extractor,omitempty"`
	Updater   *universalUpdater                    `json:"updater,omitempty"`
}

type compositeIncrementor[E any] struct {
	Type      string                             `json:"@class,omitempty"`
	Extractor *extractors.ValueExtractor[any, E] `json:"extractor,omitempty"`
	Updater   *universalUpdater                  `json:"updater,omitempty"`
}

type universalUpdater struct {
	Type string `json:"@class,omitempty"`
	Name string `json:"name,omitempty"`
}

func newUniversalUpdater(propertyName string) *universalUpdater {
	return &universalUpdater{universalUpdaterType, propertyName}
}

func newCompositeIncrementor[I Number](propertyName string) *compositeIncrementor[I] {
	ve := extractors.Extract[I](propertyName)
	return &compositeIncrementor[I]{
		Type:      compositeUpdaterType,
		Extractor: &ve,
		Updater:   newUniversalUpdater(propertyName),
	}
}

type versionedPutAllEntriesWrapper[K comparable, V any] struct {
	Type    string               `json:"@class,omitempty"`
	Entries []*putAllEntry[K, V] `json:"entries,omitempty"`
}

func newVersionedPutAllEntriesWrapper[K comparable, V any](typeStr string, entries []*putAllEntry[K, V]) *versionedPutAllEntriesWrapper[K, V] {
	return &versionedPutAllEntriesWrapper[K, V]{
		Type:    typeStr,
		Entries: entries,
	}
}

type versionedPutAllProcessor[K comparable, V any] struct {
	*abstractProcessor
	Entries    *versionedPutAllEntriesWrapper[K, V] `json:"entries,omitempty"`
	CanInsert  bool                                 `json:"insert"`
	RetCurrent bool                                 `json:"return"`
}

func newVersionedPutAllProcessor[K comparable, V any](entries map[K]V, canInsert, returnCurrent bool) *versionedPutAllProcessor[K, V] {
	e := make([]*putAllEntry[K, V], len(entries))
	counter := 0
	for k, v := range entries {
		e[counter] = &putAllEntry[K, V]{Key: k, Value: v}
		counter++
	}
	cp := &versionedPutAllProcessor[K, V]{
		Entries:    newVersionedPutAllEntriesWrapper[K, V]("java.util.HashMap", e),
		CanInsert:  canInsert,
		RetCurrent: returnCurrent,
	}

	cp.abstractProcessor = newAbstractProcessor(versionedPutAllProcessorType, cp)

	return cp
}

// ReturnCurrent marks if this processor should return current value or not.
func (cp *versionedPutAllProcessor[K, V]) ReturnCurrent(returnCurrent bool) *versionedPutAllProcessor[K, V] {
	cp.RetCurrent = returnCurrent
	return cp
}

// AllowInsert marks if this processor should allow inserts or not.
func (cp *versionedPutAllProcessor[K, V]) AllowInsert(allowInsert bool) *versionedPutAllProcessor[K, V] {
	cp.CanInsert = allowInsert
	return cp
}

type versionedPutProcessor[V any] struct {
	*abstractProcessor
	Value      V    `json:"value,omitempty"`
	CanInsert  bool `json:"insert"`
	RetCurrent bool `json:"return"`
}

func newVersionedPutProcessor[V any](value V, canInsert, returnCurrent bool) *versionedPutProcessor[V] {
	cp := &versionedPutProcessor[V]{
		Value:      value,
		CanInsert:  canInsert,
		RetCurrent: returnCurrent,
	}
	cp.abstractProcessor = newAbstractProcessor(versionedPutProcessorType, cp)

	return cp
}

// ReturnCurrent marks if this processor should return current value or not.
func (cp *versionedPutProcessor[V]) ReturnCurrent(returnCurrent bool) Processor {
	cp.RetCurrent = returnCurrent
	return cp
}

// AllowInsert marks if this processor should allow inserts or not.
func (cp *versionedPutProcessor[V]) AllowInsert(allowInsert bool) Processor {
	cp.CanInsert = allowInsert
	return cp
}
