/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"errors"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence/aggregators"
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
	"log"
	"sync"
	"time"
)

var _ NamedMap[string, string] = &NamedMapClient[string, string]{}

// NamedMapClient is the implementation of the [NamedMap] interface.
// The type parameters are K = type of the key and V = type of the value.
type NamedMapClient[K comparable, V any] struct {
	NamedMap[K, V]
	baseClient[K, V]
	namedMapReconnectListener[K, V]
}

func (nm *NamedMapClient[K, V]) getBaseClient() *baseClient[K, V] { //nolint
	return &nm.baseClient
}

// Invoke the specified processor against the entry mapped to the specified key.
// Processors are invoked atomically against a specific entry as the process may mutate the entry.
// The type parameter is R = type of the result of the invocation.
//
// The example below shows how to run an entry processor to increment the age of person identified by the key 1.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	newAge, err := coherence.Invoke[int, Person, int](ctx, namedMap, 1, processors.Increment("age", 1))
//	fmt.Println("New age is", *newAge)
func Invoke[K comparable, V, R any](ctx context.Context, nm NamedMap[K, V], key K, proc processors.Processor) (*R, error) {
	return executeInvoke[K, V, R](ctx, nm.getBaseClient(), key, proc)
}

// InvokeAllFilter invokes the specified function against the entries matching the specified filter.
// Functions are invoked atomically against a specific entry as the function may mutate the entry.
// The type parameter is R = type of the result of the invocation.
//
// The example below shows how to run an entry processor to increment the age of any people older than 1. This function
// returns a stream of [StreamedValue][R] of the values changed.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	age := extractors.Extract[int]("age")
//
//	ch := coherence.InvokeAllFilter[int, Person, int](ctx, namedMap, filters.Greater(age, 1), processors.Increment("age", 1)
//	for se := range ch {
//	    // Check the error
//	    if se.Err != nil {
//	        // process the error
//	        log.Println(se.Err)
//	    } else {
//	        // process the result which will be the person changed
//	        fmt.Println(se.Value)
//	    }
//	}
func InvokeAllFilter[K comparable, V any, R any](ctx context.Context, nm NamedMap[K, V], fltr filters.Filter, proc processors.Processor) <-chan *StreamedValue[R] {
	return executeInvokeAllFilterOrKeys[K, V, R](ctx, nm.getBaseClient(), fltr, []K{}, proc)
}

// InvokeAllKeys invokes the specified function against the entries matching the specified keys.
// Functions are invoked atomically against a specific entry as the function may mutate the entry.
// The type parameter is R = type of the result of the invocation.
//
// The example below shows how to run an entry processor to increment the age of any people with keys 1 and 2. This function
// returns a stream of  [StreamedValue][R] of the values changed.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := coherence.InvokeAllKeys[int, Person, int](ctx, namedMap, []int{1, 2}, processors.Increment("age", 1))
//	for se := range ch {
//	    // Check the error
//	    if se.Err != nil {
//	        // process the error
//	        log.Println(se.Err)
//	    } else {
//	        // process the result which will be the key of the person changed
//	        fmt.Println(se.Value)
//	    }
//	}
func InvokeAllKeys[K comparable, V any, R any](ctx context.Context, nm NamedMap[K, V], keys []K, proc processors.Processor) <-chan *StreamedValue[R] {
	return executeInvokeAllFilterOrKeys[K, V, R](ctx, nm.getBaseClient(), nil, keys, proc)
}

// InvokeAll invokes the specified function against all entries in a [NamedMap].
// Functions are invoked atomically against a specific entry as the function may mutate the entry.
// The type parameter is R = type of the result of the invocation.
//
// The example below shows how to run an entry processor to increment the age of all people. This function
// returns a stream of [StreamedValue][R] of the values changed.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := coherence.InvokeAll[int, Person, int](ctx, namedMap, processors.Increment("age", 1))
//	for se := range ch {
//	    // Check the error
//	    if se.Err != nil {
//	        // process the error
//	        log.Println(se.Err)
//	    } else {
//	        // process the result which will be the key of the person changed
//	        fmt.Println(se.Value)
//	    }
//	}
func InvokeAll[K comparable, V any, R any](ctx context.Context, nm NamedMap[K, V], proc processors.Processor) <-chan *StreamedValue[R] {
	return executeInvokeAllFilterOrKeys[K, V, R](ctx, nm.getBaseClient(), filters.Always(), nil, proc)
}

// AggregateKeys performs an aggregating operation (identified by aggregator) against the
// set of entries selected by the specified keys.
// The type parameter is R = type of the result of the aggregation.
//
// The example below shows how to get the minimum age across the people with keys 3, 4, and 5.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	minAge, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4, 5}, aggregators.Min(extractors.Extract[int]("age")))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Minimum age of people with keys 3, 4 and 5 is", *minAge)
func AggregateKeys[K comparable, V, R any](ctx context.Context, nm NamedMap[K, V], keys []K, aggr aggregators.Aggregator[R]) (*R, error) {
	return executeAggregate[K, V, R](ctx, nm.getBaseClient(), keys, nil, aggr)
}

// AggregateFilter performs an aggregating operation (identified by aggregator) against the
// set of entries selected by the specified filter.
// The type parameter is R = type of the result of the aggregation.
//
// The example below shows how to get the count of people ages older than 19.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	count, err = coherence.AggregateFilter(ctx, namedMap, filters.Greater(extractors.Extract[int]("age"), 19), aggregators.Count())
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Number of people aged greater than 19 is", *count)
func AggregateFilter[K comparable, V, R any](ctx context.Context, nm NamedMap[K, V], filter filters.Filter, aggr aggregators.Aggregator[R]) (*R, error) {
	var noKeys = make([]K, 0)
	return executeAggregate[K, V, R](ctx, nm.getBaseClient(), noKeys, filter, aggr)
}

// Aggregate performs an aggregating operation (identified by aggregator) against all the
// entries in a [NamedMap] or [NamedCache].
// The type parameter is R = type of the result of the aggregation.
//
// The example below shows how to get the average age of all people.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Note: the Average aggregator returns a big.Rat
//	bigRat, err = coherence.Aggregate(ctx, namedMap aggregators.Average(extractors.Extract[int]("age")))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	value, _ := bigRat.Float32()
//	fmt.Printf("Average age of people is %.2f\n", value)
func Aggregate[K comparable, V, R any](ctx context.Context, nm NamedMap[K, V], aggr aggregators.Aggregator[R]) (*R, error) {
	return executeAggregate[K, V, R](ctx, nm.getBaseClient(), make([]K, 0), nil, aggr)
}

// AddIndex adds the index based upon the supplied [extractors.ValueExtractor].
// The type parameters are T = type to extract from and E = type of the extracted value.
//
// The example below shows how to add a sorted index (on age) on the age attribute.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err = coherence.AddIndex(ctx, namedMap, extractors.Extract[int]("age"), true); err != nil {
//	    log.Fatal(err)
//	}
func AddIndex[K comparable, V, T, E any](ctx context.Context, nm NamedMap[K, V], extractor extractors.ValueExtractor[T, E], sorted bool) error {
	return executeAddIndex(ctx, nm.getBaseClient(), extractor, sorted, nil)
}

// AddIndexWithComparator adds the index based upon the supplied [extractors.ValueExtractor] and comparator.
// The type parameters are T = type to extract from and E = type of the extracted value.
//
// The example below shows how to add an index on the age attribute sorted by name.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	err = coherence.AddIndexWithComparator(ctx, namedMap, extractors.Extract[int]("age"), extractors.Extract[int]("name"))
//	if err != nil {
//	    log.Fatal(err)
//	}
func AddIndexWithComparator[K comparable, V, T, E any](ctx context.Context, nm NamedMap[K, V], extractor extractors.ValueExtractor[T, E], comparator extractors.ValueExtractor[T, E]) error {
	return executeAddIndex(ctx, nm.getBaseClient(), extractor, false, comparator)
}

// RemoveIndex removes index based upon the supplied [extractors.ValueExtractor].
// The type parameters are T = type to extract from and E = type of the extracted value.
//
// The example below shows how to remove and index on the age attribute.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err = coherence.RemoveIndex(ctx, namedMap, extractors.Extract[int]("age")); err != nil {
//	    log.Fatal(err)
//	}
func RemoveIndex[K comparable, V, T, E any](ctx context.Context, nm NamedMap[K, V], extractor extractors.ValueExtractor[T, E]) error {
	return executeRemoveIndex(ctx, nm.getBaseClient(), extractor)
}

// AddLifecycleListener adds a [MapLifecycleListener] that will receive events (truncated or released) that occur
// against the [NamedMap].
func (nm *NamedMapClient[K, V]) AddLifecycleListener(listener MapLifecycleListener[K, V]) {
	registerLifecycleListener(nm.getBaseClient(), listener)
}

// AddFilterListener adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the [NamedMap] where entries satisfy the specified [filters.Filter], with the key, and optionally,
// the old-value and new-value included.
func (nm *NamedMapClient[K, V]) AddFilterListener(ctx context.Context, listener MapListener[K, V], filter filters.Filter) error {
	return nm.getBaseClient().eventManager.addFilterListener(ctx, listener, filter, false)
}

// AddFilterListenerLite adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the [NamedMap] where entries satisfy the specified [filters.Filter], with the key,
// the old-value and new-value included.
func (nm *NamedMapClient[K, V]) AddFilterListenerLite(ctx context.Context, listener MapListener[K, V], filter filters.Filter) error {
	return nm.getBaseClient().eventManager.addFilterListener(ctx, listener, filter, true)
}

// AddListener adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the map, with the key, old-value and new-value included.
// This call is equivalent to calling [AddFilterListener] with [filters.Always] as the filter.
func (nm *NamedMapClient[K, V]) AddListener(ctx context.Context, listener MapListener[K, V]) error {
	return nm.getBaseClient().eventManager.addFilterListener(ctx, listener, nil, false)
}

// AddListenerLite adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the map, with the key, and optionally, the old-value and new-value included.
// This call is equivalent to calling [AddFilterListenerLite] with [filters.Always] as the filter.
func (nm *NamedMapClient[K, V]) AddListenerLite(ctx context.Context, listener MapListener[K, V]) error {
	return nm.getBaseClient().eventManager.addFilterListener(ctx, listener, nil, true)
}

// AddKeyListener adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the specified key within the [NamedMap], with the key, old-value and new-value included.
func (nm *NamedMapClient[K, V]) AddKeyListener(ctx context.Context, listener MapListener[K, V], key K) error {
	return nm.getBaseClient().eventManager.addKeyListener(ctx, listener, key, false)
}

// AddKeyListenerLite adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the specified key within the [NamedMap], with the key, and optionally, the old-value and new-value included.
func (nm *NamedMapClient[K, V]) AddKeyListenerLite(ctx context.Context, listener MapListener[K, V], key K) error {
	return nm.getBaseClient().eventManager.addKeyListener(ctx, listener, key, true)
}

// Clear removes all mappings from the [NamedMap]. This operation is observable and will
// trigger any registered events.
func (nm *NamedMapClient[K, V]) Clear(ctx context.Context) error {
	return executeClear[K, V](ctx, &nm.baseClient)
}

// Truncate removes all mappings from the [NamedMap].
// Note: the removal of entries caused by this truncate operation will not be observable.
func (nm *NamedMapClient[K, V]) Truncate(ctx context.Context) error {
	return executeTruncate[K, V](ctx, &nm.baseClient)
}

// Destroy releases and destroys this instance of [NamedMap].
// Warning This method is used to completely destroy the specified
// [NamedMap] across the cluster. All references in the entire cluster to this
// cache will be invalidated, the data will be cleared, and all
// internal resources will be released.
// Note: the removal of entries caused by this operation will not be observable.
func (nm *NamedMapClient[K, V]) Destroy(ctx context.Context) error {
	bc := &nm.baseClient
	s := bc.session

	// protect updates to maps
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	namedMap := convertNamedMapClient[K, V](nm)

	if err := executeDestroy(ctx, bc, namedMap); err != nil {
		return err
	}

	// remove the cache from the session.cache map
	delete(s.maps, nm.Name())

	if nm.namedMapReconnectListener.listener != nil {
		s.RemoveSessionLifecycleListener(nm.namedMapReconnectListener.listener)
	}

	return nil
}

// Release releases the instance of [NamedMap].
// This operation does not affect the contents of the [NamedMap], but only releases the client
// resources. To access the [NamedMap], you must get a new instance.
func (nm *NamedMapClient[K, V]) Release() {
	s := nm.session

	// protect updates to maps
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	if nm.baseClient.nearCacheListener != nil {
		err := nm.RemoveListener(context.Background(), nm.baseClient.nearCacheListener.listener)
		if err != nil {
			log.Printf("unable to remove listener to near cache: %v", err)
		}
	}

	executeRelease[K, V](&nm.baseClient, nm.NamedMap)

	// remove the NamedMap from the session.maps map
	delete(s.maps, nm.Name())

	if nm.namedMapReconnectListener.listener != nil {
		s.RemoveSessionLifecycleListener(nm.namedMapReconnectListener.listener)
	}
}

// ContainsKey returns true if the [NamedMap] contains a mapping for the specified key.
//
// The example below shows how to check if a [NamedMap] contains a mapping for the key 1.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if found, err = namedMap.ContainsKey(ctx, 1); err != nil {
//	   log.Fatal(err)
//	}
func (nm *NamedMapClient[K, V]) ContainsKey(ctx context.Context, key K) (bool, error) {
	return executeContainsKey(ctx, &nm.baseClient, key)
}

// ContainsValue returns true if the [NamedMap] contains a mapping for the specified value.
//
// The example below shows how to check if a [NamedMap] contains a mapping for the person.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	person := Person{ID: 1, Name: "Tim"}
//
//	if found, err = namedMap.ContainsValue(ctx, person); err != nil {
//	   log.Fatal(err)
//	}
func (nm *NamedMapClient[K, V]) ContainsValue(ctx context.Context, value V) (bool, error) {
	return executeContainsValue(ctx, &nm.baseClient, value)
}

// ContainsEntry returns true if the [NamedMap] contains a mapping for the specified key and value.
//
// The example below shows how to check if a [NamedMap] contains a mapping for the key 1 and person.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	person := Person{ID: 1, Name: "Tim"}
//
//	if found, err = namedMap.ContainsEntry(ctx, person.ID, person); err != nil {
//	   log.Fatal(err)
//	}
func (nm *NamedMapClient[K, V]) ContainsEntry(ctx context.Context, key K, value V) (bool, error) {
	return executeContainsEntry(ctx, &nm.baseClient, key, value)
}

// IsEmpty returns true if the [NamedMap] contains no mappings.
func (nm *NamedMapClient[K, V]) IsEmpty(ctx context.Context) (bool, error) {
	return executeIsEmpty(ctx, &nm.baseClient)
}

// EntrySetFilter returns a channel from which entries satisfying the specified filter can be obtained.
// Each entry in the channel is of type [*StreamedEntry] which wraps an error and the result.
// As always, the result must be accessed (and will be valid) only if the error is nil.
//
// The example below shows how to iterate the entries in a [NamedMap] where the age > 20.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedMap.EntrySetFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key, "Value:", result.Value)
//	    }
//	}
func (nm *NamedMapClient[K, V]) EntrySetFilter(ctx context.Context, fltr filters.Filter) <-chan *StreamedEntry[K, V] {
	return executeEntrySetFilter[K, V](ctx, &nm.baseClient, fltr)
}

// EntrySet returns a channel from which  all entries can be obtained.
//
// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
// careful when running this operation against [NamedMap]s with large number of entries.
//
// The example below shows how to iterate the entries in a [NamedMap].
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedMap.EntrySet(ctx)
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key, "Value:", result.Value)
//	    }
//	}
func (nm *NamedMapClient[K, V]) EntrySet(ctx context.Context) <-chan *StreamedEntry[K, V] {
	return executeEntrySet[K, V](ctx, &nm.baseClient)
}

// Get returns the value to which the specified key is mapped. V will be nil
// if the [NamedMap] contains no mapping for the key.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	person, err = namedMap.Get(1)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if person != nil {
//	    fmt.Println("Person is", *value)
//	} else {
//	    fmt.Println("No person found")
//	}
func (nm *NamedMapClient[K, V]) Get(ctx context.Context, key K) (*V, error) {
	return executeGet(ctx, &nm.baseClient, key)
}

// GetAll returns a channel from which entries satisfying the specified filter can be obtained.
// Each entry in the channel is of type [*StreamedEntry] which wraps an error and the result.
// As always, the result must be accessed (and will be valid) only if the error is nil.
//
// The example below shows how to get all the entries for keys 1, 3 and 4.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedMap.GetAll(ctx, []int{1, 3, 4})
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key, "Value:", result.Value)
//	    }
//	}
func (nm *NamedMapClient[K, V]) GetAll(ctx context.Context, keys []K) <-chan *StreamedEntry[K, V] {
	return executeGetAll[K, V](ctx, &nm.baseClient, keys)
}

// GetOrDefault will return the value mapped to the specified key,
// or if there is no mapping, it will return the specified default.
func (nm *NamedMapClient[K, V]) GetOrDefault(ctx context.Context, key K, def V) (*V, error) {
	return executeGetOrDefault(ctx, &nm.baseClient, key, def)
}

// KeySetFilter returns a channel from which keys of the entries that satisfy the filter can be obtained.
// Each entry in the channel is of type *StreamEntry which wraps an error and the key.
// As always, the result must be accessed (and will be valid) only if the error is nil.
//
// The example below shows how to iterate the keys in a [NamedMap] where the age > 20.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedMap.KeySetFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key)
//	    }
//	}
func (nm *NamedMapClient[K, V]) KeySetFilter(ctx context.Context, fltr filters.Filter) <-chan *StreamedKey[K] {
	return executeKeySetFilter(ctx, &nm.baseClient, fltr)
}

// KeySet returns a channel from which keys of all entries can be obtained.
//
// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
// careful when running this operation against [NamedMap]s with large number of entries.
//
// The example below shows how to iterate the keys in a [NamedMap].
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedMap.KeySet(ctx)
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key)
//	    }
//	}
func (nm *NamedMapClient[K, V]) KeySet(ctx context.Context) <-chan *StreamedKey[K] {
	return executeKeySet[K, V](ctx, &nm.baseClient)
}

// Name returns the name of the NamedMap.
func (nm *NamedMapClient[K, V]) Name() string {
	return nm.name
}

// PutAll copies all the mappings from the specified map to the [NamedMap].
// This is the most efficient way to add multiple entries into a [NamedMap] as it
// is carried out in parallel and no previous values are returned.
//
//	var peopleData = map[int]Person{
//	    1: {ID: 1, Name: "Tim", Age: 21},
//	    2: {ID: 2, Name: "Andrew", Age: 44},
//	    3: {ID: 3, Name: "Helen", Age: 20},
//	    4: {ID: 4, Name: "Alexa", Age: 12},
//	}
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err = namedMap.PutAll(ctx, peopleData); err != nil {
//	    log.Fatal(err)
//	}
func (nm *NamedMapClient[K, V]) PutAll(ctx context.Context, entries map[K]V) error {
	return executePutAll(ctx, &nm.baseClient, entries)
}

// PutIfAbsent adds the specified mapping if the key is not already associated with a value in the [NamedMap]
// and returns nil, else returns the current value.
func (nm *NamedMapClient[K, V]) PutIfAbsent(ctx context.Context, key K, value V) (*V, error) {
	return executePutIfAbsent(ctx, &nm.baseClient, key, value)
}

// Put associates the specified value with the specified key returning the previously
// mapped value, if any. V will be nil if there was no previous value.
func (nm *NamedMapClient[K, V]) Put(ctx context.Context, key K, value V) (*V, error) {
	return executePutWithExpiry(ctx, &nm.baseClient, key, value, time.Duration(0))
}

// Remove removes the mapping for a key from the [NamedMap] if it is present and
// returns the previous value or nil if there wasn't one.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	oldValue, err = namedMap.Remove(ctx, 1)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if oldValue == nil {
//	    fmt.Println("No previous person was found")
//	} else {
//	    fmt.Println("Previous person was", *oldValue)
//	}
func (nm *NamedMapClient[K, V]) Remove(ctx context.Context, key K) (*V, error) {
	return executeRemove(ctx, &nm.baseClient, key)
}

// RemoveLifecycleListener removes the lifecycle listener that was previously registered to receive events.
func (nm *NamedMapClient[K, V]) RemoveLifecycleListener(listener MapLifecycleListener[K, V]) {
	unregisterLifecycleListener[K, V](nm.getBaseClient(), listener)
}

// RemoveFilterListener removes the listener that was previously registered to receive events
// where entries satisfy the specified filters.Filter.
func (nm *NamedMapClient[K, V]) RemoveFilterListener(ctx context.Context, listener MapListener[K, V], f filters.Filter) error {
	return nm.getBaseClient().eventManager.removeFilterListener(ctx, listener, f)
}

// RemoveKeyListener removes the listener that was previously registered to receive events against the specified key.
func (nm *NamedMapClient[K, V]) RemoveKeyListener(ctx context.Context, listener MapListener[K, V], key K) error {
	return nm.getBaseClient().eventManager.removeKeyListener(ctx, listener, key)
}

// RemoveListener removes the listener that was previously registered to receive events.
func (nm *NamedMapClient[K, V]) RemoveListener(ctx context.Context, listener MapListener[K, V]) error {
	return nm.RemoveFilterListener(ctx, listener, nil)
}

// RemoveMapping removes the entry for the specified key only if it is currently
// mapped to the specified value.
func (nm *NamedMapClient[K, V]) RemoveMapping(ctx context.Context, key K, value V) (bool, error) {
	return executeRemoveMapping(ctx, &nm.baseClient, key, value)
}

// Replace replaces the entry for the specified key only if it is
// currently mapped to some value.
func (nm *NamedMapClient[K, V]) Replace(ctx context.Context, key K, value V) (*V, error) {
	return executeReplace(ctx, &nm.baseClient, key, value)
}

// ReplaceMapping replaces the entry for the specified key only if it is
// currently mapped to some value. Returns true if the value was replaced.
func (nm *NamedMapClient[K, V]) ReplaceMapping(ctx context.Context, key K, prevValue V, newValue V) (bool, error) {
	return executeReplaceMapping(ctx, &nm.baseClient, key, prevValue, newValue)
}

// GetSession returns the session.
func (nm *NamedMapClient[K, V]) GetSession() *Session {
	return nm.session
}

// Size returns the number of mappings contained within the [NamedMap].
func (nm *NamedMapClient[K, V]) Size(ctx context.Context) (int, error) {
	return executeSize(ctx, &nm.baseClient)
}

// ValuesFilter returns a view of filtered values contained in the [NamedMap].
// The returned channel will be asynchronously filled with values in the
// [NamedMap] that satisfy the filter.
//
// The example below shows how to iterate the values in a [NamedMap] where the age > 20.
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedMap.ValuesFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Value:", result.Value)
//	    }
//	}
func (nm *NamedMapClient[K, V]) ValuesFilter(ctx context.Context, fltr filters.Filter) <-chan *StreamedValue[V] {
	return executeValues(ctx, &nm.baseClient, fltr)
}

// Values returns a view of all values contained in the [NamedMap].
//
// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
// careful when running this operation against [NamedMap]s with large number of entries.
//
// The example below shows how to iterate the values in a [NamedMap].
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedMap.Values(ctx)
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Value:", result.Value)
//	    }
//	}
func (nm *NamedMapClient[K, V]) Values(ctx context.Context) <-chan *StreamedValue[V] {
	return executeValuesNoFilter[K, V](ctx, &nm.baseClient)
}

// IsReady returns whether this [NamedMap] is ready to be used.
// An example of when this method would return false would
// be where a partitioned cache service that owns this cache has no
// storage-enabled members.
// If it is not supported by the gRPC proxy, an error will be returned.
func (nm *NamedMapClient[K, V]) IsReady(ctx context.Context) (bool, error) {
	return executeIsReady[K, V](ctx, &nm.baseClient)
}

// GetNearCacheStats returns the [CacheStats] for a near cache for a [NamedMap].
// If no near cache is defined, nil is returned.
func (nm *NamedMapClient[K, V]) GetNearCacheStats() CacheStats {
	return nm.getBaseClient().nearCache
}

// String returns a string representation of a NamedMapClient.
func (nm *NamedMapClient[K, V]) String() string {
	return fmt.Sprintf("NamedMap{name=%s, format=%s, destroyed=%v, released=%v, options=%v}",
		nm.Name(), nm.format, nm.destroyed, nm.released, nm.cacheOpts.NearCacheOptions)
}

// getNamedMap gets a [NamedMap] of the generic type specified or if a cache already exists with the
// same type parameters, it will return it otherwise it will create a new one.
func getNamedMap[K comparable, V any](session *Session, name string, sOpts *SessionOptions, options ...func(cache *CacheOptions)) (*NamedMapClient[K, V], error) {
	var (
		format        = sOpts.Format
		existingCache interface{}
		ok            bool
	)

	if session.closed {
		return nil, ErrClosed
	}

	// protect updates to maps
	session.mapMutex.Lock()
	defer session.mapMutex.Unlock()

	cacheOptions := &CacheOptions{
		DefaultExpiry: time.Duration(0),
	}

	// apply any cache options
	for _, f := range options {
		f(cacheOptions)
	}

	err := validateNearCacheOptions(cacheOptions.NearCacheOptions)
	if err != nil {
		return nil, err
	}

	if cacheOptions.DefaultExpiry != time.Duration(0) {
		return nil, errors.New("you cannot use a non-zero expiry for a NamedMap")
	}

	// check to see if we already have an entry for the map
	if existingCache, ok = session.maps[name]; ok {
		existing, ok2 := existingCache.(*NamedMapClient[K, V])
		if !ok2 {
			// the casting failed so return an error indicating the NamedMap exists with different type mappings
			return nil, getExistingError("NamedMap", name)
		}

		// check if there is a difference in cache options wrt near cache.
		// e.g. user has asked for a cache with near cache and previously
		// cache does not have this, or visa-versa
		if existing.baseClient.nearCache == nil && cacheOptions.NearCacheOptions != nil ||
			existing.baseClient.nearCache != nil && cacheOptions.NearCacheOptions == nil {
			return nil, getExistingNearCacheError("NamedMap", name)
		}

		session.debug("using existing NamedMap", existing)
		return existing, nil
	}

	newMap := &NamedMapClient[K, V]{
		baseClient: newBaseClient[K, V](session, name, format, sOpts, cacheOptions),
	}
	if err := newMap.baseClient.ensureClientConnection(); err != nil {
		return nil, err
	}

	namedMap := convertNamedMapClient[K, V](newMap)

	manager, err := newMapEventManager(&namedMap, newMap.baseClient, session)
	if err != nil {
		return nil, err
	}
	newMap.baseClient.eventManager = manager

	// store the new cache
	session.maps[name] = newMap

	listener := newNamedMapReconnectListener[K, V](*newMap)
	newMap.namedMapReconnectListener = *listener

	session.AddSessionLifecycleListener(newMap.namedMapReconnectListener.listener)

	// if near cache then add listener
	if newMap.baseClient.nearCache != nil {
		nearCacheListener := newNearNamedMapMapLister[K, V](*newMap, newMap.baseClient.nearCache)
		newMap.baseClient.nearCacheListener = nearCacheListener
		err = newMap.AddListener(context.Background(), newMap.baseClient.nearCacheListener.listener)
		if err != nil {
			return nil, fmt.Errorf("unable to add listener to near cache: %v", err)
		}
	}

	session.debug("newNamedMap", newMap, "session:", session)
	return newMap, nil
}

// reconnectSessionListener is a session listener to be called on reconnect for a specific [NamedMap].
type namedMapReconnectListener[K comparable, V any] struct {
	listener SessionLifecycleListener
}

// newReconnectSessionListener creates new namedMapReconnectListener.
func newNamedMapReconnectListener[K comparable, V any](nm NamedMapClient[K, V]) *namedMapReconnectListener[K, V] {
	listener := namedMapReconnectListener[K, V]{
		listener: NewSessionLifecycleListener(),
	}

	listener.listener.OnReconnected(func(e SessionLifecycleEvent) {
		// re-register listeners for the NamedMap
		namedMap := convertNamedMapClient[K, V](&nm)
		if err := reRegisterListeners[K, V](context.Background(), &namedMap, &nm.baseClient); err != nil {
			log.Println(err)
		}
	})

	return &listener
}

func newBaseClient[K comparable, V any](session *Session, name string, format string, sOpts *SessionOptions, cOpts *CacheOptions) baseClient[K, V] {
	bc := baseClient[K, V]{
		session:         session,
		name:            name,
		sessionOpts:     sOpts,
		format:          format,
		keySerializer:   NewSerializer[K](format),
		valueSerializer: NewSerializer[V](format),
		mutex:           &sync.RWMutex{},
		cacheOpts:       cOpts,
	}

	// if near cache options specified then setup internal local cache
	if bc.cacheOpts.NearCacheOptions != nil {
		var options = make([]func(localCache *LocalCacheOptions), 0)
		if bc.cacheOpts.NearCacheOptions.TTL != 0 {
			options = append(options, WithLocalCacheExpiry(bc.cacheOpts.NearCacheOptions.TTL))
		}
		if bc.cacheOpts.NearCacheOptions.HighUnits != 0 {
			options = append(options, WithLocalCacheExpiry(bc.cacheOpts.NearCacheOptions.TTL))
		}
		nearCache := newLocalCache[K, V](bc.name, options...)
		bc.nearCache = nearCache
	}

	return bc
}

func newNearNamedMapMapLister[K comparable, V any](nc NamedMapClient[K, V], cache *localCache[K, V]) *namedCacheNearCacheListener[K, V] {
	listener := namedCacheNearCacheListener[K, V]{
		listener:  NewMapListener[K, V](),
		nearCache: cache,
	}

	listener.listener.OnAny(func(e MapEvent[K, V]) {
		err := processNearCacheEvent(nc.baseClient.nearCache, e)
		if err != nil {
			log.Println("Error processing near cache MapEvent", e)
		}
	})

	return &listener
}

// getExistingError returns an error indicating a [NamedMap] or [NamedCache] exists with different type parameters.
func getExistingError(cacheType, name string) error {
	return fmt.Errorf(mapOrCacheExists, cacheType, name)
}

// getExistingError returns an error indicating a [NamedMap] or [NamedCache] exists with different near cache options.
func getExistingNearCacheError(cacheType, name string) error {
	return fmt.Errorf(mapOrCacheExistsNearCache, cacheType, name)
}

func convertNamedMapClient[K comparable, V any](client *NamedMapClient[K, V]) NamedMap[K, V] {
	return client
}
