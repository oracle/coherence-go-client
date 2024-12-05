/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"time"
)

var _ NamedCache[string, string] = &NamedCacheClient[string, string]{}

// NamedCacheClient is the implementation of the [NamedCache] interface.
// The type parameters are K = type of the key and V= type of the value.
type NamedCacheClient[K comparable, V any] struct {
	NamedCache[K, V]
	*baseClient[K, V]
	namedCacheReconnectListener[K, V]
}

func (nc *NamedCacheClient[K, V]) getBaseClient() *baseClient[K, V] { // nolint
	return nc.baseClient
}

// GetCacheName returns the cache name of the [NamedCache].
func (nc *NamedCacheClient[K, V]) GetCacheName() string {
	return nc.name
}

// AddLifecycleListener Adds a [MapLifecycleListener] that will receive events (truncated or released) that occur
// against the [NamedCache].
func (nc *NamedCacheClient[K, V]) AddLifecycleListener(listener MapLifecycleListener[K, V]) {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		nc.getBaseClient().addLifecycleListener(listener)
	} else {
		registerLifecycleListener(nc.getBaseClient(), listener)
	}
}

// AddFilterListener Adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the map where entries satisfy the specified [filters.Filter], with the key, and optionally,
// the old-value and new-value included.
func (nc *NamedCacheClient[K, V]) AddFilterListener(ctx context.Context, listener MapListener[K, V], filter filters.Filter) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return addFilterListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, filter, false)
	}
	return nc.getBaseClient().eventManager.addFilterListener(ctx, listener, filter, false)
}

// AddFilterListenerLite Adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the map where entries satisfy the specified [filters.Filter], with the key,
// the old-value and new-value included.
func (nc *NamedCacheClient[K, V]) AddFilterListenerLite(ctx context.Context, listener MapListener[K, V], filter filters.Filter) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return addFilterListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, filter, true)
	}
	return nc.getBaseClient().eventManager.addFilterListener(ctx, listener, filter, true)
}

// AddListener Adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the map, with the key, old-value and new-value included.
// This call is equivalent to calling AddFilterListener with filters.Always as the filter.
func (nc *NamedCacheClient[K, V]) AddListener(ctx context.Context, listener MapListener[K, V]) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return addFilterListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, nil, false)
	}
	return nc.getBaseClient().eventManager.addFilterListener(ctx, listener, nil, false)
}

// AddListenerLite Adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the map, with the key, and optionally, the old-value and new-value included.
// This call is equivalent to calling [AddFilterListenerLite] with [filters.Always] as the filter.
func (nc *NamedCacheClient[K, V]) AddListenerLite(ctx context.Context, listener MapListener[K, V]) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return addFilterListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, nil, true)
	}
	return nc.getBaseClient().eventManager.addFilterListener(ctx, listener, nil, true)
}

// AddKeyListener Adds a [MapListener] that will receive events (inserts, updates, deletes) that occur
// against the specified key within the map, with the key, old-value and new-value included.
func (nc *NamedCacheClient[K, V]) AddKeyListener(ctx context.Context, listener MapListener[K, V], key K) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return addKeyListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, key, false)
	}

	return nc.getBaseClient().eventManager.addKeyListener(ctx, listener, key, false)
}

// AddKeyListenerLite Adds a ]MapListener] that will receive events (inserts, updates, deletes) that occur
// against the specified key within the map, with the key, and optionally, the old-value and new-value included.
func (nc *NamedCacheClient[K, V]) AddKeyListenerLite(ctx context.Context, listener MapListener[K, V], key K) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return addKeyListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, key, true)
	}

	return nc.getBaseClient().eventManager.addKeyListener(ctx, listener, key, true)
}

// Clear removes all mappings from this [NamedCache]. This operation is observable and will
// trigger any registered events.
func (nc *NamedCacheClient[K, V]) Clear(ctx context.Context) error {
	return executeClear[K, V](ctx, nc.baseClient)
}

// Truncate removes all mappings from this [NamedCache].
// Note: the removal of entries caused by this truncate operation will not be observable.
func (nc *NamedCacheClient[K, V]) Truncate(ctx context.Context) error {
	return executeTruncate[K, V](ctx, nc.baseClient)
}

// Destroy releases and destroys this instance of [NamedCache].
// Warning This method is used to completely destroy the specified
// [NamedCache] across the cluster. All references in the entire cluster to this
// cache will be invalidated, the data will be cleared, and all
// internal resources will be released.
// Note: the removal of entries caused by this operation will not be observable.
func (nc *NamedCacheClient[K, V]) Destroy(ctx context.Context) error {
	bc := nc.baseClient
	s := bc.session

	// protect updates to maps
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	nameMap := convertNamedCacheClient[K, V](nc)

	if err := executeDestroy(ctx, bc, nameMap); err != nil {
		return err
	}

	// remove the cache from the session.cache map
	delete(s.caches, nc.Name())

	if nc.namedCacheReconnectListener.listener != nil {
		s.RemoveSessionLifecycleListener(nc.namedCacheReconnectListener.listener)
	}

	return nil
}

// Release releases the instance of [NamedCache].
// This operation does not affect the contents of the [NamedCache], but only releases the client
// resources. To access the [NamedCache], you must get a new instance.
func (nc *NamedCacheClient[K, V]) Release() {
	s := nc.session

	// protect updates to maps
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	// remove near cache Map Listener
	if nc.baseClient.nearCacheListener != nil {
		err := nc.RemoveListener(context.Background(), nc.baseClient.nearCacheListener.listener)
		if err != nil {
			logMessage(WARNING, "unable to remove listener from near cache: %v", err)
		}
	}

	// remove near cache Lifecycle Listener
	if nc.baseClient.nearCacheLifecycleListener != nil {
		if nc.getBaseClient().getProtocolVersion() > 0 {
			nc.getBaseClient().removeLifecycleListener(nc.baseClient.nearCacheLifecycleListener.listener)
		} else {
			nc.RemoveLifecycleListener(nc.baseClient.nearCacheLifecycleListener.listener)
		}
	}

	executeRelease[K, V](nc.baseClient, nc.NamedCache)

	// remove the NamedCache from the session.cache map
	delete(s.caches, nc.Name())

	// remove the cacheID mapping
	if s.GetProtocolVersion() > 0 {
		s.cacheIDMap.Remove(nc.name)
	}

	if nc.namedCacheReconnectListener.listener != nil {
		s.RemoveSessionLifecycleListener(nc.namedCacheReconnectListener.listener)
	}
}

// ContainsKey returns true if this [NamedCache] contains a mapping for the specified key.
//
// The example below shows how to check if a [NamedCache] contains a mapping for the key 1.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if found, err = namedCache.ContainsKey(ctx, 1); err != nil {
//	   log.Fatal(err)
//	}
func (nc *NamedCacheClient[K, V]) ContainsKey(ctx context.Context, key K) (bool, error) {
	return executeContainsKey(ctx, nc.baseClient, key)
}

// ContainsValue returns true if this [NamedCache] contains a mapping for the specified value.
//
// The example below shows how to check if a [NamedCache] contains a mapping for the person.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	person := Person{ID: 1, Name: "Tim"}
//
//	if found, err = namedCache.ContainsValue(ctx, person); err != nil {
//	   log.Fatal(err)
//	}
func (nc *NamedCacheClient[K, V]) ContainsValue(ctx context.Context, value V) (bool, error) {
	return executeContainsValue(ctx, nc.baseClient, value)
}

// ContainsEntry returns true if this [NamedCache] contains a mapping for the specified key and value.
//
// The example below shows how to check if a [NamedCache] contains a mapping for the key 1 and person.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	person := Person{ID: 1, Name: "Tim"}
//
//	if found, err = namedCache.ContainsEntry(ctx, person.ID, person); err != nil {
//	   log.Fatal(err)
//	}
func (nc *NamedCacheClient[K, V]) ContainsEntry(ctx context.Context, key K, value V) (bool, error) {
	return executeContainsEntry(ctx, nc.baseClient, key, value)
}

// IsEmpty returns true if this [NamedCache] contains no mappings.
func (nc *NamedCacheClient[K, V]) IsEmpty(ctx context.Context) (bool, error) {
	return executeIsEmpty(ctx, nc.baseClient)
}

// EntrySetFilter returns a channel from which entries satisfying the specified filter can be obtained.
// Each entry in the channel is of type *StreamEntry which wraps an error and the result.
// As always, the result must be accessed (and will be valid) only if the error is nil.
//
// The example below shows how to iterate the entries in a [NamedCache] where the age > 20.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedCache.EntrySetFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key, "Value:", result.Value)
//	    }
//	}
func (nc *NamedCacheClient[K, V]) EntrySetFilter(ctx context.Context, fltr filters.Filter) <-chan *StreamedEntry[K, V] {
	return executeEntrySetFilter(ctx, nc.baseClient, fltr)
}

// EntrySet returns a channel from which all entries can be obtained.
//
// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
// careful when running this operation against NamedCaches with large number of entries.
//
// The example below shows how to iterate the entries in a [NamedCache].
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedCache.EntrySet(ctx)
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key, "Value:", result.Value)
//	    }
//	}
func (nc *NamedCacheClient[K, V]) EntrySet(ctx context.Context) <-chan *StreamedEntry[K, V] {
	return executeEntrySet[K, V](ctx, nc.baseClient)
}

// Get returns the value to which the specified key is mapped. V will be nil
// if this [NamedCache] contains no mapping for the key.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	person, err = namedCache.Get(1)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if person != nil {
//	    fmt.Println("Person is", *value)
//	} else {
//	    fmt.Println("No person found")
//	}
func (nc *NamedCacheClient[K, V]) Get(ctx context.Context, key K) (*V, error) {
	return executeGet(ctx, nc.baseClient, key)
}

// GetAll returns a channel from which entries satisfying the specified filter can be obtained.
// Each entry in the channel is of type [*StreamedEntry] which wraps an error and the result.
// As always, the result must be accessed (and will be valid) only if the error is nil.
//
// The example below shows how to get all the entries for keys 1, 3 and 4.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedCache.GetAll(ctx, []int{1, 3, 4})
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key, "Value:", result.Value)
//	    }
//	}
func (nc *NamedCacheClient[K, V]) GetAll(ctx context.Context, keys []K) <-chan *StreamedEntry[K, V] {
	return executeGetAll[K, V](ctx, nc.baseClient, keys)
}

// GetOrDefault will return the value mapped to the specified key,
// or if there is no mapping, it will return the specified default.
func (nc *NamedCacheClient[K, V]) GetOrDefault(ctx context.Context, key K, def V) (*V, error) {
	return executeGetOrDefault(ctx, nc.baseClient, key, def)
}

// KeySetFilter returns a channel from which keys of the entries that satisfy the filter can be obtained.
// Each entry in the channel is of type *StreamEntry which wraps an error and the key.
// As always, the result must be accessed (and will be valid) only if the error is nil.
//
// The example below shows how to iterate the keys in a [NamedCache] where the age > 20.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session,"people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedCache.KeySetFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key)
//	    }
//	}
func (nc *NamedCacheClient[K, V]) KeySetFilter(ctx context.Context, fltr filters.Filter) <-chan *StreamedKey[K] {
	return executeKeySetFilter(ctx, nc.baseClient, fltr)
}

// KeySet returns a channel from which keys of all entries can be obtained.
//
// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
// careful when running this operation against [NamedCache]s with large number of entries.
//
// The example below shows how to iterate the keys in a [NamedCache].
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedCache.KeySet(ctx)
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Key:", result.Key)
//	    }
//	}
func (nc *NamedCacheClient[K, V]) KeySet(ctx context.Context) <-chan *StreamedKey[K] {
	return executeKeySet[K, V](ctx, nc.baseClient)
}

// Name returns the name of the [NamedCache]].
func (nc *NamedCacheClient[K, V]) Name() string {
	return nc.name
}

// PutAll copies all the mappings from the specified map to this [NamedCache].
// This is the most efficient way to add multiple entries into a [NamedCache] as it
// is carried out in parallel and no previous values are returned.
//
//	var peopleData = map[int]Person{
//	    1: {ID: 1, Name: "Tim", Age: 21},
//	    2: {ID: 2, Name: "Andrew", Age: 44},
//	    3: {ID: 3, Name: "Helen", Age: 20},
//	    4: {ID: 4, Name: "Alexa", Age: 12},
//	}
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err = namedCache.PutAll(ctx, peopleData); err != nil {
//	    log.Fatal(err)
//	}
func (nc *NamedCacheClient[K, V]) PutAll(ctx context.Context, entries map[K]V) error {
	return executePutAll(ctx, nc.baseClient, entries, 0)
}

// PutAllWithExpiry copies all the mappings from the specified map to this [NamedCache] and sets the ttl for each entry.
// This is the most efficient way to add multiple entries into a [NamedCache] as it
// is carried out in parallel and no previous values are returned.
//
//	var peopleData = map[int]Person{
//	    1: {ID: 1, Name: "Tim", Age: 21},
//	    2: {ID: 2, Name: "Andrew", Age: 44},
//	    3: {ID: 3, Name: "Helen", Age: 20},
//	    4: {ID: 4, Name: "Alexa", Age: 12},
//	}
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err = namedCache.PutAll(ctx, peopleData, time.Duration(5) * time.Second); err != nil {
//	    log.Fatal(err)
//	}
//
// Note: If PutAllWithExpiry is not supported by the Coherence server, an error will be thrown
func (nc *NamedCacheClient[K, V]) PutAllWithExpiry(ctx context.Context, entries map[K]V, ttl time.Duration) error {
	return executePutAll(ctx, nc.baseClient, entries, ttl)
}

// PutIfAbsent adds the specified mapping if the key is not already associated with a value in the [NamedCache]
// and returns nil, else returns the current value.
func (nc *NamedCacheClient[K, V]) PutIfAbsent(ctx context.Context, key K, value V) (*V, error) {
	return executePutIfAbsent(ctx, nc.baseClient, key, value)
}

// Put associates the specified value with the specified key returning the previously
// mapped value, if any. V will be nil if there was no previous value.
func (nc *NamedCacheClient[K, V]) Put(ctx context.Context, key K, value V) (*V, error) {
	return executePutWithExpiry(ctx, nc.baseClient, key, value, nc.baseClient.cacheOpts.DefaultExpiry)
}

// PutWithExpiry associates the specified value with the specified key. If the [NamedCache]
// previously contained a value for this key, the old value is replaced.
// This variation of the Put(ctx context.Context, key K, value V)
// function allows the caller to specify an expiry (or "time to live")
// for the cache entry.  If coherence.ExpiryNever < ttl < 1 millisecond,
// ttl is set to 1 millisecond. V will be nil if there was no previous value.
func (nc *NamedCacheClient[K, V]) PutWithExpiry(ctx context.Context, key K, value V, ttl time.Duration) (*V, error) {
	return executePutWithExpiry(ctx, nc.baseClient, key, value, ttl)
}

// Remove removes the mapping for a key from this [NamedCache] if it is present and
// returns the previous value or nil if there wasn't one.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	oldValue, err = namedCache.Remove(ctx, 1)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if oldValue == nil {
//	    fmt.Println("No previous person was found")
//	} else {
//	    fmt.Println("Previous person was", *oldValue)
//	}
func (nc *NamedCacheClient[K, V]) Remove(ctx context.Context, key K) (*V, error) {
	return executeRemove(ctx, nc.baseClient, key)
}

// RemoveLifecycleListener removes the lifecycle listener that was previously registered to receive events.
func (nc *NamedCacheClient[K, V]) RemoveLifecycleListener(listener MapLifecycleListener[K, V]) {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		nc.getBaseClient().removeLifecycleListener(listener)
	} else {
		unregisterLifecycleListener[K, V](nc.getBaseClient(), listener)
	}
}

// RemoveFilterListener removes the listener that was previously registered to receive events
// where entries satisfy the specified [filters.Filter].
func (nc *NamedCacheClient[K, V]) RemoveFilterListener(ctx context.Context, listener MapListener[K, V], f filters.Filter) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return removeFilterListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, f)
	}
	return nc.getBaseClient().eventManager.removeFilterListener(ctx, listener, f)
}

// RemoveKeyListener removes the listener that was previously registered to receive events against the specified key.
func (nc *NamedCacheClient[K, V]) RemoveKeyListener(ctx context.Context, listener MapListener[K, V], key K) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return removeKeyListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, key)
	}
	return nc.getBaseClient().eventManager.removeKeyListener(ctx, listener, key)
}

// RemoveListener removes the listener that was previously registered to receive events.
func (nc *NamedCacheClient[K, V]) RemoveListener(ctx context.Context, listener MapListener[K, V]) error {
	if nc.getBaseClient().getProtocolVersion() > 0 {
		return removeFilterListenerInternalV1[K, V](ctx, nc.getBaseClient(), listener, nil)
	}
	return nc.RemoveFilterListener(ctx, listener, nil)
}

// RemoveMapping removes the entry for the specified key only if it is currently
// mapped to the specified value. Returns true if the entry was removed.
func (nc *NamedCacheClient[K, V]) RemoveMapping(ctx context.Context, key K, value V) (bool, error) {
	return executeRemoveMapping(ctx, nc.baseClient, key, value)
}

// Replace replaces the entry for the specified key only if it is
// currently mapped to some value.
func (nc *NamedCacheClient[K, V]) Replace(ctx context.Context, key K, value V) (*V, error) {
	return executeReplace(ctx, nc.baseClient, key, value)
}

// ReplaceMapping replaces the entry for the specified key only if it is
// currently mapped to some value. Returns true if the value was replaced.
func (nc *NamedCacheClient[K, V]) ReplaceMapping(ctx context.Context, key K, prevValue V, newValue V) (bool, error) {
	return executeReplaceMapping(ctx, nc.baseClient, key, prevValue, newValue)
}

// GetSession returns the session.
func (nc *NamedCacheClient[K, V]) GetSession() *Session {
	return nc.session
}

// Size returns the number of mappings contained within this [NamedCache].
func (nc *NamedCacheClient[K, V]) Size(ctx context.Context) (int, error) {
	return executeSize(ctx, nc.baseClient)
}

// ValuesFilter returns a view of filtered values contained in this [NamedCache].
// The returned channel will be asynchronously filled with values in the
// [NamedCache] that satisfy the filter.
//
// The example below shows how to iterate the values in a [NamedCache] where the age > 20.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedCache.ValuesFilter(ctx, filters.GreaterEqual(extractors.Extract[int]("age"), 20))
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Value:", result.Value)
//	    }
//	}
func (nc *NamedCacheClient[K, V]) ValuesFilter(ctx context.Context, fltr filters.Filter) <-chan *StreamedValue[V] {
	return executeValues(ctx, nc.baseClient, fltr)
}

// Values returns a view of all values contained in this [NamedCache].
//
// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
// careful when running this operation against NamedCaches with large number of entries.
//
// The example below shows how to iterate the values in a [NamedCache].
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ch := namedCache.Values(ctx)
//	for result := range ch {
//	    if result.Err != nil {
//	        // process, handle the error
//	    } else {
//	        fmt.Println("Value:", result.Value)
//	    }
//	}
func (nc *NamedCacheClient[K, V]) Values(ctx context.Context) <-chan *StreamedValue[V] {
	return executeValuesNoFilter[K, V](ctx, nc.baseClient)
}

// IsReady returns whether this [NamedCache] is ready to be used.
// An example of when this method would return false would
// be where a partitioned cache service that owns this cache has no
// storage-enabled members.
// If it is not supported by the gRPC proxy, an error will be returned.
func (nc *NamedCacheClient[K, V]) IsReady(ctx context.Context) (bool, error) {
	return executeIsReady[K, V](ctx, nc.baseClient)
}

// GetNearCacheStats returns the [CacheStats] for a near cache for a [NamedMap].
// If no near cache is defined, nil is returned.
func (nc *NamedCacheClient[K, V]) GetNearCacheStats() CacheStats {
	return nc.getBaseClient().nearCache
}

// String returns a string representation of a [NamedCacheClient].
func (nc *NamedCacheClient[K, V]) String() string {
	return fmt.Sprintf("NamedCache{name=%s, format=%s, options=%v}",
		nc.Name(), nc.format, nc.cacheOpts.NearCacheOptions)
}

// getNamedCache gets a [NamedCache] of the generic type specified or if a cache already exists with the
// same type parameters, it will return it otherwise it will create a new one.
func getNamedCache[K comparable, V any](session *Session, name string, sOpts *SessionOptions, options ...func(cache *CacheOptions)) (*NamedCacheClient[K, V], error) {
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

	err := ensureNearCacheOptions(cacheOptions.NearCacheOptions)
	if err != nil {
		return nil, err
	}

	// check to see if we already have an entry for the cache
	if existingCache, ok = session.caches[name]; ok {
		existing, ok2 := existingCache.(*NamedCacheClient[K, V])
		if !ok2 {
			// the casting failed so return an error indicating the cache exists with different type mappings
			return nil, getExistingError("NamedCache", name)
		}

		// check if there is a difference in cache options wrt near cache.
		// e.g. user has asked for a cache with near cache and previously
		// cache does not have this, or visa-versa
		if !isNearCacheEqual[K, V](existing.baseClient.nearCache, cacheOptions.NearCacheOptions) {
			return nil, getExistingNearCacheError("NamedCache", name)
		}

		session.debug("using existing NamedCache %v", existing)
		return existing, nil
	}

	newCache := &NamedCacheClient[K, V]{
		baseClient: newBaseClient[K, V](session, name, format, sOpts, cacheOptions),
	}

	if err = newCache.baseClient.ensureClientConnection(); err != nil {
		return nil, err
	}

	namedCache := convertNamedCacheClient[K, V](newCache)

	if session.GetProtocolVersion() > 0 {
		// ensure the cache via gRPC v1
		_, err = session.v1StreamManagerCache.ensureCache(context.Background(), name)
		if err != nil {
			return nil, err
		}
	} else {
		// only create event manager if v0
		manager, err1 := newMapEventManager(&namedCache, newCache.baseClient, session)
		if err1 != nil {
			return nil, err1
		}
		newCache.baseClient.eventManager = manager
	}

	// store the new cache
	session.caches[name] = newCache

	listener := newNamedCacheReconnectListener[K, V](*newCache)
	newCache.namedCacheReconnectListener = *listener

	// if near cache then add listener for events and lifecycle
	if newCache.baseClient.nearCache != nil {
		nearCacheListener := newNearNamedCacheMapLister[K, V](*newCache, newCache.baseClient.nearCache)
		newCache.baseClient.nearCacheListener = nearCacheListener
		err = newCache.AddListener(context.Background(), newCache.baseClient.nearCacheListener.listener)
		if err != nil {
			return nil, fmt.Errorf("unable to add listener to near cache: %v", err)
		}

		nearCacheLifecycleListener := newNamedCacheNearLifecycleListener[K, V](*newCache, newCache.baseClient.nearCache)
		newCache.baseClient.nearCacheLifecycleListener = nearCacheLifecycleListener
		newCache.AddLifecycleListener(newCache.baseClient.nearCacheLifecycleListener.listener)
	}
	session.AddSessionLifecycleListener(newCache.namedCacheReconnectListener.listener)

	session.debug("getNamedCache: %v session: %v", namedCache, session)
	return newCache, nil
}

// namedCacheReconnectListener is a session listener to be called on reconnect for a specific [NamedCache].
type namedCacheReconnectListener[K comparable, V any] struct {
	listener SessionLifecycleListener
}

// newReconnectSessionListener creates a new namedCacheReconnectListener.
func newNamedCacheReconnectListener[K comparable, V any](nc NamedCacheClient[K, V]) *namedCacheReconnectListener[K, V] {
	listener := namedCacheReconnectListener[K, V]{
		listener: NewSessionLifecycleListener(),
	}

	listener.listener.OnReconnected(func(_ SessionLifecycleEvent) {
		// re-register listeners for the NamedCache
		namedMap := convertNamedCacheClient[K, V](&nc)
		if err := reRegisterListeners[K, V](context.Background(), &namedMap, nc.baseClient); err != nil {
			logMessage(WARNING, "error re-registering listeners: %v", err)
		}
	})

	return &listener
}

// namedCacheNearCacheListener is a [MapListener] to be called when invalidation events are received for a near cache.
type namedCacheNearCacheListener[K comparable, V any] struct {
	listener  MapListener[K, V]
	nearCache *localCacheImpl[K, V]
}

// namedCacheNearLifecycleListener is a [MapLifecycleListener] to be called when truncate events are received for a near cache.
type namedCacheNearLifestyleListener[K comparable, V any] struct {
	listener  MapLifecycleListener[K, V]
	nearCache *localCacheImpl[K, V]
}

func newNearNamedCacheMapLister[K comparable, V any](nc NamedCacheClient[K, V], cache *localCacheImpl[K, V]) *namedCacheNearCacheListener[K, V] {
	listener := namedCacheNearCacheListener[K, V]{
		listener:  NewMapListener[K, V](),
		nearCache: cache,
	}

	// ensure this is a synchronous MapListener, so we receive events before the result of mutations
	listener.listener.SetSynchronous()

	listener.listener.OnAny(func(e MapEvent[K, V]) {
		err := processNearCacheEvent(nc.baseClient.nearCache, e)
		if err != nil {
			logMessage(WARNING, "error processing near cache MapEvent: %v", e)
		}
	})

	return &listener
}

func newNamedCacheNearLifecycleListener[K comparable, V any](nc NamedCacheClient[K, V], cache *localCacheImpl[K, V]) *namedCacheNearLifestyleListener[K, V] {
	listener := namedCacheNearLifestyleListener[K, V]{
		listener:  NewMapLifecycleListener[K, V](),
		nearCache: cache,
	}

	listener.listener.OnAny(func(e MapLifecycleEvent[K, V]) {
		processNearCacheLifecycleEvent(nc.baseClient.nearCache, e.Type())
	})

	return &listener
}

// ProcessEvent processes a map event and carries out the appropriate action. error is non nil
// if there are any deserialization issues.
func processNearCacheEvent[K comparable, V any](l *localCacheImpl[K, V], e MapEvent[K, V]) error {
	var value *V

	key, err := e.Key()
	if err != nil {
		return err
	}

	if e.Type() == EntryInserted || e.Type() == EntryUpdated {
		value, err = e.NewValue()
		if err != nil {
			return err
		}
		// check to see if the near cache contains this key and if it does then update
		localCacheValue := l.Get(*key)
		if localCacheValue != nil {
			l.Put(*key, *value)
		}

		return nil
	}

	// type must be EntryDeleted, so delete if the near cache contains the entry
	l.Remove(*key)

	return nil
}

// processNearCacheLifecycleEvent processes a lifecycle event and updates the near cache.
func processNearCacheLifecycleEvent[K comparable, V any](l *localCacheImpl[K, V], e MapLifecycleEventType) {
	if e == Truncated || e == Destroyed {
		l.Clear()
	}
}

func convertNamedCacheClient[K comparable, V any](client *NamedCacheClient[K, V]) NamedMap[K, V] {
	return client
}

func ensureNearCacheOptions(options *NearCacheOptions) error {
	if options == nil {
		return nil
	}

	// You can have the following:
	// TTL only
	// TTL + HighUnits
	// TTL + HighUnitsMemory
	// HighUnits
	// HighUnitsMemory

	if options.InvalidationStrategy != ListenAll {
		return fmt.Errorf("the only invalidation strategy supported currently is %v", getInvalidationStrategyString(options.InvalidationStrategy))
	}

	if options.HighUnits < 0 || options.HighUnitsMemory < 0 {
		return ErrNegativeNearCacheOptions
	}

	if options.PruneFactor != 0 && options.PruneFactor < 0.1 || options.PruneFactor > 1 {
		return ErrInvalidPruneFactor
	}

	if options.TTL == 0 && options.HighUnits == 0 && options.HighUnitsMemory == 0 {
		return ErrInvalidNearCache
	}

	if options.TTL != 0 && options.HighUnits != 0 && options.HighUnitsMemory != 0 {
		return ErrInvalidNearCacheWithTTL
	}

	if options.TTL == 0 && options.HighUnits != 0 && options.HighUnitsMemory != 0 {
		return ErrInvalidNearCacheWithNoTTL
	}

	// ensure the default prune factor is set if it is zero
	if options.PruneFactor == 0 {
		options.PruneFactor = defaultPruneFactor
	}

	return nil
}

// isNearCacheEqual returns true if the existing local cache has same options as the provided options
func isNearCacheEqual[K comparable, V any](existing *localCacheImpl[K, V], cacheOptions *NearCacheOptions) bool {
	if existing == nil && cacheOptions == nil {
		return true
	}

	// e.g. user has asked for a cache with near cache and previously
	// cache does not have this, or visa-versa
	if existing == nil && cacheOptions != nil || existing != nil && cacheOptions == nil {
		return false
	}

	// compare near cache options
	existingOptions := existing.options
	if existingOptions.TTL != cacheOptions.TTL {
		return false
	}
	if existingOptions.HighUnits != cacheOptions.HighUnits {
		return false
	}
	if existingOptions.HighUnitsMemory != cacheOptions.HighUnitsMemory {
		return false
	}
	if existingOptions.InvalidationStrategy != cacheOptions.InvalidationStrategy {
		return false
	}
	return existingOptions.PruneFactor == cacheOptions.PruneFactor
}
