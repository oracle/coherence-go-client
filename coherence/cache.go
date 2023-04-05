/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
	"time"
)

// NamedMap defines the APIs to cache data, mapping keys to values, supporting full
// concurrency of retrievals and high expected concurrency for updates.  Like traditional maps,
// this object cannot contain duplicate keys; each key can map to at most one value.
//
// As keys and values must be serializable in some manner. The current supported serialization method is JSON.
//
// Instances of this interface are typically acquired via a coherence.Session.
//
// Although all operations are thread-safe, retrieval operations do not entail locking, and there is no support for
// locking an entire map in a way to prevent all access.  Retrievals reflect the results of the most recently completed
// update operations holding upon their onset.
//
// The type parameters are K = type of the key and V= type of the value.
type NamedMap[K comparable, V any] interface {
	// AddLifecycleListener Adds a MapLifecycleListener that will receive events (truncated, released) that occur
	// against the NamedMap.
	AddLifecycleListener(listener MapLifecycleListener[K, V])

	// AddFilterListener adds a MapListener that will receive events (inserts, updates, deletes) that occur
	// against the NamedMap where entries satisfy the specified filters.Filter, with the key, and optionally,
	// the old-value and new-value included.
	AddFilterListener(ctx context.Context, listener MapListener[K, V], filter filters.Filter) error

	// AddFilterListenerLite adds a MapListener that will receive events (inserts, updates, deletes) that occur
	// against the NamedMap where entries satisfy the specified filters.Filter, with the key,
	// the old-value and new-value included.
	AddFilterListenerLite(ctx context.Context, listener MapListener[K, V], filter filters.Filter) error

	// AddKeyListener adds a MapListener that will receive events (inserts, updates, deletes) that occur
	// against the specified key within the NamedMap, with the key, old-value and new-value included.
	AddKeyListener(ctx context.Context, listener MapListener[K, V], key K) error

	// AddKeyListenerLite adds a MapListener that will receive events (inserts, updates, deletes) that occur
	// against the specified key within the NamedMap, with the key, and optionally, the old-value and new-value included.
	AddKeyListenerLite(ctx context.Context, listener MapListener[K, V], key K) error

	// AddListener adds a MapListener that will receive events (inserts, updates, deletes) that occur
	// against the NamedMap, with the key, old-value and new-value included.
	// This call is equivalent to calling AddFilterListener with filters.Always as the filter.
	AddListener(ctx context.Context, listener MapListener[K, V]) error

	// AddListenerLite adds a MapListener that will receive events (inserts, updates, deletes) that occur
	// against the NamedMap, with the key, and optionally, the old-value and new-value included.
	// This call is equivalent to calling AddFilterListenerLite with filters.Always as the filter.
	AddListenerLite(ctx context.Context, listener MapListener[K, V]) error

	// Clear removes all mappings from this NamedMap.
	Clear(ctx context.Context) error

	// Truncate removes all mappings from this NamedMap.
	// Note: the removal of entries caused by this truncate operation will not be observable.
	Truncate(ctx context.Context) error

	// Destroy releases and destroys this instance of NamedMap.
	// Warning This method is used to completely destroy the specified
	// NamedMap across the cluster. All references in the entire cluster to this
	// cache will be invalidated, the data will be cleared, and all
	// internal resources will be released.
	// Note: the removal of entries caused by this truncate operation will not be observable.
	Destroy(ctx context.Context) error

	// Release releases the instance of NamedMap.
	// This operation does not affect the contents of the NamedMap, but only releases the client
	// resources. To access the NamedMap, you must get a new instance.
	Release()

	// ContainsKey returns true if this NamedMap contains a mapping for the specified key.
	ContainsKey(ctx context.Context, key K) (bool, error)

	// ContainsValue returns true if this NamedMap maps one or more keys to the specified value
	ContainsValue(ctx context.Context, value V) (bool, error)

	// ContainsEntry returns true if this NamedMap contains a mapping for the specified key and value.
	ContainsEntry(ctx context.Context, key K, value V) (bool, error)

	// IsEmpty returns true if this NamedMap contains no mappings.
	IsEmpty(ctx context.Context) (bool, error)

	// EntrySetFilter returns a channel from which entries satisfying the specified filter can be obtained.
	// Each entry in the channel is of type *StreamEntry which basically wraps an error and the result.
	// As always, the result must be accessed (and will be valid) only if the error is nil.
	EntrySetFilter(ctx context.Context, filter filters.Filter) <-chan *StreamedEntry[K, V]

	// EntrySet returns a channel from which all entries can be obtained.
	// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
	// carefull when running this operation against NamedMaps with large number of entries.
	EntrySet(ctx context.Context) <-chan *StreamedEntry[K, V]

	// Get returns the value to which the specified key is mapped. V will be nil if there was no previous value.
	Get(ctx context.Context, key K) (*V, error)

	// GetAll returns a channel from which entries satisfying the specified filter can be obtained.
	// Each entry in the channel is of type *StreamEntry which basically wraps an error and the result.
	// As always, the result must be accessed (and will be valid) only of the error is nil.
	GetAll(ctx context.Context, keys []K) <-chan *StreamedEntry[K, V]

	// GetOrDefault will return the value mapped to the specified key,
	// or if there is no mapping, it will return the specified default.
	GetOrDefault(ctx context.Context, key K, def V) (*V, error)

	// InvokeAll invokes the specified processor against the entries matching the specified keys or filter.  If no
	// keys or filter are specified, then the function will be run against all entries.
	// Functions are invoked atomically against a specific entry as the function may mutate the entry.
	InvokeAll(ctx context.Context, keysOrFilter any, proc processors.Processor) <-chan *StreamedValue[V]

	// KeySetFilter returns a channel from which keys of the entries that satisfy the filter can be obtained.
	// Each entry in the channel is of type *StreamEntry which basically wraps an error and the key.
	// As always, the result must be accessed (and will be valid) only of the error is nil.
	KeySetFilter(ctx context.Context, filter filters.Filter) <-chan *StreamedKey[K]

	// KeySet returns a channel from which keys of all entries can be obtained.
	// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
	// carefull when running this operation against NamedMaps with large number of entries.
	KeySet(ctx context.Context) <-chan *StreamedKey[K]

	// Name returns the name of the NamedMap.
	Name() string

	// Put associates the specified value with the specified key returning the previously
	// mapped value. V will be nil if there was no previous value.
	Put(ctx context.Context, key K, value V) (*V, error)

	// PutAll copies all the mappings from the specified map to the NamedMap.
	PutAll(ctx context.Context, entries map[K]V) error

	// PutIfAbsent adds the specified mapping if the key is not already associated with a value in the NamedMap.
	// Error will be equal to coherence. V will be nil if there was no previous value.
	PutIfAbsent(ctx context.Context, key K, value V) (*V, error)

	// Remove removes the mapping for a key from this NamedMap if it is present and returns the previously
	// mapped value, if any. V will be nil if there was no previous value.
	Remove(ctx context.Context, key K) (*V, error)

	// RemoveLifecycleListener removes the lifecycle listener that was previously registered to receive events.
	RemoveLifecycleListener(listener MapLifecycleListener[K, V])

	// RemoveFilterListener removes the listener that was previously registered to receive events.
	RemoveFilterListener(ctx context.Context, listener MapListener[K, V], filter filters.Filter) error

	// RemoveKeyListener removes the listener that was previously registered to receive events.
	RemoveKeyListener(ctx context.Context, listener MapListener[K, V], key K) error

	// RemoveListener removes the listener that was previously registered to receive events.
	RemoveListener(ctx context.Context, listener MapListener[K, V]) error

	// RemoveMapping removes the entry for the specified key only if it is currently
	// mapped to the specified value.
	RemoveMapping(ctx context.Context, key K, value V) (bool, error)

	// Replace replaces the entry for the specified key only if it is
	// currently mapped to some value.
	Replace(ctx context.Context, key K, value V) (*V, error)

	// ReplaceMapping replaces the entry for the specified key only if it is
	// currently mapped to some value. Returns true if the value was replaced
	ReplaceMapping(ctx context.Context, key K, prevValue V, newValue V) (bool, error)

	// Size returns the number of mappings contained within this NamedMap.
	Size(ctx context.Context) (int, error)

	// GetSession returns the Session associated with this Map.
	GetSession() *Session

	// ValuesFilter return a view of filtered values contained in this NamedMap.
	// The returned channel will be asynchronously filled with values in the
	// NamedMap that satisfy the filter.
	ValuesFilter(ctx context.Context, filter filters.Filter) <-chan *StreamedValue[V]

	// Values return a view of all values contained in this NamedMap.
	// Note: the entries are paged internally to avoid excessive memory usage, but you need to be
	// carefull when running this operation against NamedMaps with large number of entries.
	Values(ctx context.Context) <-chan *StreamedValue[V]

	getBaseClient() *baseClient[K, V]
}

// NamedCache is syntactically identical in behaviour to a NamedMap, but additionally implements
// the PutWithExpiry operation. The type parameters are K = type of the key and V = type of the value.
type NamedCache[K comparable, V any] interface {
	NamedMap[K, V]

	// PutWithExpiry associates the specified value with the specified key. If the cache
	// previously contained a value for this key, the old value is replaced.
	// This variation of the Put(ctx context.Context, key K, value V)
	// function allows the caller to specify an expiry (or "time to live")
	// for the cache entry.  If coherence.ExpiryNever < ttl < 1 millisecond,
	// ttl is set to 1 millisecond.
	// V will be nil if there was no previous value.
	PutWithExpiry(ctx context.Context, key K, value V, ttl time.Duration) (*V, error)
}

// StreamedKey is wrapper object that wraps an error and a key. The Err object must be checked for errors
// before accessing the Key field.
type StreamedKey[K comparable] struct {
	// Err contains the error (if any) while obtaining the key.
	Err error
	// Key contains the key of the entry.
	Key K
}

// StreamedValue is wrapper object that wraps an error and a value. The Err object must be checked for errors
// before accessing the Value field.
type StreamedValue[V any] struct {
	// Err contains the error (if any) while obtaining the value.
	Err error
	// Value contains the value of the entry.
	Value V
}

// StreamedEntry is wrapper object that wraps an error and a Key and a Value .
// As always, the Err object must be checked for errors before accessing the Key or the Value fields.
type StreamedEntry[K comparable, V any] struct {
	// Err contains the error (if any) while obtaining the value.
	Err error
	// Key contains the key of the entry.
	Key K
	// Value contains the value of the entry.
	Value V
}

// Entry represents a returned entry from EntryPageIterator.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}
