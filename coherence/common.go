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
	pb "github.com/oracle/coherence-go-client/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"io"
	"os"
	"sync"
	"time"
)

const (
	envHostName           = "COHERENCE_SERVER_ADDRESS"
	envTLSCertPath        = "COHERENCE_TLS_CERTS_PATH"
	envTLSClientCert      = "COHERENCE_TLS_CLIENT_CERT"
	envTLSClientKey       = "COHERENCE_TLS_CLIENT_KEY"
	envIgnoreInvalidCerts = "COHERENCE_IGNORE_INVALID_CERTS"
	envRequestTimeout     = "COHERENCE_CLIENT_REQUEST_TIMEOUT"
	envDisconnectTimeout  = "COHERENCE_SESSION_DISCONNECT_TIMEOUT"
	envReadyTimeout       = "COHERENCE_READY_TIMEOUT"

	// envSessionDebug enabled session debug messages to be displayed.
	envSessionDebug = "COHERENCE_SESSION_DEBUG"

	// Integer.MAX_VALUE on Java
	integerMaxValue = 2147483647

	ListenAll InvalidationStrategyType = 0
)

var (
	// ErrDestroyed indicates that the NamedMap or NamedCache has been destroyed and can no-longer be used.
	ErrDestroyed = errors.New("the NamedMap or NamedCache has been destroyed and is not usable")

	// ErrReleased indicates that the NamedMap or NamedCache has been released and can no-longer be used.
	ErrReleased = errors.New("the NamedMap or NamedCache has been released and is not usable")

	// ErrClosed indicates that the session has been closed.
	ErrClosed = errors.New("the session is closed and is not usable")

	// ErrShutdown indicates the gRPC channel has been shutdown.
	ErrShutdown = errors.New("gRPC channel has been shutdown")
)

// InvalidationStrategyType described the type if invalidation strategies for near cache.
type InvalidationStrategyType int

// baseClient is a struct that is used for both NamedMap and NamedCache
type baseClient[K comparable, V any] struct {
	session                    *Session
	name                       string          // Name of the NamedMap or NamedCache
	sessionOpts                *SessionOptions // Options for the sessions
	cacheOpts                  *CacheOptions   // Options for the cache or map
	client                     pb.NamedCacheServiceClient
	format                     string
	keySerializer              Serializer[K]
	valueSerializer            Serializer[V]
	eventManager               *mapEventManager[K, V]
	destroyed                  bool
	released                   bool
	mutex                      *sync.RWMutex
	nearCache                  *localCacheImpl[K, V]
	nearCacheListener          *namedCacheNearCacheListener[K, V]
	nearCacheLifecycleListener *namedCacheNearLifecyleListener[K, V]
}

// CacheOptions holds various cache options.
type CacheOptions struct {
	DefaultExpiry    time.Duration
	NearCacheOptions *NearCacheOptions
}

type NearCacheOptions struct {
	TTL                  time.Duration
	HighUnits            int64
	HighUnitsMemory      int64
	InvalidationStrategy InvalidationStrategyType
}

func (n NearCacheOptions) String() string {
	return fmt.Sprintf("NearCacheOptions{TTL=%v, HighUnits=%v, HighUnitsMemory=%v, invalidationStrategy=%v}",
		n.TTL, n.HighUnits, n.HighUnitsMemory, getInvalidationStrategyString(n.InvalidationStrategy))
}

// WithExpiry returns a function to set the default expiry for a [NamedCache]. This option is not valid on [NamedMap].
func WithExpiry(ttl time.Duration) func(cacheOptions *CacheOptions) {
	return func(s *CacheOptions) {
		s.DefaultExpiry = ttl
	}
}

// WithNearCache returns a function to set [NearCacheOptions].
func WithNearCache(options *NearCacheOptions) func(cacheOptions *CacheOptions) {
	return func(s *CacheOptions) {
		s.NearCacheOptions = options
	}
}

// executeClear executes the clear operation against a baseClient.
func executeClear[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) error {
	err := bc.ensureClientConnection()
	if err != nil {
		return err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	clearRequest := pb.ClearRequest{Cache: bc.name, Scope: bc.sessionOpts.Scope}

	_, err = bc.client.Clear(newCtx, &clearRequest)

	return err
}

// registerLifecycleListener registers a lifecycle listener against a base client.
func registerLifecycleListener[K comparable, V any](baseClient *baseClient[K, V], listener MapLifecycleListener[K, V]) {
	baseClient.mutex.Lock()
	defer baseClient.mutex.Unlock()

	baseClient.eventManager.addLifecycleListener(listener)
}

// unregisterLifecycleListener unregisters a lifecycle listener against a base client.
func unregisterLifecycleListener[K comparable, V any](baseClient *baseClient[K, V], listener MapLifecycleListener[K, V]) {
	baseClient.mutex.Lock()
	defer baseClient.mutex.Unlock()

	baseClient.eventManager.removeLifecycleListener(listener)
}

// executeAddIndex executes the add index operation against a baseClient.
func executeAddIndex[K comparable, V, T, E any](ctx context.Context, bc *baseClient[K, V], extractor extractors.ValueExtractor[T, E], sorted bool, comparator extractors.ValueExtractor[T, E]) error {
	var (
		extractorSerializer = NewSerializer[any](bc.format)
		binExtractor        []byte
		binComparator       []byte
		err                 = bc.ensureClientConnection()
	)

	if err != nil {
		return err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binExtractor, err = extractorSerializer.Serialize(extractor)
	if err != nil {
		return err
	}

	binComparator, err = extractorSerializer.Serialize(comparator)
	if err != nil {
		return err
	}

	addIndexRequest := pb.AddIndexRequest{
		Cache: bc.name, Scope: bc.sessionOpts.Scope, Format: bc.format, Extractor: binExtractor, Sorted: sorted, Comparator: binComparator}
	_, err = bc.client.AddIndex(newCtx, &addIndexRequest)
	return err
}

// executeRemoveIndex executes the remove index operation against a baseClient.
func executeRemoveIndex[K comparable, V, T, E any](ctx context.Context, bc *baseClient[K, V], extractor extractors.ValueExtractor[T, E]) error {
	var (
		extractorSerializer = NewSerializer[any](bc.format)
		binExtractor        []byte
		err                 = bc.ensureClientConnection()
	)

	if err != nil {
		return err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binExtractor, err = extractorSerializer.Serialize(extractor)
	if err != nil {
		return err
	}

	removeIndexRequest := pb.RemoveIndexRequest{
		Cache: bc.name, Scope: bc.sessionOpts.Scope, Format: bc.format, Extractor: binExtractor}

	_, err = bc.client.RemoveIndex(newCtx, &removeIndexRequest)
	return err
}

// executeTruncate executes the truncate operation against a baseClient.
func executeTruncate[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) error {
	err := bc.ensureClientConnection()
	if err != nil {
		return err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	request := pb.TruncateRequest{Cache: bc.name, Scope: bc.sessionOpts.Scope}

	_, err = bc.client.Truncate(newCtx, &request)
	return err
}

// executeDestroy executes the destroy operation against a baseClient.
func executeDestroy[K comparable, V any](ctx context.Context, bc *baseClient[K, V], nm NamedMap[K, V]) error {
	err := bc.ensureClientConnection()
	if err != nil {
		return err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	request := pb.DestroyRequest{Cache: bc.name, Scope: bc.sessionOpts.Scope}

	_, err = bc.client.Destroy(newCtx, &request)
	if err != nil {
		return err
	}

	// also mark as released, which
	executeRelease[K, V](bc, nm)

	if manager := bc.eventManager; manager != nil {
		manager.close()
	}

	// make the baseClient as destroyed as it can no longer be used
	bc.destroyed = true

	return nil
}

// executeRelease releases a NamedCache or NamedMap.
func executeRelease[K comparable, V any](bc *baseClient[K, V], nm NamedMap[K, V]) {
	bc.eventManager.dispatch(Released, func() MapLifecycleEvent[K, V] {
		return newMapLifecycleEvent(nm, Released)
	})

	bc.released = true

	if manager := bc.eventManager; manager != nil {
		manager.close()
	}
}

// executeContainsKey executes the containsKey operation against a baseClient.
func executeContainsKey[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K) (bool, error) {
	var (
		result    *wrapperspb.BoolValue
		err       = bc.ensureClientConnection()
		binKey    []byte
		nearCache = bc.nearCache
	)
	if err != nil {
		return false, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// check near cache
	if nearCache != nil {
		ncValue := bc.nearCache.Get(key)
		if ncValue != nil {
			nearCache.registerHit()
			return true, nil
		}
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return false, err
	}

	containsKeyRequest := pb.ContainsKeyRequest{Cache: bc.name, Key: binKey, Format: bc.format, Scope: bc.sessionOpts.Scope}

	result, err = bc.client.ContainsKey(newCtx, &containsKeyRequest)
	if err != nil {
		return false, err
	}
	return result.Value, nil
}

// executeContainsKey executes the containsValue operation against a baseClient.
func executeContainsValue[K comparable, V any](ctx context.Context, bc *baseClient[K, V], value V) (bool, error) {
	var (
		result   *wrapperspb.BoolValue
		err      = bc.ensureClientConnection()
		binValue []byte
	)
	if err != nil {
		return false, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binValue, err = bc.valueSerializer.Serialize(value)
	if err != nil {
		return false, err
	}

	containsValueRequest := pb.ContainsValueRequest{Cache: bc.name, Value: binValue, Format: bc.format, Scope: bc.sessionOpts.Scope}

	result, err = bc.client.ContainsValue(newCtx, &containsValueRequest)
	if err != nil {
		return false, err
	}
	return result.Value, nil
}

// executeContainsEntry executes the containsEntry operation against a baseClient.
func executeContainsEntry[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V) (bool, error) {
	var (
		result   *wrapperspb.BoolValue
		err      = bc.ensureClientConnection()
		binKey   []byte
		binValue []byte
	)
	if err != nil {
		return false, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return false, err
	}

	binValue, err = bc.valueSerializer.Serialize(value)
	if err != nil {
		return false, err
	}

	containsEntryRequest := pb.ContainsEntryRequest{Cache: bc.name, Key: binKey, Value: binValue, Format: bc.format, Scope: bc.sessionOpts.Scope}

	result, err = bc.client.ContainsEntry(newCtx, &containsEntryRequest)
	if err != nil {
		return false, err
	}
	return result.Value, nil
}

// executeIsEmpty executes the IsEmpty operation against a baseClient.
func executeIsEmpty[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) (bool, error) {
	var (
		result *wrapperspb.BoolValue
		err    = bc.ensureClientConnection()
	)
	if err != nil {
		return false, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	emptyRequest := pb.IsEmptyRequest{Cache: bc.name}

	result, err = bc.client.IsEmpty(newCtx, &emptyRequest)
	if err != nil {
		return false, err
	}
	return result.Value, nil
}

// executeGet executes the Get operation against a baseClient.
func executeGet[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K) (*V, error) {
	var (
		binKey    []byte
		err       = bc.ensureClientConnection()
		zeroValue *V
		nearCache = bc.nearCache
	)

	if err != nil {
		return zeroValue, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// check near cache
	if nearCache != nil {
		ncValue := bc.nearCache.Get(key)
		if ncValue != nil {
			nearCache.registerHit()
			return ncValue, nil
		}
	}

	binKey, err = bc.keySerializer.Serialize(key)

	if err != nil {
		return zeroValue, err
	}

	if nearCache != nil {
		// if we get here then it must be a near cache miss because if it was a hit
		// we would have returned already, so save total time we have spent getting
		// the value from Coherence.
		defer func(start time.Time) {
			nearCache.registerMissesNanos(time.Since(start).Nanoseconds())
			nearCache.registerMiss()
		}(time.Now())
	}

	getRequest := pb.GetRequest{Key: binKey, Cache: bc.name, Format: bc.format, Scope: bc.sessionOpts.Scope}

	result, err := bc.client.Get(newCtx, &getRequest)

	if err != nil {
		return zeroValue, err
	}

	if result.Present {
		v, err1 := bc.valueSerializer.Deserialize(result.Value)
		if err1 != nil {
			return nil, err1
		}

		// Add to near cache if one is configured, and we found a value
		if nearCache != nil && v != nil {
			bc.nearCache.Put(key, *v)
		}
		return v, nil
	}

	return nil, nil
}

// executeGet executes the GetAll operation against a baseClient.
func executeGetAll[K comparable, V any](ctx context.Context, bc *baseClient[K, V], keys []K) <-chan *StreamedEntry[K, V] {
	var (
		err              = bc.ensureClientConnection()
		binKeys          = make([][]byte, 0)
		ch               = make(chan *StreamedEntry[K, V])
		nearCache        = bc.nearCache
		nearCacheEntries = make(map[K]*V) // entries found in near cache
		finalKeys        []K
	)

	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		return ch
	}

	newCtx, cancel := bc.session.ensureContext(ctx)

	// if we have a near cache and size > 0 then check to see if we can get any of
	// the keys and values from the near cache
	if nearCache != nil && nearCache.Size() > 0 {
		nearCacheEntries = nearCache.GetAll(keys)
	}

	if len(nearCacheEntries) == 0 {
		// no keys in near cache so fetch all keys
		finalKeys = keys
	} else {
		// we have some keys that were fetched from the near cache, so only
		// include the keys not found in near cache in the finalKeys list to fetch from cluster
		finalKeys = make([]K, 0)

		for _, key := range keys {
			// if we cannot find the key in the near cache entries, then add to final keys to fetch
			if _, ok := nearCacheEntries[key]; !ok {
				finalKeys = append(finalKeys, key)
				nearCache.registerMiss()
			} else {
				// we found the key so increment cache hits
				nearCache.registerHit()
			}
		}
	}

	// serialize the array of keys
	binKeys, err = serializeKeys[K](bc.keySerializer, finalKeys)
	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		return ch
	}

	go func() {
		if cancel != nil {
			defer cancel()
		}

		// if we have any entries in the nearCacheEntries then stream these first
		for k, v := range nearCacheEntries {
			ch <- &StreamedEntry[K, V]{Key: k, Value: *v}
		}

		var (
			request = pb.GetAllRequest{Cache: bc.name, Key: binKeys,
				Format: bc.format, Scope: bc.sessionOpts.Scope}
			key   *K
			value *V
		)

		getAllClient, err1 := bc.client.GetAll(newCtx, &request)
		if err1 != nil {
			ch <- &StreamedEntry[K, V]{Err: err1}
			close(ch)
			return
		}

		for {
			var response = new(pb.Entry)

			err1 = getAllClient.RecvMsg(response)
			if err1 == io.EOF {
				// end of stream
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedEntry[K, V]{Err: err1}
				close(ch)
				return
			}

			// deserialize key and value
			if key, err1 = bc.keySerializer.Deserialize(response.Key); err1 != nil {
				ch <- &StreamedEntry[K, V]{Err: err1}
				close(ch)
				return
			}
			if value, err1 = bc.valueSerializer.Deserialize(response.Value); err1 != nil {
				ch <- &StreamedEntry[K, V]{Err: err1}
				close(ch)
				return
			}

			if nearCache != nil {
				// add to near cache
				nearCache.Put(*key, *value)
			}

			ch <- &StreamedEntry[K, V]{Key: *key, Value: *value}
		}
	}()

	return ch
}

// executeGetOrDefault executes the GetOrDefault operation against a baseClient.
func executeGetOrDefault[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, def V) (*V, error) {
	var (
		finalResult *V
		err         error
	)
	finalResult, err = executeGet(ctx, bc, key)

	if err != nil {
		return finalResult, err
	}

	if finalResult == nil {
		// Nil error indicates no value for key
		return &def, nil
	}

	return finalResult, nil
}

func executeAggregate[K comparable, V, R any](ctx context.Context, bc *baseClient[K, V], keys []K, filter filters.Filter, aggr aggregators.Aggregator[R]) (*R, error) {
	var (
		err              = bc.ensureClientConnection()
		binKeys          = make([][]byte, 0)
		binFilter        = make([]byte, 0)
		binAggregator    []byte
		result           *wrapperspb.BytesValue
		resultSerializer = NewSerializer[R](bc.format)
		zeroValue        *R
		keysLen          = len(keys)
	)
	if err != nil {
		return zeroValue, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	aggregatorSerializer := NewSerializer[any](bc.format)
	binAggregator, err = aggregatorSerializer.Serialize(aggr)
	if err != nil {
		return zeroValue, err
	}

	if keys != nil && keysLen > 0 {
		// keys were specified
		binKeys = make([][]byte, keysLen)

		for i, key := range keys {
			binKeys[i], err = bc.keySerializer.Serialize(key)
			if err != nil {
				return zeroValue, err
			}
		}
	} else if filter != nil {
		// filter was specified
		binFilter, err = NewSerializer[any](bc.format).Serialize(filter)
		if err != nil {
			return zeroValue, err
		}
	}
	// else keys and filter are empty

	request := pb.AggregateRequest{Keys: binKeys, Cache: bc.name,
		Format: bc.format, Scope: bc.sessionOpts.Scope, Aggregator: binAggregator,
		Filter: binFilter}

	result, err = bc.client.Aggregate(newCtx, &request)
	if err != nil {
		return zeroValue, err
	}

	return resultSerializer.Deserialize(result.Value)
}

// executeInvoke executes the Invoke operation against a baseClient.
func executeInvoke[K comparable, V any, R any](ctx context.Context, bc *baseClient[K, V], key K, proc processors.Processor) (*R, error) {
	var (
		err              = bc.ensureClientConnection()
		binKey           []byte
		binProcessor     []byte
		result           *wrapperspb.BytesValue
		resultSerializer = NewSerializer[R](bc.format)
		zeroValue        *R
	)
	if err != nil {
		return zeroValue, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return zeroValue, err
	}

	procSerializer := NewSerializer[any](bc.format)
	binProcessor, err = procSerializer.Serialize(proc)
	if err != nil {
		return zeroValue, err
	}

	request := pb.InvokeRequest{Key: binKey, Cache: bc.name,
		Format: bc.format, Scope: bc.sessionOpts.Scope, Processor: binProcessor}

	result, err = bc.client.Invoke(newCtx, &request)
	if err != nil {
		return zeroValue, err
	}

	return resultSerializer.Deserialize(result.Value)
}

// executeInvokeAll executes the InvokeAll operation with filter or keys, against a baseClient.
func executeInvokeAllFilterOrKeys[K comparable, V any, R any](ctx context.Context, bc *baseClient[K, V], fltr filters.Filter, keys []K, proc processors.Processor) <-chan *StreamedValue[R] {
	var (
		err          = bc.ensureClientConnection()
		binFilter    = make([]byte, 0)
		binProcessor = make([]byte, 0)
		binKeys      = make([][]byte, 0)
		ch           = make(chan *StreamedValue[R])
	)

	if err != nil {
		ch <- &StreamedValue[R]{Err: err}
		return ch
	}

	newCtx, cancel := bc.session.ensureContext(ctx)

	procSerializer := NewSerializer[any](bc.format)
	if binProcessor, err = procSerializer.Serialize(proc); err != nil {
		ch <- &StreamedValue[R]{Err: err}
		return ch
	}

	if fltr != nil {
		if binFilter, err = NewSerializer[any](bc.format).Serialize(fltr); err != nil {
			ch <- &StreamedValue[R]{Err: err}
			return ch
		}
	}
	if len(keys) > 0 {
		// serialize the array of keys
		if binKeys, err = serializeKeys[K](bc.keySerializer, keys); err != nil {
			ch <- &StreamedValue[R]{Err: err}
			return ch
		}
	}

	go func() {
		if cancel != nil {
			defer cancel()
		}
		request := pb.InvokeAllRequest{Cache: bc.name, Filter: binFilter, Keys: binKeys,
			Processor: binProcessor, Format: bc.format, Scope: bc.sessionOpts.Scope}
		valuesClient, err1 := bc.client.InvokeAll(newCtx, &request)
		resultSerializer := NewSerializer[R](bc.format)

		if err1 != nil {
			ch <- &StreamedValue[R]{Err: err1}
			close(ch)
			return
		}

		for {
			var (
				m        = new(wrapperspb.BytesValue)
				response *R
			)
			err1 = valuesClient.RecvMsg(m)
			if err1 == io.EOF {
				// end of stream
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedValue[R]{Err: err1}
				close(ch)
				return
			}

			response, err1 = resultSerializer.Deserialize(m.Value)

			if err1 != nil {
				ch <- &StreamedValue[R]{Err: err1}
				close(ch)
				return
			}

			ch <- &StreamedValue[R]{Value: *response}
		}
	}()

	return ch
}

// executeKeySet executes the KeySet operation against a baseClient.
func executeKeySet[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) <-chan *StreamedKey[K] {
	var (
		err  = bc.ensureClientConnection()
		ch   = make(chan *StreamedKey[K])
		iter = newKeyPageIterator[K, V](ctx, bc)
	)

	if err != nil {
		ch <- &StreamedKey[K]{Err: err}
		return ch
	}

	go func() {
		for {
			result, err1 := iter.Next()

			if err1 == ErrDone {
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedKey[K]{Err: err1}
				close(ch)
				return
			}
			ch <- &StreamedKey[K]{Key: *result}
		}
	}()

	return ch
}

// executeKeySetFilter executes the KeySet operation with filter against a baseClient.
func executeKeySetFilter[K comparable, V any](ctx context.Context, bc *baseClient[K, V], fltr filters.Filter) <-chan *StreamedKey[K] {
	var (
		err       = bc.ensureClientConnection()
		binFilter = make([]byte, 0)
		ch        = make(chan *StreamedKey[K])
	)

	if err != nil {
		ch <- &StreamedKey[K]{Err: err}
		return ch
	}

	newCtx, cancel := bc.session.ensureContext(ctx)

	if fltr == nil {
		fltr = filters.Always()
	}
	binFilter, err = NewSerializer[any](bc.format).Serialize(fltr)
	if err != nil {
		ch <- &StreamedKey[K]{Err: err}
		return ch
	}

	go func() {
		if cancel != nil {
			defer cancel()
		}

		request := pb.KeySetRequest{Cache: bc.name, Filter: binFilter,
			Format: bc.format, Scope: bc.sessionOpts.Scope}
		valuesClient, err1 := bc.client.KeySet(newCtx, &request)

		if err1 != nil {
			ch <- &StreamedKey[K]{Err: err1}
			close(ch)
			return
		}

		for {
			var (
				m        = new(wrapperspb.BytesValue)
				response *K
			)
			err1 = valuesClient.RecvMsg(m)
			if err1 == io.EOF {
				// end of stream
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedKey[K]{Err: err1}
				close(ch)
				return
			}

			response, err1 = bc.keySerializer.Deserialize(m.Value)

			if err1 != nil {
				ch <- &StreamedKey[K]{Err: err1}
				close(ch)
				return
			}

			ch <- &StreamedKey[K]{Key: *response}
		}
	}()

	return ch
}

// executePutAll executes the PutAll operation against a baseClient.
func executePutAll[K comparable, V any](ctx context.Context, bc *baseClient[K, V], entries map[K]V) error {
	var (
		err      = bc.ensureClientConnection()
		binKey   []byte
		binValue []byte
	)
	if err != nil {
		return err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	e := make([]*pb.Entry, len(entries))
	counterPutAll := 0
	for k, v := range entries {
		binKey, err = bc.keySerializer.Serialize(k)
		if err != nil {
			return err
		}

		binValue, err = bc.valueSerializer.Serialize(v)
		if err != nil {
			return err
		}
		e[counterPutAll] = &pb.Entry{Key: binKey, Value: binValue}
		counterPutAll++
	}

	putAllRequest := pb.PutAllRequest{Entry: e, Cache: bc.name, Format: bc.format, Scope: bc.sessionOpts.Scope}

	_, err = bc.client.PutAll(newCtx, &putAllRequest)
	if err != nil {
		return err
	}

	return nil
}

// executePutIfAbsent executes the PutIfAbsent operation against a baseClient.
func executePutIfAbsent[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V) (*V, error) {
	var (
		err       = bc.ensureClientConnection()
		result    *wrapperspb.BytesValue
		binKey    []byte
		binValue  []byte
		zeroValue *V
	)

	if err != nil {
		return zeroValue, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return zeroValue, err
	}

	binValue, err = bc.valueSerializer.Serialize(value)
	if err != nil {
		return zeroValue, err
	}

	putIfAbsentRequest := pb.PutIfAbsentRequest{Key: binKey, Value: binValue, Cache: bc.name, Format: bc.format,
		Scope: bc.sessionOpts.Scope}

	result, err = bc.client.PutIfAbsent(newCtx, &putIfAbsentRequest)
	if err != nil {
		return zeroValue, err
	}

	return bc.valueSerializer.Deserialize(result.Value)
}

// executePutWithExpiry executes the PutWithExpiry operation against a baseClient.
func executePutWithExpiry[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V, ttl time.Duration) (*V, error) {
	var (
		result    *wrapperspb.BytesValue
		binKey    []byte
		binValue  []byte
		err       = bc.ensureClientConnection()
		zeroValue *V
	)

	// check that the expiry value is no > Integer.MAX_VALUE millis on Java, which is 2147483647
	if ttl.Milliseconds() > integerMaxValue {
		return zeroValue, fmt.Errorf("expiry cannot be greater than %d millis or %v", integerMaxValue, integerMaxValue*time.Millisecond)
	}

	if err != nil {
		return zeroValue, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return zeroValue, err
	}

	binValue, err = bc.valueSerializer.Serialize(value)
	if err != nil {
		return zeroValue, err
	}

	putRequest := pb.PutRequest{Key: binKey, Value: binValue, Cache: bc.name,
		Format: bc.format, Ttl: ttl.Milliseconds(), Scope: bc.sessionOpts.Scope}

	result, err = bc.client.Put(newCtx, &putRequest)
	if err != nil {
		return zeroValue, err
	}

	return bc.valueSerializer.Deserialize(result.Value)
}

// executeRemove executes the Remove operation against a baseClient.
func executeRemove[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K) (*V, error) {
	var (
		err       = bc.ensureClientConnection()
		oldValue  *wrapperspb.BytesValue
		binKey    []byte
		zeroValue *V
	)
	if err != nil {
		return zeroValue, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return zeroValue, err
	}

	removeRequest := pb.RemoveRequest{Key: binKey, Cache: bc.name, Format: bc.format, Scope: bc.sessionOpts.Scope}

	oldValue, err = bc.client.Remove(newCtx, &removeRequest)
	if err != nil {
		return zeroValue, err
	}

	return bc.valueSerializer.Deserialize(oldValue.Value)
}

// executeRemoveMapping executes the RemoveMapping operation against a baseClient.
func executeRemoveMapping[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V) (bool, error) {
	var (
		result   *wrapperspb.BoolValue
		err      = bc.ensureClientConnection()
		binKey   []byte
		binValue []byte
	)

	if err != nil {
		return false, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return false, err
	}

	binValue, err = bc.valueSerializer.Serialize(value)
	if err != nil {
		return false, err
	}

	request := pb.RemoveMappingRequest{Cache: bc.name, Key: binKey, Value: binValue, Format: bc.format, Scope: bc.sessionOpts.Scope}

	result, err = bc.client.RemoveMapping(newCtx, &request)
	if err != nil {
		return false, err
	}

	return result.Value, nil
}

// executeReplace executes the Replace operation against a baseClient.
func executeReplace[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V) (*V, error) {
	var (
		err       = bc.ensureClientConnection()
		oldValue  *wrapperspb.BytesValue
		binKey    []byte
		binValue  []byte
		zeroValue *V
	)
	if err != nil {
		return zeroValue, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return zeroValue, err
	}

	binValue, err = bc.valueSerializer.Serialize(value)
	if err != nil {
		return zeroValue, err
	}

	request := pb.ReplaceRequest{Key: binKey, Value: binValue, Cache: bc.name, Format: bc.format, Scope: bc.sessionOpts.Scope}

	oldValue, err = bc.client.Replace(newCtx, &request)
	if err != nil {
		return zeroValue, err
	}

	return bc.valueSerializer.Deserialize(oldValue.Value)
}

// executeReplaceMapping executes the ReplaceMapping operation against a baseClient.
func executeReplaceMapping[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, prevValue V, newValue V) (bool, error) {
	var (
		result       *wrapperspb.BoolValue
		err          = bc.ensureClientConnection()
		binKey       []byte
		binPrevValue []byte
		binNewValue  []byte
	)

	if err != nil {
		return false, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	binKey, err = bc.keySerializer.Serialize(key)
	if err != nil {
		return false, err
	}

	binPrevValue, err = bc.valueSerializer.Serialize(prevValue)
	if err != nil {
		return false, err
	}

	binNewValue, err = bc.valueSerializer.Serialize(newValue)
	if err != nil {
		return false, err
	}

	request := pb.ReplaceMappingRequest{Cache: bc.name, Key: binKey, PreviousValue: binPrevValue,
		NewValue: binNewValue, Format: bc.format, Scope: bc.sessionOpts.Scope}

	result, err = bc.client.ReplaceMapping(newCtx, &request)
	if err != nil {
		return false, err
	}
	return result.Value, nil
}

// executeSize executes the size operation against a baseClient.
func executeSize[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) (int, error) {
	err := bc.ensureClientConnection()
	if err != nil {
		return 0, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	sizeRequest := pb.SizeRequest{Cache: bc.name}

	size, err := bc.client.Size(newCtx, &sizeRequest)
	if err != nil {
		return 0, err
	}
	return int(size.Value), nil
}

// executeIsReady executes the isReady operation against a baseClient.
func executeIsReady[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) (bool, error) {
	err := bc.ensureClientConnection()
	if err != nil {
		return false, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	isReadyRequest := pb.IsReadyRequest{Cache: bc.name}

	isReady, err := bc.client.IsReady(newCtx, &isReadyRequest)

	if err != nil {
		return false, ensureError(err)
	}
	return isReady.Value, nil
}

// executeEntrySet executes the KeySet operation against a baseClient.
func executeEntrySet[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) <-chan *StreamedEntry[K, V] {
	var (
		err = bc.ensureClientConnection()
		ch  = make(chan *StreamedEntry[K, V])
	)

	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		return ch
	}

	iter := newEntryPageIterator[K, V](ctx, bc)

	go func() {
		for {
			result, err1 := iter.Next()

			if err1 == ErrDone {
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedEntry[K, V]{Err: err1}
				close(ch)
				return
			}
			ch <- &StreamedEntry[K, V]{Key: result.Key, Value: result.Value}
		}
	}()

	return ch
}

func executeEntrySetFilter[K comparable, V any](ctx context.Context, bc *baseClient[K, V], fltr filters.Filter) <-chan *StreamedEntry[K, V] {
	var (
		err       = bc.ensureClientConnection()
		binFilter = make([]byte, 0)
		ch        = make(chan *StreamedEntry[K, V])
	)

	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		return ch
	}

	newCtx, cancel := bc.session.ensureContext(ctx)

	if fltr == nil {
		fltr = filters.Always()
	}
	binFilter, err = NewSerializer[any](bc.format).Serialize(fltr)
	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		return ch
	}

	go func() {
		if cancel != nil {
			defer cancel()
		}

		var (
			request = pb.EntrySetRequest{Cache: bc.name, Filter: binFilter,
				Format: bc.format, Scope: bc.sessionOpts.Scope}
			key   *K
			value *V
		)

		entrySetClient, err1 := bc.client.EntrySet(newCtx, &request)
		if err1 != nil {
			ch <- &StreamedEntry[K, V]{Err: err1}
			close(ch)
			return
		}

		for {
			var (
				response = new(pb.Entry)
			)

			err1 = entrySetClient.RecvMsg(response)
			if err1 == io.EOF {
				// end of stream
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedEntry[K, V]{Err: err1}
				close(ch)
				return
			}

			// deserialize key and value
			if key, err1 = bc.keySerializer.Deserialize(response.Key); err1 != nil {
				ch <- &StreamedEntry[K, V]{Err: err1}
				close(ch)
				return
			}
			if value, err1 = bc.valueSerializer.Deserialize(response.Value); err1 != nil {
				ch <- &StreamedEntry[K, V]{Err: err1}
				close(ch)
				return
			}

			ch <- &StreamedEntry[K, V]{Key: *key, Value: *value}
		}
	}()

	return ch
}

// executeValues executes the Values operation against a baseClient.
func executeValues[K comparable, V any](ctx context.Context, bc *baseClient[K, V], fltr filters.Filter) <-chan *StreamedValue[V] {
	var (
		err       = bc.ensureClientConnection()
		binFilter = make([]byte, 0)
		ch        = make(chan *StreamedValue[V])
	)

	if err != nil {
		ch <- &StreamedValue[V]{Err: err}
		return ch
	}

	newCtx, cancel := bc.session.ensureContext(ctx)

	if fltr == nil {
		fltr = filters.Always()
	}
	binFilter, err = NewSerializer[any](bc.format).Serialize(fltr)
	if err != nil {
		ch <- &StreamedValue[V]{Err: err}
		return ch
	}

	go func() {
		if cancel != nil {
			defer cancel()
		}
		request := pb.ValuesRequest{Cache: bc.name, Filter: binFilter,
			Format: bc.format, Scope: bc.sessionOpts.Scope}
		valuesClient, err1 := bc.client.Values(newCtx, &request)

		if err1 != nil {
			ch <- &StreamedValue[V]{Err: err1}
			close(ch)
			return
		}

		for {
			var (
				m        = new(wrapperspb.BytesValue)
				response *V
			)
			err1 = valuesClient.RecvMsg(m)
			if err1 == io.EOF {
				// end of stream
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedValue[V]{Err: err1}
				close(ch)
				return
			}

			response, err1 = bc.valueSerializer.Deserialize(m.Value)

			if err1 != nil {
				ch <- &StreamedValue[V]{Err: err1}
				close(ch)
				return
			}

			ch <- &StreamedValue[V]{Value: *response}
		}
	}()

	return ch
}

// executeValuesNoFilter executes the Values operation against a baseClient when no filter is required.
func executeValuesNoFilter[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) <-chan *StreamedValue[V] {
	var (
		err = bc.ensureClientConnection()
		ch  = make(chan *StreamedValue[V])
	)

	if err != nil {
		ch <- &StreamedValue[V]{Err: err}
		return ch
	}

	iter := newValuePageIterator[K, V](ctx, bc)

	go func() {
		for {
			result, err1 := iter.Next()

			if err1 == ErrDone {
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedValue[V]{Err: err1}
				close(ch)
				return
			}
			ch <- &StreamedValue[V]{Value: *result}
		}
	}()

	return ch
}

// ensureClientConnection ensures the connection is established and
// ensures a NewNamedCacheServiceClient is present.
func (c *baseClient[K, V]) ensureClientConnection() error {
	// ensure we have a valid connection
	var (
		session = c.session
		err     error
	)
	if c.destroyed {
		return ErrDestroyed
	}
	if c.released {
		return ErrReleased
	}

	err = session.ensureConnection()
	if err != nil {
		return err
	}

	// ensure we have a NamedCacheServiceClient
	if c.client != nil {
		return nil
	}

	c.client = pb.NewNamedCacheServiceClient(session.conn)

	return nil
}

func getStringValueFromEnvVarOrDefault(envVar string, defaultValue string) string {
	if val := os.Getenv(envVar); val != "" {
		return val
	}
	return defaultValue
}

func getBoolValueFromEnvVarOrDefault(envVar string, defaultValue bool) bool {
	if val := os.Getenv(envVar); val != "" {
		return val == "true"
	}
	return defaultValue
}

// serializeKeys serializes an array of keys
func serializeKeys[K comparable](serializer Serializer[K], keys []K) ([][]byte, error) {
	var (
		binKeys = make([][]byte, 0)
		err     error
		binKey  []byte
	)

	for _, k := range keys {
		if binKey, err = serializer.Serialize(k); err != nil {
			return binKeys, err
		}
		binKeys = append(binKeys, binKey)
	}

	return binKeys, nil
}

// ensureError inspects the error and if it is gRPC related, will wrap with a more helpful message.
// any new execute* function added since v1.0.0 must call this to wrap errors.
func ensureError(err error) error {
	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("this operation is not supported by the current gRPC proxy, either upgrade the version of Coherence on the gRPC proxy or connect to a gRPC proxy that supports the operation: %w", err)
	}
	return err
}

func getInvalidationStrategyString(strategy InvalidationStrategyType) string {
	if strategy == ListenAll {
		return "ListenAll"
	}
	return "UNKNOWN"
}
