/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"errors"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence/aggregators"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
	pb "github.com/oracle/coherence-go-client/v2/proto"
	pb1 "github.com/oracle/coherence-go-client/v2/proto/v1"
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

	// envSessionDebug enables session debug messages to be displayed. Deprecated, use COHERENCE_LOG_LEVEL=DEBUG instead.
	envSessionDebug = "COHERENCE_SESSION_DEBUG"

	// envMessageDebug enables message debug messages to be displayed. Deprecated, use COHERENCE_LOG_LEVEL=ALL instead.
	envMessageDebug = "COHERENCE_MESSAGE_DEBUG"

	// envResolverDebug enables resolver debug messages to be displayed. Deprecated, use COHERENCE_LOG_LEVEL=DEBUG instead.
	envResolverDebug = "COHERENCE_RESOLVER_DEBUG"

	// envResolverDebug sets the number of retries when the resolver fails.
	envResolverRetries = "COHERENCE_RESOLVER_RETRIES"

	// envResolverDebug enables randomization of addresses returned by resolver
	envResolverRandomize = "COHERENCE_RESOLVER_RANDOMIZE"

	// the Coherence log level: 1 -> 5 (ERROR -> ALL)
	envLogLevel = "COHERENCE_LOG_LEVEL"

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
	name                       string                     // Name of the NamedMap or NamedCache
	sessionOpts                *SessionOptions            // Options for the sessions
	cacheOpts                  *CacheOptions              // Options for the cache or map
	client                     pb.NamedCacheServiceClient // original v0 client
	format                     string
	keySerializer              Serializer[K]
	valueSerializer            Serializer[V]
	eventManager               *mapEventManager[K, V]
	destroyed                  bool
	released                   bool
	mutex                      *sync.RWMutex
	nearCache                  *localCacheImpl[K, V]
	nearCacheListener          *namedCacheNearCacheListener[K, V]
	nearCacheLifecycleListener *namedCacheNearLifestyleListener[K, V]

	// gRPC v1 listeners registered
	keyListenersV1       map[K]*listenerGroupV1[K, V]
	filterListenersV1    map[filters.Filter]*listenerGroupV1[K, V]
	filterIDToGroupV1    map[int64]*listenerGroupV1[K, V]
	lifecycleListenersV1 []*MapLifecycleListener[K, V]
}

// CacheOptions holds various cache options.
type CacheOptions struct {
	DefaultExpiry    time.Duration
	NearCacheOptions *NearCacheOptions
}

// NearCacheOptions defines options when creating a near cache.
type NearCacheOptions struct {
	// TTL is the maximum time to keep the entry in the near cache. When this time has been reached it will be expired.
	TTL time.Duration

	// HighUnits is the maximum number of cache entries to keep in the near cache.
	HighUnits int64

	// HighUnitsMemory is the maximum amount of memory to use for entries in the near cache.
	HighUnitsMemory int64

	InvalidationStrategy InvalidationStrategyType // currently only supports ListenAll

	// PruneFactor indicates the percentage of the total number of units that will remain
	// after the cache manager prunes the near cache(i.e. this is the "low watermark" value)
	// this value is in the range 0.1 to 1.0 and the default is 0.8 or 80%.
	PruneFactor float32
}

func (n NearCacheOptions) String() string {
	return fmt.Sprintf("NearCacheOptions{TTL=%v, HighUnits=%v, HighUnitsMemory=%v, PruneFactor=%.2f, invalidationStrategy=%v}",
		n.TTL, n.HighUnits, n.HighUnitsMemory, n.PruneFactor, getInvalidationStrategyString(n.InvalidationStrategy))
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
	var (
		err       = bc.ensureClientConnection()
		nearCache = bc.nearCache
	)

	if err != nil {
		return err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	if bc.session.GetProtocolVersion() > 0 {
		err = bc.session.v1StreamManagerCache.clearCache(newCtx, bc.name)
	} else {
		clearRequest := pb.ClearRequest{Cache: bc.name, Scope: bc.sessionOpts.Scope}

		_, err = bc.client.Clear(newCtx, &clearRequest)
	}

	// clear the near cache
	if bc.session.GetProtocolVersion() == 0 && nearCache != nil {
		nearCache.Clear()
	}

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
func executeAddIndex[K comparable, V, T, E any](ctx context.Context, bc *baseClient[K, V], extractor extractors.ValueExtractor[T, E], sorted bool, comparator extractors.Comparator[E]) error {
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

	if bc.session.GetProtocolVersion() > 0 {
		return bc.session.v1StreamManagerCache.addIndex(newCtx, bc.name, binExtractor, &sorted, binComparator)
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

	if bc.session.GetProtocolVersion() > 0 {
		return bc.session.v1StreamManagerCache.removeIndex(newCtx, bc.name, binExtractor)
	}

	removeIndexRequest := pb.RemoveIndexRequest{
		Cache: bc.name, Scope: bc.sessionOpts.Scope, Format: bc.format, Extractor: binExtractor}

	_, err = bc.client.RemoveIndex(newCtx, &removeIndexRequest)
	return err
}

// executeTruncate executes the truncate operation against a baseClient.
func executeTruncate[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) error {
	var (
		err       = bc.ensureClientConnection()
		nearCache = bc.nearCache
	)
	if err != nil {
		return err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	if bc.session.GetProtocolVersion() > 0 {
		err = bc.session.v1StreamManagerCache.truncateCache(newCtx, bc.name)
		if err != nil {
			return err
		}
	} else {
		request := pb.TruncateRequest{Cache: bc.name, Scope: bc.sessionOpts.Scope}

		_, err = bc.client.Truncate(newCtx, &request)
	}

	// clear the near cache as the lifecycle listeners are not synchronous
	if nearCache != nil {
		nearCache.Clear()
	}

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

	if bc.session.GetProtocolVersion() > 0 {
		err = bc.session.v1StreamManagerCache.destroyCache(newCtx, bc.name)
		if err != nil {
			return err
		}
	} else {
		request := pb.DestroyRequest{Cache: bc.name, Scope: bc.sessionOpts.Scope}

		_, err = bc.client.Destroy(newCtx, &request)
		if err != nil {
			return err
		}
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
	if bc.session.GetProtocolVersion() == 0 {
		bc.eventManager.dispatch(Released, func() MapLifecycleEvent[K, V] {
			return newMapLifecycleEvent(nm, Released)
		})

		if manager := bc.eventManager; manager != nil {
			manager.close()
		}
	} else {
		bc.generateMapLifecycleEvent(nm, Released)
	}
	bc.released = true
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

	if bc.session.GetProtocolVersion() > 0 {
		return bc.session.v1StreamManagerCache.containsKey(newCtx, bc.name, binKey)
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

	if bc.session.GetProtocolVersion() > 0 {
		return bc.session.v1StreamManagerCache.containsValue(newCtx, bc.name, binValue)
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

	if bc.session.GetProtocolVersion() > 0 {
		return bc.session.v1StreamManagerCache.containsEntry(newCtx, bc.name, binKey, binValue)
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
		result  *wrapperspb.BoolValue
		err     = bc.ensureClientConnection()
		isEmpty bool
	)
	if err != nil {
		return false, err
	}

	newCtx, cancel := bc.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	if bc.session.GetProtocolVersion() > 0 {
		isEmpty, err = bc.session.v1StreamManagerCache.isEmpty(newCtx, bc.name)
		if err != nil {
			return false, err
		}
		result = &wrapperspb.BoolValue{Value: isEmpty}
	} else {
		emptyRequest := pb.IsEmptyRequest{Cache: bc.name}

		result, err = bc.client.IsEmpty(newCtx, &emptyRequest)
		if err != nil {
			return false, err
		}
	}

	return result.Value, nil
}

// executeGet executes the Get operation against a baseClient.
func executeGet[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K) (*V, error) {
	var (
		result      *pb.OptionalValue
		resultBytes *[]byte
		binKey      []byte
		err         = bc.ensureClientConnection()
		zeroValue   *V
		nearCache   = bc.nearCache
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

	if bc.session.GetProtocolVersion() > 0 {
		resultBytes, err = bc.session.v1StreamManagerCache.get(newCtx, bc.name, binKey)
		if err != nil {
			return zeroValue, err
		}
		result = ensureOptionalValue(resultBytes)
	} else {
		getRequest := pb.GetRequest{Key: binKey, Cache: bc.name, Format: bc.format, Scope: bc.sessionOpts.Scope}

		result, err = bc.client.Get(newCtx, &getRequest)

		if err != nil {
			return zeroValue, err
		}
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

		// if we have any entries in the near cache entries then stream these first
		for k, v := range nearCacheEntries {
			ch <- &StreamedEntry[K, V]{Key: k, Value: *v}
		}

		// if we can get all keys from near cache then return immediately
		if len(finalKeys) == 0 {
			close(ch)
			return
		}

		if bc.session.GetProtocolVersion() > 0 {
			executeGetAllV1(ctx, bc, binKeys, ch)
			close(ch)
			return
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

			ch <- makeStreamedEntry[K, V](key, value, nil)
		}
	}()

	return ch
}

// executeGetAllV1 executes a getAll() when connected to v1 gRPC proxy.
func executeGetAllV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V], binKeys [][]byte, ch chan *StreamedEntry[K, V]) {
	nearCache := bc.nearCache
	chGetAll, err := bc.session.v1StreamManagerCache.getAll(ctx, bc.name, binKeys)
	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		close(ch)
	}

	var (
		key   *K
		value *V
	)

	for v := range chGetAll {
		if v.Err != nil {
			ch <- &StreamedEntry[K, V]{Err: v.Err}
			close(ch)
			return
		}

		// deserialize key and value
		if key, err = bc.keySerializer.Deserialize(v.Key); err != nil {
			ch <- &StreamedEntry[K, V]{Err: err}
			close(ch)
			return
		}
		if value, err = bc.valueSerializer.Deserialize(v.Value); err != nil {
			ch <- &StreamedEntry[K, V]{Err: err}
			close(ch)
			return
		}

		if nearCache != nil {
			// add to near cache
			nearCache.Put(*key, *value)
		}

		ch <- makeStreamedEntry[K, V](key, value, nil)
	}
}

// executeInvokeAllFilterOrKeysV1 executes an invokeAll() when connected to v1 gRPC proxy.
func executeInvokeAllFilterOrKeysV1[K comparable, V any, R any](ctx context.Context, bc *baseClient[K, V], agent []byte, binKeys [][]byte, binFilter []byte, ch chan *StreamedEntry[K, R]) {
	keysOrFilter := ensureKeysOrFilterGrpcV1(binKeys, binFilter)
	chInvoke, err := bc.session.v1StreamManagerCache.invoke(ctx, bc.name, agent, keysOrFilter)

	if err != nil {
		ch <- &StreamedEntry[K, R]{Err: err}
		close(ch)
	}

	getStreamedEntry[K, V, R](bc, ch, chInvoke)
}

// executeInvokeAllFilterOrKeysV1 executes an invokeAll() when connected to v1 gRPC proxy.
func executeInvokeAllFilterOrKeysV1Value[K comparable, V any, R any](ctx context.Context, bc *baseClient[K, V], agent []byte, binKeys [][]byte, binFilter []byte, ch chan *StreamedValue[R]) {
	keysOrFilter := ensureKeysOrFilterGrpcV1(binKeys, binFilter)
	chInvoke, err := bc.session.v1StreamManagerCache.invoke(ctx, bc.name, agent, keysOrFilter)

	if err != nil {
		ch <- &StreamedValue[R]{Err: err}
		close(ch)
	}

	getStreamedValuesWithEntry[K, V, R](bc, ch, chInvoke)
}

// executeInvokeAllFilterOrKeysV1 executes an invokeAll() when connected to v1 gRPC proxy.
func executeInvokeEntrySetFilterV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V], binFilter []byte, binComparator []byte, ch chan *StreamedEntry[K, V]) {
	chInvoke, err := bc.session.v1StreamManagerCache.entrySetFilter(ctx, bc.name, binFilter, binComparator)

	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		close(ch)
	}

	getStreamedEntries[K, V](bc, ch, chInvoke)
}

// executeKeySetFilterV1 executes an keySet with filter when connected to v1 gRPC proxy.
func executeKeySetFilterV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V], binFilter []byte, ch chan *StreamedKey[K]) {
	chInvoke, err := bc.session.v1StreamManagerCache.keySetFilter(ctx, bc.name, binFilter)

	if err != nil {
		ch <- &StreamedKey[K]{Err: err}
		close(ch)
	}

	getStreamedKeys[K, V](bc, ch, chInvoke)
}

// executeValuesFilterV1 executes an values with filter when connected to v1 gRPC proxy.
func executeValuesFilterV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V], binFilter []byte, binComparator []byte, ch chan *StreamedValue[V]) {
	chInvoke, err := bc.session.v1StreamManagerCache.valuesFilter(ctx, bc.name, binFilter, binComparator)

	if err != nil {
		ch <- &StreamedValue[V]{Err: err}
		close(ch)
	}

	getStreamedValuesWithValue[K, V, V](bc, ch, chInvoke)
}

func getStreamedEntries[K comparable, V any](bc *baseClient[K, V], chResponse chan *StreamedEntry[K, V], ch <-chan BinaryKeyAndValue) {
	var (
		key   *K
		value *V
		err   error
	)

	for v := range ch {
		if v.Err != nil {
			chResponse <- &StreamedEntry[K, V]{Err: v.Err}
			close(chResponse)
			return
		}

		// deserialize key and value
		if key, err = bc.keySerializer.Deserialize(v.Key); err != nil {
			chResponse <- &StreamedEntry[K, V]{Err: err}
			close(chResponse)
			return
		}
		if value, err = bc.valueSerializer.Deserialize(v.Value); err != nil {
			chResponse <- &StreamedEntry[K, V]{Err: err}
			close(chResponse)
			return
		}

		chResponse <- makeStreamedEntry[K, V](key, value, nil)
	}
}

func getStreamedKeys[K comparable, V any](bc *baseClient[K, V], chResponse chan *StreamedKey[K], ch <-chan BinaryKey) {
	var (
		key *K
		err error
	)

	for v := range ch {
		if v.Err != nil {
			chResponse <- &StreamedKey[K]{Err: v.Err}
			close(chResponse)
			return
		}
		// deserialize key
		if key, err = bc.keySerializer.Deserialize(v.Key); err != nil {
			chResponse <- &StreamedKey[K]{Err: err}
			close(chResponse)
			return
		}

		chResponse <- &StreamedKey[K]{Key: *key}
	}
}

// getStreamedValues returns the binary key and value for an entry processor, but will just return the key
func getStreamedEntry[K comparable, V any, R any](bc *baseClient[K, V], chResponse chan *StreamedEntry[K, R], ch <-chan BinaryKeyAndValue) {
	var (
		key   *K
		value *R
		err   error
	)

	for v := range ch {
		if v.Err != nil {
			chResponse <- &StreamedEntry[K, R]{Err: v.Err}
			return
		}

		// deserialize key and value
		resultSerializer := NewSerializer[R](bc.format)
		if value, err = resultSerializer.Deserialize(v.Value); err != nil {
			chResponse <- &StreamedEntry[K, R]{Err: err}
			return
		}

		if key, err = bc.keySerializer.Deserialize(v.Key); err != nil {
			chResponse <- &StreamedEntry[K, R]{Err: err}
			close(chResponse)
			return
		}

		chResponse <- makeStreamedEntry[K, R](key, value, err)
	}
}

func makeStreamedEntry[K comparable, V any](key *K, value *V, err error) *StreamedEntry[K, V] {
	streamedEntry := StreamedEntry[K, V]{Err: err}
	if key != nil {
		streamedEntry.Key = *key
	}
	if value != nil {
		streamedEntry.Value = *value
	}
	return &streamedEntry
}

func getStreamedValuesWithEntry[K comparable, V any, R any](bc *baseClient[K, V], chResponse chan *StreamedValue[R], ch <-chan BinaryKeyAndValue) {
	var (
		value *R
		err   error
	)

	for v := range ch {
		if v.Err != nil {
			chResponse <- &StreamedValue[R]{Err: v.Err}
			close(chResponse)
			return
		}

		// deserialize the result only
		resultSerializer := NewSerializer[R](bc.format)
		if value, err = resultSerializer.Deserialize(v.Value); err != nil {
			chResponse <- &StreamedValue[R]{Err: err}
			close(chResponse)
			return
		}

		if value == nil {
			chResponse <- &StreamedValue[R]{IsValueEmpty: true}
		} else {
			chResponse <- &StreamedValue[R]{Value: *value}
		}
	}
}

func getStreamedValuesWithValue[K comparable, V any, R any](bc *baseClient[K, V], chResponse chan *StreamedValue[R], ch <-chan BinaryValue) {
	var (
		value *R
		err   error
	)

	for v := range ch {
		if v.Err != nil {
			chResponse <- &StreamedValue[R]{Err: v.Err}
			close(chResponse)
			return
		}

		// deserialize the result only
		resultSerializer := NewSerializer[R](bc.format)
		if value, err = resultSerializer.Deserialize(v.Value); err != nil {
			chResponse <- &StreamedValue[R]{Err: err}
			close(chResponse)
			return
		}

		if value == nil {
			chResponse <- &StreamedValue[R]{IsValueEmpty: true}
		} else {
			chResponse <- &StreamedValue[R]{Value: *value}
		}
	}
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

	if bc.session.GetProtocolVersion() > 0 {
		res, err1 := bc.session.v1StreamManagerCache.aggregate(newCtx, bc.name, binAggregator, ensureKeysOrFilterGrpcV1(binKeys, binFilter))
		if err1 != nil {
			return zeroValue, err1
		}

		result = ensureBytesValue(res)
	} else {
		request := pb.AggregateRequest{Keys: binKeys, Cache: bc.name,
			Format: bc.format, Scope: bc.sessionOpts.Scope, Aggregator: binAggregator,
			Filter: binFilter}

		result, err = bc.client.Aggregate(newCtx, &request)
		if err != nil {
			return zeroValue, err
		}
	}

	return resultSerializer.Deserialize(result.Value)
}

func ensureKeysOrFilterGrpcV1(binKeys [][]byte, binFilter []byte) *pb1.KeysOrFilter {
	keysOrFilter := &pb1.KeysOrFilter{}
	if len(binKeys) != 0 {
		keysOrFilterKeys := &pb1.KeysOrFilter_Keys{Keys: &pb1.CollectionOfBytesValues{Values: binKeys}}
		keysOrFilter.KeyOrFilter = keysOrFilterKeys
	} else {
		keysOrFilterFilter := &pb1.KeysOrFilter_Filter{Filter: binFilter}
		keysOrFilter.KeyOrFilter = keysOrFilterFilter
	}

	return keysOrFilter
}

func ensureKeyOrFilterGrpcV1(binKey []byte, binFilter []byte) *pb1.KeyOrFilter {
	keyOrFilter := &pb1.KeyOrFilter{}
	if len(binKey) != 0 {
		keyOrFilterKey := &pb1.KeyOrFilter_Key{Key: binKey}
		keyOrFilter.KeyOrFilter = keyOrFilterKey
	} else {
		keyOrFilterFilter := &pb1.KeyOrFilter_Filter{Filter: binFilter}
		keyOrFilter.KeyOrFilter = keyOrFilterFilter
	}

	return keyOrFilter
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

	if bc.session.GetProtocolVersion() > 0 {
		binKeys := make([][]byte, 1)
		binKeys[0] = binKey

		// create a channel from which we will get the first value
		ch := make(chan *StreamedValue[R])

		go func() {
			defer close(ch)
			executeInvokeAllFilterOrKeysV1Value(ctx, bc, binProcessor, binKeys, emptyByte, ch)
		}()

		v, ok := <-ch
		if !ok {
			// channel has closed, no data to return
			return nil, nil
		}

		if v.Err != nil {
			return zeroValue, v.Err
		}
		if v.IsValueEmpty {
			return nil, nil
		}
		return &v.Value, nil
	}

	// gRPC v0
	request := pb.InvokeRequest{Key: binKey, Cache: bc.name,
		Format: bc.format, Scope: bc.sessionOpts.Scope, Processor: binProcessor}

	result, err = bc.client.Invoke(newCtx, &request)
	if err != nil {
		return zeroValue, err
	}

	return resultSerializer.Deserialize(result.Value)
}

// executeInvokeAll executes the InvokeAll operation with filter or keys, against a baseClient.
func executeInvokeAllFilterOrKeys[K comparable, V any, R any](ctx context.Context, bc *baseClient[K, V], fltr filters.Filter, keys []K, proc processors.Processor) <-chan *StreamedEntry[K, R] {
	var (
		err          = bc.ensureClientConnection()
		binFilter    = make([]byte, 0)
		binProcessor = make([]byte, 0)
		binKeys      = make([][]byte, 0)
		ch           = make(chan *StreamedEntry[K, R])
	)

	if err != nil {
		ch <- &StreamedEntry[K, R]{Err: err}
		return ch
	}

	newCtx, cancel := bc.session.ensureContext(ctx)

	procSerializer := NewSerializer[any](bc.format)
	if binProcessor, err = procSerializer.Serialize(proc); err != nil {
		ch <- &StreamedEntry[K, R]{Err: err}
		return ch
	}

	if fltr != nil {
		if binFilter, err = NewSerializer[any](bc.format).Serialize(fltr); err != nil {
			ch <- &StreamedEntry[K, R]{Err: err}
			return ch
		}
	}
	if len(keys) > 0 {
		// serialize the array of keys
		if binKeys, err = serializeKeys[K](bc.keySerializer, keys); err != nil {
			ch <- &StreamedEntry[K, R]{Err: err}
			return ch
		}
	}

	go func() {
		if cancel != nil {
			defer cancel()
		}

		if bc.session.GetProtocolVersion() > 0 {
			executeInvokeAllFilterOrKeysV1(ctx, bc, binProcessor, binKeys, binFilter, ch)
			close(ch)
			return
		}

		request := pb.InvokeAllRequest{Cache: bc.name, Filter: binFilter, Keys: binKeys,
			Processor: binProcessor, Format: bc.format, Scope: bc.sessionOpts.Scope}
		valuesClient, err1 := bc.client.InvokeAll(newCtx, &request)
		resultSerializer := NewSerializer[R](bc.format)

		if err1 != nil {
			ch <- &StreamedEntry[K, R]{Err: err1}
			close(ch)
			return
		}

		for {
			var (
				m        = new(pb.Entry)
				key      *K
				response *R
			)
			err1 = valuesClient.RecvMsg(m)
			if err1 == io.EOF {
				// end of stream
				close(ch)
				return
			} else if err1 != nil {
				ch <- &StreamedEntry[K, R]{Err: err1}
				close(ch)
				return
			}

			response, err1 = resultSerializer.Deserialize(m.Value)

			if err1 != nil {
				ch <- &StreamedEntry[K, R]{Err: err1}
				close(ch)
				return
			}
			key, err1 = bc.keySerializer.Deserialize(m.Key)
			if err1 != nil {
				ch <- &StreamedEntry[K, R]{Err: err1}
				close(ch)
				return
			}

			ch <- makeStreamedEntry(key, response, nil)
		}
	}()

	return ch
}

// executeKeySet executes the KeySet operation against a baseClient.
func executeKeySet[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) <-chan *StreamedKey[K] {
	var (
		err  = bc.ensureClientConnection()
		ch   = make(chan *StreamedKey[K])
		iter keyPageIterator[K, V]
	)

	if bc.getProtocolVersion() > 0 {
		iter = newKeyPageIteratorV1[K, V](ctx, bc)
	} else {
		iter = newKeyPageIterator[K, V](ctx, bc)
	}

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

		if bc.session.GetProtocolVersion() > 0 {
			executeKeySetFilterV1(ctx, bc, binFilter, ch)
			close(ch)
			return
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
func executePutAll[K comparable, V any](ctx context.Context, bc *baseClient[K, V], entries map[K]V, ttl time.Duration) error {
	var (
		err       = bc.ensureClientConnection()
		binKey    []byte
		binValue  []byte
		nearCache = bc.nearCache
	)
	if err != nil {
		return err
	}

	if bc.session.GetProtocolVersion() == 0 && ttl > 0 {
		return errors.New("this Coherence cluster version does not support PutAllWithExpiry")
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

	if bc.session.GetProtocolVersion() > 0 {
		v1Entries := make([]*pb1.BinaryKeyAndValue, counterPutAll)
		for k, v := range e {
			v1Entries[k] = &pb1.BinaryKeyAndValue{Key: v.Key, Value: v.Value}
		}

		err = bc.session.v1StreamManagerCache.putAll(newCtx, bc.name, v1Entries, ttl)
		if err != nil {
			return err
		}
	} else {
		putAllRequest := pb.PutAllRequest{Entry: e, Cache: bc.name, Format: bc.format, Scope: bc.sessionOpts.Scope}

		_, err = bc.client.PutAll(newCtx, &putAllRequest)
		if err != nil {
			return err
		}
	}

	// if we have near cache and the entry exists then update (gRPC v0)
	if bc.session.GetProtocolVersion() == 0 && nearCache != nil {
		for k, v := range entries {
			if oldVal := nearCache.Get(k); oldVal != nil {
				nearCache.Put(k, v)
			}
		}
	}

	return nil
}

// executePutIfAbsent executes the PutIfAbsent operation against a baseClient.
func executePutIfAbsent[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V) (*V, error) {
	var (
		err         = bc.ensureClientConnection()
		bytesResult *[]byte
		result      *wrapperspb.BytesValue
		binKey      []byte
		binValue    []byte
		zeroValue   *V
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

	if bc.session.GetProtocolVersion() > 0 {
		bytesResult, err = bc.session.v1StreamManagerCache.putIfAbsent(newCtx, bc.name, binKey, binValue)
		if err != nil {
			return zeroValue, err
		}
		result = ensureBytesValue(bytesResult)
	} else {
		putIfAbsentRequest := pb.PutIfAbsentRequest{Key: binKey, Value: binValue, Cache: bc.name, Format: bc.format,
			Scope: bc.sessionOpts.Scope}

		result, err = bc.client.PutIfAbsent(newCtx, &putIfAbsentRequest)
		if err != nil {
			return zeroValue, err
		}
	}

	return bc.valueSerializer.Deserialize(result.Value)
}

// executePutWithExpiry executes the PutWithExpiry operation against a baseClient.
func executePutWithExpiry[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V, ttl time.Duration) (*V, error) {
	var (
		result      *wrapperspb.BytesValue
		bytesResult *[]byte
		binKey      []byte
		binValue    []byte
		err         = bc.ensureClientConnection()
		zeroValue   *V
		nearCache   = bc.nearCache
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

	if bc.session.GetProtocolVersion() > 0 {
		bytesResult, err = bc.session.v1StreamManagerCache.put(newCtx, bc.name, binKey, binValue, ttl)
		if err != nil {
			return zeroValue, err
		}
		result = ensureBytesValue(bytesResult)
	} else {
		putRequest := pb.PutRequest{Key: binKey, Value: binValue, Cache: bc.name,
			Format: bc.format, Ttl: ttl.Milliseconds(), Scope: bc.sessionOpts.Scope}

		result, err = bc.client.Put(newCtx, &putRequest)
		if err != nil {
			return zeroValue, err
		}
	}

	// if we have near cache and the entry exists then update this as well because we do
	// not use synchronous listener in gRPC v0
	if bc.session.GetProtocolVersion() == 0 && nearCache != nil {
		if oldValue := nearCache.Get(key); oldValue != nil {
			nearCache.Put(key, value)
		}
	}

	return bc.valueSerializer.Deserialize(result.Value)
}

func ensureBytesValue(result *[]byte) *wrapperspb.BytesValue {
	var bytesValue = wrapperspb.BytesValue{}

	if result != nil {
		bytesValue.Value = *result
	}

	return &bytesValue
}

func ensureOptionalValue(result *[]byte) *pb.OptionalValue {
	var optionalValue = pb.OptionalValue{}

	if result != nil {
		optionalValue.Value = *result
		optionalValue.Present = true
	}

	return &optionalValue
}

// executeRemove executes the Remove operation against a baseClient.
func executeRemove[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K) (*V, error) {
	var (
		err         = bc.ensureClientConnection()
		oldValue    *wrapperspb.BytesValue
		bytesResult *[]byte
		binKey      []byte
		zeroValue   *V
		nearCache   = bc.nearCache
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

	if bc.session.GetProtocolVersion() > 0 {
		bytesResult, err = bc.session.v1StreamManagerCache.remove(newCtx, bc.name, binKey)
		if err != nil {
			return zeroValue, err
		}
		oldValue = ensureBytesValue(bytesResult)
	} else {
		removeRequest := pb.RemoveRequest{Key: binKey, Cache: bc.name, Format: bc.format, Scope: bc.sessionOpts.Scope}

		oldValue, err = bc.client.Remove(newCtx, &removeRequest)
		if err != nil {
			return zeroValue, err
		}
	}

	if bc.session.GetProtocolVersion() == 0 && nearCache != nil {
		nearCache.Remove(key)
	}

	return bc.valueSerializer.Deserialize(oldValue.Value)
}

// executeRemoveMapping executes the RemoveMapping operation against a baseClient.
func executeRemoveMapping[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V) (bool, error) {
	var (
		result    *wrapperspb.BoolValue
		err       = bc.ensureClientConnection()
		binKey    []byte
		binValue  []byte
		nearCache = bc.nearCache
		removed   bool
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

	if bc.session.GetProtocolVersion() > 0 {
		removed, err = bc.session.v1StreamManagerCache.removeMapping(newCtx, bc.name, binKey, binValue)
		if err != nil {
			return removed, err
		}
		result = &wrapperspb.BoolValue{Value: removed}
	} else {
		request := pb.RemoveMappingRequest{Cache: bc.name, Key: binKey, Value: binValue, Format: bc.format, Scope: bc.sessionOpts.Scope}

		result, err = bc.client.RemoveMapping(newCtx, &request)
		if err != nil {
			return false, err
		}
	}

	if bc.session.GetProtocolVersion() == 0 && result.Value && nearCache != nil {
		nearCache.Remove(key)
	}

	return result.Value, nil
}

// executeReplace executes the Replace operation against a baseClient.
func executeReplace[K comparable, V any](ctx context.Context, bc *baseClient[K, V], key K, value V) (*V, error) {
	var (
		err         = bc.ensureClientConnection()
		oldValue    *wrapperspb.BytesValue
		binKey      []byte
		binValue    []byte
		bytesResult *[]byte
		zeroValue   *V
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

	if bc.session.GetProtocolVersion() > 0 {
		bytesResult, err = bc.session.v1StreamManagerCache.replace(newCtx, bc.name, binKey, binValue)
		if err != nil {
			return zeroValue, err
		}
		oldValue = ensureBytesValue(bytesResult)
	} else {
		request := pb.ReplaceRequest{Key: binKey, Value: binValue, Cache: bc.name, Format: bc.format, Scope: bc.sessionOpts.Scope}

		oldValue, err = bc.client.Replace(newCtx, &request)
		if err != nil {
			return zeroValue, err
		}
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
		replaced     bool
		nearCache    = bc.nearCache
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

	if bc.session.GetProtocolVersion() > 0 {
		replaced, err = bc.session.v1StreamManagerCache.replaceMapping(newCtx, bc.name, binKey, binPrevValue, binNewValue)
		if err != nil {
			return replaced, err
		}
		result = &wrapperspb.BoolValue{Value: replaced}
	} else {
		request := pb.ReplaceMappingRequest{Cache: bc.name, Key: binKey, PreviousValue: binPrevValue,
			NewValue: binNewValue, Format: bc.format, Scope: bc.sessionOpts.Scope}

		result, err = bc.client.ReplaceMapping(newCtx, &request)
		if err != nil {
			return false, err
		}
	}

	if bc.session.GetProtocolVersion() == 0 && nearCache != nil && result.Value {
		if old := nearCache.Get(key); old != nil {
			nearCache.Put(key, newValue)
		}
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

	if bc.session.GetProtocolVersion() > 0 {
		size, err1 := bc.session.v1StreamManagerCache.size(newCtx, bc.name)
		if err1 != nil {
			return 0, err1
		}
		return int(size), nil
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

	if bc.session.GetProtocolVersion() > 0 {
		return bc.session.v1StreamManagerCache.isReady(newCtx, bc.name)
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
		err  = bc.ensureClientConnection()
		ch   = make(chan *StreamedEntry[K, V])
		iter entryPageIterator[K, V]
	)

	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		return ch
	}

	if bc.getProtocolVersion() > 0 {
		iter = newEntryPageIteratorV1[K, V](ctx, bc)
	} else {
		iter = newEntryPageIterator[K, V](ctx, bc)
	}

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

func executeEntrySetFilter[K comparable, V any, E any](ctx context.Context, bc *baseClient[K, V], fltr filters.Filter, comparator extractors.Comparator[E]) <-chan *StreamedEntry[K, V] {
	var (
		err           = bc.ensureClientConnection()
		binFilter     = make([]byte, 0)
		binComparator = make([]byte, 0)
		ch            = make(chan *StreamedEntry[K, V])
		serializer    = NewSerializer[any](bc.format)
	)

	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		return ch
	}

	newCtx, cancel := bc.session.ensureContext(ctx)

	if fltr == nil {
		fltr = filters.Always()
	}
	binFilter, err = serializer.Serialize(fltr)
	if err != nil {
		ch <- &StreamedEntry[K, V]{Err: err}
		return ch
	}

	if comparator != nil {
		if bc.session.GetProtocolVersion() == 0 {
			// feature not available in V0
			ch <- &StreamedEntry[K, V]{Err: fmt.Errorf("this feature is not available in this server version")}
			return ch
		}

		binComparator, err = serializer.Serialize(comparator)
		if err != nil {
			ch <- &StreamedEntry[K, V]{Err: err}
			return ch
		}
	}

	go func() {
		if cancel != nil {
			defer cancel()
		}

		if bc.session.GetProtocolVersion() > 0 {
			executeInvokeEntrySetFilterV1(ctx, bc, binFilter, binComparator, ch)
			close(ch)
			return
		}

		var (
			request = pb.EntrySetRequest{Cache: bc.name, Filter: binFilter,
				Format: bc.format, Scope: bc.sessionOpts.Scope, Comparator: binComparator}
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

			ch <- makeStreamedEntry[K, V](key, value, nil)
		}
	}()

	return ch
}

// executeValues executes the Values operation against a baseClient.
func executeValues[K comparable, V any, E any](ctx context.Context, bc *baseClient[K, V], fltr filters.Filter, comparator extractors.Comparator[E]) <-chan *StreamedValue[V] {
	var (
		err           = bc.ensureClientConnection()
		binFilter     = make([]byte, 0)
		binComparator = make([]byte, 0)
		ch            = make(chan *StreamedValue[V])
		serializer    = NewSerializer[any](bc.format)
	)

	if err != nil {
		ch <- &StreamedValue[V]{Err: err}
		return ch
	}

	newCtx, cancel := bc.session.ensureContext(ctx)

	if fltr == nil {
		fltr = filters.Always()
	}
	binFilter, err = serializer.Serialize(fltr)
	if err != nil {
		ch <- &StreamedValue[V]{Err: err}
		return ch
	}

	if comparator != nil {
		binComparator, err = serializer.Serialize(comparator)
		if err != nil {
			ch <- &StreamedValue[V]{Err: err}
			return ch
		}
	}

	go func() {
		if cancel != nil {
			defer cancel()
		}

		if bc.session.GetProtocolVersion() > 0 {
			executeValuesFilterV1(ctx, bc, binFilter, binComparator, ch)
			close(ch)
			return
		}
		request := pb.ValuesRequest{Cache: bc.name, Filter: binFilter,
			Format: bc.format, Scope: bc.sessionOpts.Scope, Comparator: binComparator}
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
		err  = bc.ensureClientConnection()
		ch   = make(chan *StreamedValue[V])
		iter valuePageIterator[K, V]
	)

	if err != nil {
		ch <- &StreamedValue[V]{Err: err}
		return ch
	}

	if bc.getProtocolVersion() > 0 {
		iter = newValuePageIteratorV1[K, V](ctx, bc)
	} else {
		iter = newValuePageIterator[K, V](ctx, bc)
	}

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

func (bc *baseClient[K, V]) getProtocolVersion() int32 {
	return bc.session.GetProtocolVersion()
}

// generateMapLifecycleEvent emits the [MapLifeCycleEvent] for v1 clients.
func (bc *baseClient[K, V]) generateMapLifecycleEvent(client interface{}, eventType MapLifecycleEventType) {
	if namedMap, ok := client.(NamedMap[K, V]); ok || client == nil {
		listeners := bc.lifecycleListenersV1
		event := newMapLifecycleEvent(namedMap, eventType)
		for _, l := range listeners {
			e := *l
			e.getEmitter().emit(eventType, event)
		}

		if eventType == Destroyed {
			executeRelease[K, V](bc, namedMap)
		}
	}
}

// generateMapEvent emits the [MapEvent] for v1 clients.
func (bc *baseClient[K, V]) generateMapEvent(client interface{}, eventResponse *pb1.MapEventMessage) {
	if namedMap, ok := client.(NamedMap[K, V]); ok {
		receivedMapEvent := newMapEventV1(namedMap, eventResponse)
		if eventResponse.Key != nil {
			key, err := bc.keySerializer.Deserialize(eventResponse.Key)
			if err != nil {
				logMessage(WARNING, "unable to deserialize key from eventResponse %v, ignoring eventResponse", eventResponse)
			} else {
				keyGroup, groupPresent := bc.keyListenersV1[*key]
				if groupPresent {
					keyGroup.notify(receivedMapEvent)
				}
			}
		}
		for _, id := range eventResponse.FilterIds {
			filterGroup, groupPresent := bc.filterIDToGroupV1[id]
			if groupPresent {
				filterGroup.notify(receivedMapEvent)
			}
		}
	}
}

// ensureClientConnection ensures the connection is established and
// ensures a NewNamedCacheServiceClient is present.
func (bc *baseClient[K, V]) ensureClientConnection() error {
	// ensure we have a valid connection
	var (
		session = bc.session
		err     error
	)
	if bc.destroyed {
		return ErrDestroyed
	}
	if bc.released {
		return ErrReleased
	}

	err = session.ensureConnection()
	if err != nil {
		return err
	}

	if bc.getProtocolVersion() == 0 {
		// ensure we have a NamedCacheServiceClient only if we are using v0
		if bc.client != nil {
			return nil
		}

		bc.client = pb.NewNamedCacheServiceClient(session.conn)
	}

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
