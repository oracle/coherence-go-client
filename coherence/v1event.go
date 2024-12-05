/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	pb1 "github.com/oracle/coherence-go-client/v2/proto/v1"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

// listenerGroupV1 is a group of similar listeners registered to the same key
// or filter.
type listenerGroupV1[K comparable, V any] struct {
	registeredLite  bool
	mutex           sync.RWMutex
	listeners       map[MapListener[K, V]]bool
	liteFalseCount  int32
	streamManager   *streamManagerV1
	baseClient      *baseClient[K, V]
	key             []byte
	fltr            []byte
	filterID        int64
	postSubscribe   func()
	postUnsubscribe func()
}

// makeGeneralListenerGroupV1 creates and returns a pointer to a new ListenerGroup
// with common settings applied to the returned instance.
func makeGeneralListenerGroupV1[K comparable, V any](streamManager *streamManagerV1, bc *baseClient[K, V]) *listenerGroupV1[K, V] {
	group := listenerGroupV1[K, V]{}
	group.streamManager = streamManager
	group.listeners = map[MapListener[K, V]]bool{}
	group.liteFalseCount = 0
	group.registeredLite = false
	group.baseClient = bc

	return &group
}

// makeKeyListenerGroupV1 creates and returns a pointer to a new ListenerGroup
// for a key listener.
func makeKeyListenerGroupV1[K comparable, V any](streamManager *streamManagerV1, bc *baseClient[K, V], key K) (*listenerGroupV1[K, V], error) {
	group := makeGeneralListenerGroupV1[K, V](streamManager, bc)

	serializedKey, err := bc.keySerializer.Serialize(key)
	if err != nil {
		return nil, err
	}

	group.key = serializedKey

	group.postSubscribe = func() {
		bc.keyListenersV1[key] = group
	}
	group.postUnsubscribe = func() {
		delete(bc.keyListenersV1, key)
	}

	return group, nil
}

func makeFilterListenerGroupV1[K comparable, V any](streamManager *streamManagerV1, bc *baseClient[K, V], filter filters.Filter) (*listenerGroupV1[K, V], error) {
	group := makeGeneralListenerGroupV1[K, V](streamManager, bc)
	group.filterID = nextFilterID()

	filterLocal := filter
	hasSuffix := strings.HasSuffix(reflect.TypeOf(filter).String(), "MapEventFilter")
	if !hasSuffix {
		filterLocal = filters.NewEventFilterFromFilter(filter)
	}

	serializedFilter, err := streamManager.serializer.Serialize(filterLocal)
	if err != nil {
		return nil, err
	}

	group.fltr = serializedFilter

	group.postSubscribe = func() {
		bc.filterListenersV1[filterLocal] = group
		bc.filterIDToGroupV1[group.filterID] = group
	}
	group.postUnsubscribe = func() {
		delete(bc.filterListenersV1, filterLocal)
		delete(bc.filterIDToGroupV1, group.filterID)
	}

	return group, nil
}

// addKeyListenerInternalV1 adds a key listener for gRPC v1 protocol.
func addKeyListenerInternalV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V], listener MapListener[K, V], key K, lite bool) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	group, lPresent := bc.keyListenersV1[key]
	if !lPresent {
		groupInner, err := makeKeyListenerGroupV1(bc.session.v1StreamManagerCache, bc, key)
		if err != nil {
			return err
		}

		bc.keyListenersV1[key] = groupInner
		group = groupInner
	}

	return group.addListener(ctx, listener, lite)
}

func addFilterListenerInternalV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V], listener MapListener[K, V], filter filters.Filter, lite bool) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	filterLocal := filter
	if filterLocal == nil {
		filterLocal = defaultFilter
	}

	group, lPresent := bc.filterListenersV1[filterLocal]
	if !lPresent {
		groupInner, err := makeFilterListenerGroupV1(bc.session.v1StreamManagerCache, bc, filterLocal)
		if err != nil {
			return err
		}
		bc.filterListenersV1[filterLocal] = groupInner
		group = groupInner
	}

	return group.addListener(ctx, listener, lite)
}

// removeFilterListenerInternalV1 removes the specified filter-based listener.
func removeFilterListenerInternalV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V], listener MapListener[K, V], filter filters.Filter) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	filterLocal := filter
	if filterLocal == nil {
		filterLocal = defaultFilter
	}

	group, lPresent := bc.filterListenersV1[filterLocal]
	if lPresent {
		return group.removeListener(ctx, listener)
	}
	return nil
}

// removeKeyListenerInternalV1 removes a key listener for gRPC v1 protocol.
func removeKeyListenerInternalV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V], listener MapListener[K, V], key K) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	group, lPresent := bc.keyListenersV1[key]
	if lPresent {
		return group.removeListener(ctx, listener)
	}
	return nil
}

func (lg *listenerGroupV1[K, V]) addListener(ctx context.Context, listener MapListener[K, V], lite bool) error {
	lg.mutex.Lock()
	defer lg.mutex.Unlock()

	prevLiteStatus, hasKey := lg.listeners[listener]
	if hasKey && prevLiteStatus == lite {
		return nil
	}

	lg.listeners[listener] = lite

	if !lite {
		atomic.AddInt32(&lg.liteFalseCount, 1)
	}

	size := len(lg.listeners)
	requiresRegistration := size == 1 || lg.registeredLite && !lite

	if requiresRegistration {
		lg.registeredLite = lite

		keyOrFilter := ensureKeyOrFilterGrpcV1(lg.key, lg.fltr)

		if size > 1 {
			// unsubscribe the key and then re-subscribe
			err := lg.streamManager.mapListenerRequest(ctx, lg.baseClient.name, false, keyOrFilter, lite, listener.IsSynchronous(), listener.IsPriming(), lg.filterID)
			if err != nil {
				return err
			}
		}

		err := lg.streamManager.mapListenerRequest(ctx, lg.baseClient.name, true, keyOrFilter, lite, listener.IsSynchronous(), listener.IsPriming(), lg.filterID)
		if err != nil {
			return err
		}
		lg.postSubscribe()
	}
	return nil
}

func (lg *listenerGroupV1[K, V]) removeListener(ctx context.Context, listener MapListener[K, V]) error {
	lg.mutex.Lock()
	defer lg.mutex.Unlock()

	prevLiteStatus, hasKey := lg.listeners[listener]
	if !hasKey || len(lg.listeners) == 0 {
		return nil
	}

	delete(lg.listeners, listener)

	keyOrFilter := ensureKeyOrFilterGrpcV1(lg.key, lg.fltr)

	if len(lg.listeners) == 0 {
		return lg.streamManager.mapListenerRequest(ctx, lg.baseClient.name, false, keyOrFilter, prevLiteStatus, false, false, lg.filterID)
	}

	if !prevLiteStatus {
		atomic.AddInt32(&lg.liteFalseCount, -1)

		if lg.liteFalseCount == 0 {
			if err := lg.streamManager.mapListenerRequest(ctx, lg.baseClient.name, false, keyOrFilter, prevLiteStatus, false, false, lg.filterID); err != nil {
				return err
			}
			return lg.streamManager.mapListenerRequest(ctx, lg.baseClient.name, true, keyOrFilter, prevLiteStatus, false, false, lg.filterID)
		}
	}
	return nil
}

// notify notifies all listeners of the provided [MapEvent].
func (lg *listenerGroupV1[K, V]) notify(event MapEvent[K, V]) {
	lg.mutex.Lock()
	defer lg.mutex.Unlock()

	if len(lg.listeners) > 0 {
		for l := range lg.listeners {
			l.dispatch(event)
		}
	}
}

// newMapEvent creates and returns a pointer to a new [MapEvent].
func newMapEventV1[K comparable, V any](source NamedMap[K, V], response *pb1.MapEventMessage) *mapEvent[K, V] {
	return &mapEvent[K, V]{
		source:        source,
		eventType:     eventTypeFromID(response.Id),
		keyBytes:      &response.Key,
		oldValueBytes: &response.OldValue,
		newValueBytes: &response.NewValue,
		isgRPCv1:      true,
		isExpired:     response.Expired,
		isSynthetic:   response.Synthetic,
		isPriming:     response.Priming,
	}
}
