/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// EntryInserted this event indicates that an entry has been added to the cache.
	EntryInserted MapEventType = "insert"

	// EntryUpdated this event indicates that an entry has been updated in the cache.
	EntryUpdated MapEventType = "update"

	// EntryDeleted this event indicates that an entry has been removed from the cache.
	EntryDeleted MapEventType = "delete"

	// Destroyed raised when a storage for a given cache is destroyed
	// usually as a result of a call to NamedMap.Destroy().
	Destroyed MapLifecycleEventType = "map_destroyed"

	// Truncated raised when a storage for a given cache is truncated
	// as a result of a call to NamedMap.Truncate().
	Truncated MapLifecycleEventType = "map_truncated"

	// Released raised when the local resources for a cache has been released
	// as a result of a call to NamedMap.Release().
	Released MapLifecycleEventType = "map_released"

	// Connected raised when the session has connected.
	Connected SessionLifecycleEventType = "session_connected"

	// Disconnected raised when the session has disconnected.
	Disconnected SessionLifecycleEventType = "session_disconnected"

	// Reconnected raised when the session has re-connected.
	Reconnected SessionLifecycleEventType = "session_reconnected"

	// Closed raised when the session has been closed.
	Closed SessionLifecycleEventType = "session_closed"
)

// MapEventType describes an event raised by a cache mutation.
type MapEventType string

// MapLifecycleEventType describes an event that may be raised during the lifecycle
// of cache.
type MapLifecycleEventType string

// SessionLifecycleEventType describes an event that may be raised during the lifecycle
// of a Session.
type SessionLifecycleEventType string

var (
	// counter used to generate unique request identifiers.
	counter int32
	// filterCounter used to generate unique filter identifiers.
	filterCounter int64

	// defaultFilter the filter used when no key or filter is specified during
	// listener registration.
	defaultFilter filters.Filter = *filters.NewEventFilterFromFilter(filters.Always())

	_ SessionLifecycleListener             = &sessionLifecycleListener{}
	_ MapLifecycleListener[string, string] = &mapLifecycleListener[string, string]{}
)

type eventStream struct {
	grpcStream proto.NamedCacheService_EventsClient
	cancel     func()
}

// eventEmitter is used to register and emit events to said handlers.
// The type parameter 'L', defines the event label.
// Tye type parameter 'E', defines the event type.
type eventEmitter[L comparable, E any] struct {
	callbacks map[L][]func(E)
}

// newEventEmitter creates/initializes and returns a pointer to an eventEmitter.
func newEventEmitter[L comparable, E any]() *eventEmitter[L, E] {
	return &eventEmitter[L, E]{
		callbacks: map[L][]func(E){},
	}
}

// on register a callback to be notified when an event associated with
// the specified label is raised.
func (ee *eventEmitter[L, E]) on(label L, callback func(E)) {
	cbs, present := ee.callbacks[label]
	if !present {
		cbs = []func(E){}
	}
	result := append(cbs, callback)
	ee.callbacks[label] = result
}

// emit dispatches the specified event for all listeners associated with
// the specified label.
func (ee *eventEmitter[L, E]) emit(label L, event E) {
	for _, f := range ee.callbacks[label] {
		f(event)
	}
}

// SessionLifecycleEvent defines a session lifecycle event
type SessionLifecycleEvent interface {
	Type() SessionLifecycleEventType
	Source() *Session
}

type sessionLifecycleEvent struct {
	source    *Session
	eventType SessionLifecycleEventType
}

func newSessionLifecycleEvent(session *Session, eventType SessionLifecycleEventType) SessionLifecycleEvent {
	return &sessionLifecycleEvent{
		eventType: eventType,
		source:    session,
	}
}

func (se *sessionLifecycleEvent) Type() SessionLifecycleEventType {
	return se.eventType
}

func (se *sessionLifecycleEvent) Source() *Session {
	return se.source
}

func (se *sessionLifecycleEvent) String() string {
	return fmt.Sprintf("SessionLifecycleEvent{source=%v, format=%s}", se.Source(), se.Type())
}

type MapLifecycleEvent[K comparable, V any] interface {
	Source() NamedMap[K, V]
	Type() MapLifecycleEventType
}

func newMapLifecycleEvent[K comparable, V any](nm NamedMap[K, V], eventType MapLifecycleEventType) MapLifecycleEvent[K, V] {
	return &mapLifecycleEvent[K, V]{source: nm, eventType: eventType}
}

type mapLifecycleEvent[K comparable, V any] struct {
	// source the source of the event
	source NamedMap[K, V]

	// Type this event's MapLifecycleEventType
	eventType MapLifecycleEventType
}

// Type returns the MapLifecycleEventType for this MapLifecycleEvent.
func (l *mapLifecycleEvent[K, V]) Type() MapLifecycleEventType {
	return l.eventType
}

// Source returns the source of this MapLifecycleEvent.
func (l *mapLifecycleEvent[K, V]) Source() NamedMap[K, V] {
	return l.source
}

// String returns a string representation of a MapLifecycleEvent.
func (l *mapLifecycleEvent[K, V]) String() string {
	return fmt.Sprintf("MapLifecycleEvent{source=%v, type=%s}", l.Source(), l.Type())
}

// MapEvent an event which indicates that the content of the NamedMap or
// NamedCache has changed (i.e., an entry has been added, updated, and/or
// removed).
type MapEvent[K comparable, V any] interface {
	Source() NamedMap[K, V]
	Key() (*K, error)
	OldValue() (*V, error)
	NewValue() (*V, error)
	Type() MapEventType
}

// mapEvent struct containing data members to satisfy the MapEvent contract.
type mapEvent[K comparable, V any] struct {
	// source the source of the event
	source NamedMap[K, V]

	// Type this event's MapEventType
	eventType MapEventType

	// keyBytes the bytes for the event key
	keyBytes *[]byte

	// key the deserialized key.
	key *K

	// oldValueBytes the bytes for the old value, if any, associated with this key
	oldValueBytes *[]byte

	// oldValue the deserialized old value, if any, associated with this key
	oldValue *V

	// newValueBytes the bytes for the new value, if any, associated with this key
	newValueBytes *[]byte

	// newValue the deserialized new value, if any, associated with this key
	newValue *V
}

// newMapEvent creates and returns a pointer to a new mapEvent.
func newMapEvent[K comparable, V any](source NamedMap[K, V], response *proto.MapEventResponse) *mapEvent[K, V] {
	return &mapEvent[K, V]{
		source:        source,
		eventType:     eventTypeFromID(response.Id),
		keyBytes:      &response.Key,
		oldValueBytes: &response.OldValue,
		newValueBytes: &response.NewValue,
	}
}

// Key returns the key of the entry for which this event was raised.
func (e *mapEvent[K, V]) Key() (*K, error) {
	if e.key == nil {
		var source = e.source
		var serializer = source.getBaseClient().keySerializer
		keyDeser, err := serializer.Deserialize(*e.keyBytes)
		if err != nil {
			return nil, err
		}
		e.key = keyDeser
	}
	return e.key, nil
}

// OldValue returns the old value, if any, of the entry for which this event
// was raised.
func (e *mapEvent[K, V]) OldValue() (*V, error) {
	if e.oldValueBytes == nil {
		return nil, nil
	}
	if e.oldValue == nil {
		var source = e.source
		var serializer = source.getBaseClient().valueSerializer
		valDeser, err := serializer.Deserialize(*e.oldValueBytes)
		if err != nil {
			return nil, err
		}
		e.oldValue = valDeser
	}
	return e.oldValue, nil
}

// NewValue returns the new value, if any, of the entry for which this event
// was raised.
func (e *mapEvent[K, V]) NewValue() (*V, error) {
	if e.newValueBytes == nil {
		return nil, nil
	}
	if e.newValue == nil {
		source := e.source
		serializer := source.getBaseClient().valueSerializer
		valDeser, err := serializer.Deserialize(*e.newValueBytes)
		if err != nil {
			return nil, err
		}
		e.newValue = valDeser
	}
	return e.newValue, nil
}

// Type returns the MapEventType for this MapEvent.
func (e *mapEvent[K, V]) Type() MapEventType {
	return e.eventType
}

// Source returns the source of this MapEvent.
func (e *mapEvent[K, V]) Source() NamedMap[K, V] {
	return e.source
}

// String returns the string representation of this MapEvent.
func (e *mapEvent[K, V]) String() string {
	source := e.source
	key, keyErr := e.Key()
	oldValue, oldErr := e.OldValue()
	newValue, newErr := e.NewValue()

	keyEval := func(val *K, err error) any {
		if err != nil {
			return "error"
		}
		return *val
	}

	valueEval := func(val *V, err error) any {
		if err != nil {
			return "error"
		}
		if val == nil {
			return "nil"
		}
		return *val
	}
	return fmt.Sprintf("MapEvent{source=%v, name=%s, type=%s, key=%s, oldValue=%s, newValue=%s}",
		source, source.Name(), e.eventType, keyEval(key, keyErr), valueEval(oldValue, oldErr), valueEval(newValue, newErr))
}

type SessionLifecycleListener interface {
	OnAny(callback func(SessionLifecycleEvent)) SessionLifecycleListener
	OnConnected(callback func(SessionLifecycleEvent)) SessionLifecycleListener
	OnClosed(callback func(SessionLifecycleEvent)) SessionLifecycleListener
	OnDisconnected(callback func(SessionLifecycleEvent)) SessionLifecycleListener
	OnReconnected(callback func(SessionLifecycleEvent)) SessionLifecycleListener
	getEmitter() *eventEmitter[SessionLifecycleEventType, SessionLifecycleEvent]
}

type sessionLifecycleListener struct {
	emitter *eventEmitter[SessionLifecycleEventType, SessionLifecycleEvent]
}

func (sl *sessionLifecycleListener) getEmitter() *eventEmitter[SessionLifecycleEventType, SessionLifecycleEvent] { //lint:ignore U1000
	return sl.emitter
}

// NewSessionLifecycleListener creates and returns a pointer to a new SessionLifecycleListener instance.
func NewSessionLifecycleListener() SessionLifecycleListener {
	return &sessionLifecycleListener{newEventEmitter[SessionLifecycleEventType, SessionLifecycleEvent]()}
}

// on registers a callback for the specified MapEventType
func (sl *sessionLifecycleListener) on(event SessionLifecycleEventType, callback func(SessionLifecycleEvent)) SessionLifecycleListener {
	sl.emitter.on(event, callback)
	return sl
}

// OnConnected registers a callback that will be notified when a Session is connected.
func (sl *sessionLifecycleListener) OnConnected(callback func(SessionLifecycleEvent)) SessionLifecycleListener {
	return sl.on(Connected, callback)
}

// OnDisconnected registers a callback that will be notified when a Session is disconnected.
func (sl *sessionLifecycleListener) OnDisconnected(callback func(SessionLifecycleEvent)) SessionLifecycleListener {
	return sl.on(Disconnected, callback)
}

// OnReconnected registers a callback that will be notified when a Session is reconnected.
func (sl *sessionLifecycleListener) OnReconnected(callback func(SessionLifecycleEvent)) SessionLifecycleListener {
	return sl.on(Reconnected, callback)
}

// OnClosed registers a callback that will be notified when a Session is closed.
func (sl *sessionLifecycleListener) OnClosed(callback func(SessionLifecycleEvent)) SessionLifecycleListener {
	return sl.on(Closed, callback)
}

// OnAny registers a callback that will be notified when a Session is connected, disconnected, reconnected or closed.
func (sl *sessionLifecycleListener) OnAny(callback func(SessionLifecycleEvent)) SessionLifecycleListener {
	return sl.on(Closed, callback).OnConnected(callback).OnDisconnected(callback).OnReconnected(callback)
}

// MapLifecycleListener allows registering callbacks to be notified when lifecycle events
// (truncated or released) occur against a NamedMap or NamedCache.
type MapLifecycleListener[K comparable, V any] interface {
	OnAny(callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V]
	OnDestroyed(callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V]
	OnTruncated(callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V]
	OnReleased(callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V]
	getEmitter() *eventEmitter[MapLifecycleEventType, MapLifecycleEvent[K, V]]
}

type mapLifecycleListener[K comparable, V any] struct { //lint:ignore U1000 - required due to linter issues with generics
	emitter *eventEmitter[MapLifecycleEventType, MapLifecycleEvent[K, V]]
}

func (l *mapLifecycleListener[K, V]) getEmitter() *eventEmitter[MapLifecycleEventType, MapLifecycleEvent[K, V]] {
	return l.emitter
}

// NewMapLifecycleListener creates and returns a pointer to a new MapLifecycleListener instance.
func NewMapLifecycleListener[K comparable, V any]() MapLifecycleListener[K, V] {
	return &mapLifecycleListener[K, V]{newEventEmitter[MapLifecycleEventType, MapLifecycleEvent[K, V]]()}
}

// on registers a callback for the specified MapEventType
func (l *mapLifecycleListener[K, V]) on(event MapLifecycleEventType, callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V] {
	l.emitter.on(event, callback)
	return l
}

// OnDestroyed registers a callback that will be notified when a NamedMap is destroyed.
func (l *mapLifecycleListener[K, V]) OnDestroyed(callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V] {
	return l.on(Destroyed, callback)
}

// OnTruncated registers a callback that will be notified when a NamedMap is truncated.
func (l *mapLifecycleListener[K, V]) OnTruncated(callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V] {
	return l.on(Truncated, callback)
}

// OnReleased registers a callback that will be notified when a NamedMap is released.
func (l *mapLifecycleListener[K, V]) OnReleased(callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V] {
	return l.on(Released, callback)
}

// OnAny registers a callback that will be notified when any NamedMap event occurs.
func (l *mapLifecycleListener[K, V]) OnAny(callback func(MapLifecycleEvent[K, V])) MapLifecycleListener[K, V] {
	return l.OnTruncated(callback).OnDestroyed(callback).OnReleased(callback)
}

// MapListener allows registering callbacks to be notified when mutations events
// occur within a NamedMap or NamedCache.
type MapListener[K comparable, V any] interface {
	OnInserted(callback func(MapEvent[K, V])) MapListener[K, V]
	OnUpdated(callback func(MapEvent[K, V])) MapListener[K, V]
	OnDeleted(callback func(MapEvent[K, V])) MapListener[K, V]
	OnAny(callback func(MapEvent[K, V])) MapListener[K, V]
	dispatch(event MapEvent[K, V])
}

// mapListener struct containing data members to satisfy the MapListener contract.
type mapListener[K comparable, V any] struct {
	emitter *eventEmitter[MapEventType, MapEvent[K, V]]
}

// NewMapListener creates and returns a pointer to a new MapListener instance.
func NewMapListener[K comparable, V any]() MapListener[K, V] {
	return &mapListener[K, V]{newEventEmitter[MapEventType, MapEvent[K, V]]()}
}

// dispatch dispatches the specified event to the appropriate group of listeners.
func (l *mapListener[K, V]) dispatch(event MapEvent[K, V]) { //nolint
	l.emitter.emit(event.Type(), event)
}

// on registers a callback for the specified MapEventType
func (l *mapListener[K, V]) on(event MapEventType, callback func(MapEvent[K, V])) MapListener[K, V] {
	l.emitter.on(event, callback)
	return l
}

// OnInserted registers a callback that will be notified when an entry is inserted.
func (l *mapListener[K, V]) OnInserted(callback func(MapEvent[K, V])) MapListener[K, V] {
	return l.on(EntryInserted, callback)
}

// OnUpdated registers a callback that will be notified when an entry is updated.
func (l *mapListener[K, V]) OnUpdated(callback func(MapEvent[K, V])) MapListener[K, V] {
	return l.on(EntryUpdated, callback)
}

// OnDeleted registers a callback that will be notified when an entry is deleted.
func (l *mapListener[K, V]) OnDeleted(callback func(MapEvent[K, V])) MapListener[K, V] {
	return l.on(EntryDeleted, callback)
}

// OnAny registers a callback that will be notified when any entry mutation has occurred.
func (l *mapListener[K, V]) OnAny(callback func(MapEvent[K, V])) MapListener[K, V] {
	return l.OnInserted(callback).OnUpdated(callback).OnDeleted(callback)
}

// listenerGroup is a group of similar listeners registered to the same key
// or filter.  In effect, this means only one network operations would be
// required to register/deregister listeners in the same group.
type listenerGroup[K comparable, V any] struct {
	registeredLite  bool
	listeners       map[MapListener[K, V]]bool
	liteFalseCount  int32
	manager         *mapEventManager[K, V]
	request         proto.MapListenerRequest
	postSubscribe   func()
	postUnsubscribe func()
}

// addListener registers the specified MapListener to this group.  The lite
// flag is a hint to the Coherence cluster that an event may omit the old
// and new values when emitting a MapEvent.
func (lg *listenerGroup[K, V]) addListener(ctx context.Context, listener MapListener[K, V], lite bool) error {
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
		if size > 1 {
			if err := lg.unsubscribe(ctx); err != nil {
				return err
			}
		}
		return lg.subscribe(ctx, lite)
	}
	return nil
}

// removeListener removes the specified listener from this group.  If this
// results in no remaining registered listeners, the listener will be
// unregistered from the Coherence cluster.
func (lg *listenerGroup[K, V]) removeListener(ctx context.Context, listener MapListener[K, V]) error {
	prevLiteStatus, hasKey := lg.listeners[listener]
	if !hasKey || len(lg.listeners) == 0 {
		return nil
	}

	delete(lg.listeners, listener)

	if len(lg.listeners) == 0 {
		return lg.unsubscribe(ctx)
	}

	if !prevLiteStatus {
		atomic.AddInt32(&lg.liteFalseCount, -1)

		if lg.liteFalseCount == 0 {
			if err := lg.unsubscribe(ctx); err != nil {
				return err
			}
			return lg.subscribe(ctx, true)
		}
	}
	return nil
}

// notify notifies all listeners of the provided MapEvent.
func (lg *listenerGroup[K, V]) notify(event MapEvent[K, V]) {
	if len(lg.listeners) > 0 {
		for l := range lg.listeners {
			l.dispatch(event)
		}
	}
}

// write sends the MapListenerRequest for this group to the
// Coherence cluster.  This may be either a registration or
// de-registration operation.
func (lg *listenerGroup[K, V]) write(request *proto.MapListenerRequest) error {
	stream, err := lg.manager.ensureStream()
	if err != nil {
		return err
	}
	err = stream.grpcStream.SendMsg(request)
	if err != nil {
		return err
	}
	return nil
}

// subscribe this group.  The lite flag is a hint to the Coherence cluster
// that an event may omit the old and new values when emitting a MapEvent.
func (lg *listenerGroup[K, V]) subscribe(ctx context.Context, lite bool) error {
	lg.request.Lite = lite
	lg.request.Subscribe = true

	ctxOp, cancel := context.WithCancel(ctx)
	lg.manager.pendingRegistrations[lg.request.Uid] = &pendingListenerOp[K, V]{cancel, lg}
	err := lg.write(&lg.request)
	if err != nil {
		return err
	}

	<-ctxOp.Done()
	if errInner := ctxOp.Err(); errInner != nil && errInner == context.DeadlineExceeded {
		err = errInner
	}

	delete(lg.manager.pendingRegistrations, lg.request.Uid)

	if err == nil {
		lg.postSubscribe()
	}

	return err
}

// unsubscribe unsubscribes this group.
func (lg *listenerGroup[K, V]) unsubscribe(ctx context.Context) error {
	lg.request.Subscribe = false
	ctxOp, cancel := context.WithCancel(ctx)
	lg.manager.pendingRegistrations[lg.request.Uid] = &pendingListenerOp[K, V]{cancel, lg}
	err := lg.write(&lg.request)
	if err != nil {
		return err
	}

	<-ctxOp.Done()
	if errInner := ctxOp.Err(); errInner != nil && errInner == context.DeadlineExceeded {
		err = errInner
	}

	if err == nil {
		lg.postUnsubscribe()
	}

	return err
}

// makeGeneralListenerGroup creates and returns a pointer to a new ListenerGroup
// with common settings applied to the returned instance.
func makeGeneralListenerGroup[K comparable, V any](manager *mapEventManager[K, V]) *listenerGroup[K, V] {
	group := listenerGroup[K, V]{}
	group.manager = manager
	group.listeners = map[MapListener[K, V]]bool{}
	group.liteFalseCount = 0
	group.registeredLite = false

	return &group
}

// makeKeyListenerGroup creates and returns a pointer to a new ListenerGroup
// for a key listener.
func makeKeyListenerGroup[K comparable, V any](manager *mapEventManager[K, V], key K) (*listenerGroup[K, V], error) {
	group := makeGeneralListenerGroup(manager)
	group.request = manager.newSubscribeRequest("key")

	serializedKey, err := manager.serializer.Serialize(key)
	if err != nil {
		return nil, err
	}

	group.request.Key = serializedKey
	group.request.Type = proto.MapListenerRequest_KEY

	group.postSubscribe = func() {
		manager.keyListeners[key] = group
	}
	group.postUnsubscribe = func() {
		delete(manager.keyListeners, key)
	}

	return group, nil
}

// makeFilterListenerGroup creates and returns a pointer to a new ListenerGroup
// for a filter listener.
func makeFilterListenerGroup[K comparable, V any](manager *mapEventManager[K, V], filter filters.Filter) (*listenerGroup[K, V], error) {
	group := makeGeneralListenerGroup(manager)
	group.request = manager.newSubscribeRequest("filter")
	group.request.FilterId = nextFilterID()

	filterLocal := filter
	hasSuffix := strings.HasSuffix(reflect.TypeOf(filter).String(), "MapEventFilter")
	if !hasSuffix {
		filterLocal = filters.NewEventFilterFromFilter(filter)
	}

	serializedFilter, err := manager.serializer.Serialize(filterLocal)
	if err != nil {
		return nil, err
	}

	group.request.Filter = serializedFilter
	group.request.Type = proto.MapListenerRequest_FILTER

	group.postSubscribe = func() {
		manager.filterListeners[filterLocal] = group
		manager.filterIDToGroup[group.request.FilterId] = group
	}
	group.postUnsubscribe = func() {
		delete(manager.filterListeners, filterLocal)
		delete(manager.filterIDToGroup, group.request.FilterId)
	}

	return group, nil
}

// mapEventManager is responsible for managing ListenerGroups and the
// gRPC event stream that is used to send MapListenerRequests and process
// MapListenerResponses.
type mapEventManager[K comparable, V any] struct {
	bc                   baseClient[K, V]
	namedMap             *NamedMap[K, V]
	session              *Session
	serializer           Serializer[any]
	keyListeners         map[K]*listenerGroup[K, V]
	filterListeners      map[filters.Filter]*listenerGroup[K, V]
	filterIDToGroup      map[int64]*listenerGroup[K, V]
	lifecycleListeners   []*MapLifecycleListener[K, V]
	pendingRegistrations map[string]*pendingListenerOp[K, V]
	eventStream          *eventStream
	sync.RWMutex
}

// pendingListenerOp is a simple holder for the listener
// registration/de-registration context and the listener group
// being registered/de-registered
type pendingListenerOp[K comparable, V any] struct {
	cancel        func()
	listenerGroup *listenerGroup[K, V]
}

// newMapEventManager creates/initializes and returns a pointer to a new
// mapEventManager
func newMapEventManager[K comparable, V any](namedMap *NamedMap[K, V], bc baseClient[K, V], session *Session) (*mapEventManager[K, V], error) {
	manager := mapEventManager[K, V]{
		bc:                   bc,
		namedMap:             namedMap,
		session:              session,
		serializer:           NewSerializer[any]("json"),
		keyListeners:         map[K]*listenerGroup[K, V]{},
		filterListeners:      map[filters.Filter]*listenerGroup[K, V]{},
		filterIDToGroup:      map[int64]*listenerGroup[K, V]{},
		lifecycleListeners:   []*MapLifecycleListener[K, V]{},
		pendingRegistrations: map[string]*pendingListenerOp[K, V]{},
	}

	_, err := manager.ensureStream()
	if err != nil {
		return nil, err
	}
	return &manager, nil
}

// close closes the event stream.
func (m *mapEventManager[K, V]) close() {
	if m.eventStream != nil {
		m.eventStream.cancel()
	}
	m.eventStream = nil
	m.keyListeners = nil
	m.filterListeners = nil
	m.filterIDToGroup = nil
	m.pendingRegistrations = nil
	m.serializer = nil
	m.session = nil
	m.namedMap = nil
}

// addLifecycleListener adds the specified MapLifecycleListener.
func (m *mapEventManager[K, V]) addLifecycleListener(listener MapLifecycleListener[K, V]) {
	for _, e := range m.lifecycleListeners {
		if *e == listener {
			return
		}
	}
	m.lifecycleListeners = append(m.lifecycleListeners, &listener)
}

// removeLifecycleListener removes the specified MapLifecycleListener.
func (m *mapEventManager[K, V]) removeLifecycleListener(listener MapLifecycleListener[K, V]) {
	idx := -1
	listeners := m.lifecycleListeners
	for i, c := range listeners {
		if *c == listener {
			idx = i
			break
		}
	}
	if idx != -1 {
		result := append(listeners[:idx], listeners[idx+1:]...)
		m.lifecycleListeners = result
	}
}

// addKeyListener adds a new key-based MapListener.  The lite flag is a hint
// to the Coherence cluster that an event may omit the old and new
// values when emitting a MapEvent.
func (m *mapEventManager[K, V]) addKeyListener(ctx context.Context, listener MapListener[K, V], key K, lite bool) error {
	group, lPresent := m.keyListeners[key]
	if !lPresent {
		groupInner, err := makeKeyListenerGroup(m, key)
		if err != nil {
			return err
		}
		m.keyListeners[key] = groupInner
		group = groupInner
	}

	return group.addListener(ctx, listener, lite)
}

// removeKeyListener removes the specified key-based listener.
func (m *mapEventManager[K, V]) removeKeyListener(ctx context.Context, listener MapListener[K, V], key K) error {
	group, lPresent := m.keyListeners[key]
	if lPresent {
		return group.removeListener(ctx, listener)
	}
	return nil
}

// addFilterListener adds a new filter-based MapListener.  The lite flag is a hint
// to the Coherence cluster that an event may omit the old and new
// values when emitting a MapEvent.
func (m *mapEventManager[K, V]) addFilterListener(ctx context.Context, listener MapListener[K, V], filter filters.Filter, lite bool) error {
	filterLocal := filter
	if filterLocal == nil {
		filterLocal = defaultFilter
	}

	group, lPresent := m.filterListeners[filterLocal]
	if !lPresent {
		groupInner, err := makeFilterListenerGroup(m, filterLocal)
		if err != nil {
			return err
		}
		m.filterListeners[filterLocal] = groupInner
		group = groupInner
	}

	return group.addListener(ctx, listener, lite)
}

// removeFilterListener removes the specified filter-based listener.
func (m *mapEventManager[K, V]) removeFilterListener(ctx context.Context, listener MapListener[K, V], filter filters.Filter) error {
	filterLocal := filter
	if filterLocal == nil {
		filterLocal = defaultFilter
	}

	group, lPresent := m.filterListeners[filterLocal]
	if lPresent {
		return group.removeListener(ctx, listener)
	}
	return nil
}

// ensureStream initializes the event stream and starts a goroutine for
// managing MapEvents raised by Coherence.
func (m *mapEventManager[K, V]) ensureStream() (*eventStream, error) {
	if m.eventStream == nil {
		// because the event stream is for the lifetime of the cache,
		// we use context.Background() and ignore any user provided
		// timeouts
		ctx, cancel := context.WithCancel(context.Background())
		grpcStream, err := m.bc.client.Events(ctx)
		if err != nil {
			cancel()
			return nil, err
		}

		m.eventStream = &eventStream{grpcStream, cancel}
		request := m.newSubscribeRequest("init")
		request.Type = proto.MapListenerRequest_INIT
		err = grpcStream.Send(&request)
		if err != nil {
			log.Printf("event stream send failed: %s\n", err)
			cancel()
			return nil, err
		}
		// goroutine to handle MapEventResponse instances returned
		// from event stream
		go func() {
			for {
				response, err := grpcStream.Recv()
				if err == io.EOF {
					return
				}
				if err != nil {
					statusLocal := status.Code(err)
					if statusLocal != codes.Canceled {
						// only log if it's not a cancelled error as this is just the client closing
						log.Printf("event stream recv failed: %s\n", err)
					}
					cancel()
					return
				}
				switch response.ResponseType.(type) {
				case *proto.MapListenerResponse_Event:
					{
						eventResponse := response.GetEvent()
						var nm = *m.namedMap
						receivedMapEvent := newMapEvent(nm, eventResponse)
						for _, id := range eventResponse.FilterIds {
							filterGroup, groupPresent := m.filterIDToGroup[id]
							if groupPresent {
								filterGroup.notify(receivedMapEvent)
							}
						}

						key, err := receivedMapEvent.Key()
						if err != nil {
							fmt.Printf("Unable to deserialize event key: %s", err)
							cancel()
							return
						}
						keyGroup, groupPresent := m.keyListeners[*key]
						if groupPresent {
							keyGroup.notify(receivedMapEvent)
						}
					}
				case *proto.MapListenerResponse_Subscribed:
					{
						uid := response.GetSubscribed().GetUid()
						regOp, groupPresent := m.pendingRegistrations[uid]
						if groupPresent {
							regOp.cancel()
						}
					}
				case *proto.MapListenerResponse_Unsubscribed:
					{
						uid := response.GetUnsubscribed().GetUid()
						regOp, groupPresent := m.pendingRegistrations[uid]
						if groupPresent {
							regOp.cancel()
						}
					}
				case *proto.MapListenerResponse_Truncated:
					{
						nm := *m.namedMap
						listeners := nm.getBaseClient().eventManager.lifecycleListeners
						event := newMapLifecycleEvent(nm, Truncated)
						for _, l := range listeners {
							e := *l
							e.getEmitter().emit(Truncated, event)
						}
					}
				case *proto.MapListenerResponse_Destroyed:
					{
						if m.namedMap == nil {
							// namedMap is already destroyed, cannot do anything
							cancel()
							return
						}
						nm := *m.namedMap
						listeners := nm.getBaseClient().eventManager.lifecycleListeners
						event := newMapLifecycleEvent(nm, Destroyed)
						for _, l := range listeners {
							e := *l
							e.getEmitter().emit(Destroyed, event)
						}
					}
				case *proto.MapListenerResponse_Error:
				}
			}
		}() // END event stream read
	}

	return m.eventStream, nil
}

// newSubscribeRequest creates/initializes and returns a proto.MapListenerRequest.
func (m *mapEventManager[K, V]) newSubscribeRequest(requestType string) proto.MapListenerRequest {
	return proto.MapListenerRequest{
		Subscribe: true,
		Trigger:   []byte{},
		Scope:     m.bc.sessionOpts.Scope,
		Priming:   false,
		Format:    m.serializer.Format(),
		Cache:     m.bc.name,
		Uid:       generateRequestID(requestType, m.bc.name, m.bc.sessionOpts.Scope),
	}
}

func (m *mapEventManager[K, V]) dispatch(eventType MapLifecycleEventType,
	creator func() MapLifecycleEvent[K, V]) {
	if len(m.lifecycleListeners) > 0 {
		event := creator()
		for _, l := range m.lifecycleListeners {
			e := *l
			e.getEmitter().emit(eventType, event)
		}
	}
}

type saveListener[K comparable, V any] struct {
	listener MapListener[K, V]
	lite     bool
}

// reRegisterListeners re-registers listeners on reconnect session.
func reRegisterListeners[K comparable, V any](ctx context.Context, namedMap *NamedMap[K, V], bc *baseClient[K, V]) error {
	var (
		err   error
		debug = bc.session.debug
	)

	// save the key and filter listeners as we need to close the eventManager
	keyListeners := make(map[K]saveListener[K, V], 0)
	filterListeners := make(map[filters.Filter]saveListener[K, V], 0)

	for k, lg := range bc.eventManager.keyListeners {
		for l, lite := range lg.listeners {
			keyListeners[k] = saveListener[K, V]{lite: lite, listener: l}
		}
	}

	for f, lg := range bc.eventManager.filterListeners {
		for l, lite := range lg.listeners {
			filterListeners[f] = saveListener[K, V]{lite: lite, listener: l}
		}
	}

	// destroy the event manager and create a new one
	bc.eventManager.close()
	bc.eventManager, err = newMapEventManager[K, V](namedMap, *bc, bc.session)
	if err != nil {
		return err
	}

	// re-register key listeners
	for k, save := range keyListeners {
		debug(fmt.Sprintf("re-registering listener %v for key%v", save.listener, k))
		if err = bc.eventManager.addKeyListener(ctx, save.listener, k, save.lite); err != nil {
			return fmt.Errorf("unable to re-register listener %v for key%v - %v", k, save.listener, err)
		}
	}

	// re-register filter listeners
	for f, save := range filterListeners {
		debug(fmt.Sprintf("re-registering listener %v for filter %v", save.listener, f))
		if err = bc.eventManager.addFilterListener(ctx, save.listener, f, save.lite); err != nil {
			return fmt.Errorf("unable to add filter listener %v for filter %v - %v", f, save.listener, err)
		}
	}

	return nil
}

// generateRequestID generates a unique request ID for a listener registration.
func generateRequestID(prefix string, cacheName string, scope string) string {
	nextID := atomic.AddInt32(&counter, 1)
	now := time.Now()
	return prefix + scope + "-" + cacheName + "-" + fmt.Sprint(now.UnixMilli()) + fmt.Sprint(nextID)
}

// nextFilterID returns a monotonically increasing filter identifier.
func nextFilterID() int64 {
	return atomic.AddInt64(&filterCounter, 1)
}

// eventTypeFromID returns the MapEventType from an int32 identifier.
func eventTypeFromID(id int32) MapEventType {
	switch id {
	case 1:
		return EntryInserted
	case 2:
		return EntryUpdated
	case 3:
		return EntryDeleted
	default:
		fmt.Printf("Unknown event id %s", string(id))
		return ""
	}
}
