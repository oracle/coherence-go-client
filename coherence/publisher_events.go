/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"fmt"
)

const (
	PublisherConnected     PublisherLifecycleEventType = "publisher_connected"
	PublisherDisconnected  PublisherLifecycleEventType = "publisher_disconnected"
	PublisherReleased      PublisherLifecycleEventType = "publisher_released"
	PublisherDestroyed     PublisherLifecycleEventType = "publisher_destroyed"
	PublisherChannelsFreed PublisherLifecycleEventType = "publisher_channels_freed"
)

type PublisherLifecycleEventType string

type PublisherLifecycleEvent[V any] interface {
	Source() Publisher[V]
	Type() PublisherLifecycleEventType
}

type publisherLifecycleEvent[V any] struct {
	// source the source of the event
	source Publisher[V]

	// Type of this event's PublisherLifecycleEventType
	eventType PublisherLifecycleEventType
}

// Type returns the [PublisherLifecycleEventType] for this [PublisherLifecycleEvent].
func (l *publisherLifecycleEvent[V]) Type() PublisherLifecycleEventType {
	return l.eventType
}

// Source returns the source of this [PublisherLifecycleEvent].
func (l *publisherLifecycleEvent[V]) Source() Publisher[V] {
	return l.source
}

// String returns a string representation of a [PublisherLifecycleEvent]..
func (l *publisherLifecycleEvent[V]) String() string {
	return fmt.Sprintf("spublisherLifecycleEvent{source=%v, type=%s}", l.Source(), l.Type())
}

// PublisherLifecycleListener allows registering callbacks to be notified when lifecycle events
// occur against a [Publisher].
type PublisherLifecycleListener[V any] interface {
	// OnAny registers a callback that will be notified when any [Publisher] event occurs.
	OnAny(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V]

	// OnDestroyed registers a callback that will be notified when a [Publisher] is destroyed.
	OnDestroyed(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V]

	// OnReleased registers a callback that will be notified when a [Publisher] is released.
	OnReleased(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V]

	// OnConnected registers a callback that will be notified when a [Publisher] is disconnected.
	OnConnected(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V]

	// OnDisconnected registers a callback that will be notified when a [Publisher] is unsubscribed.
	OnDisconnected(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V]

	// OnChannelsFreed registers a callback that will be notified when a [Publisher] has channels freed.
	OnChannelsFreed(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V]

	getEmitter() *eventEmitter[PublisherLifecycleEventType, PublisherLifecycleEvent[V]]
}

// NewPublisherLifecycleListener creates and returns a pointer to a new [PublisherLifecycleListener] instance.
func NewPublisherLifecycleListener[V any]() PublisherLifecycleListener[V] {
	return &publisherLifecycleListener[V]{newEventEmitter[PublisherLifecycleEventType, PublisherLifecycleEvent[V]]()}
}

type publisherLifecycleListener[V any] struct { //lint:ignore U1000 - required due to linter issues with generics
	emitter *eventEmitter[PublisherLifecycleEventType, PublisherLifecycleEvent[V]]
}

// OnAny registers a callback that will be notified when any [Publisher] event occurs.
func (t *publisherLifecycleListener[V]) OnAny(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V] {
	return t.OnDestroyed(callback).OnReleased(callback).OnDisconnected(callback).
		OnDisconnected(callback).OnChannelsFreed(callback)
}

// OnDestroyed registers a callback that will be notified when a [SubscPublisherriber] is destroyed.
func (t *publisherLifecycleListener[V]) OnDestroyed(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V] {
	return t.on(PublisherDestroyed, callback)
}

// OnReleased registers a callback that will be notified when a [Publisher] is released.
func (t *publisherLifecycleListener[V]) OnReleased(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V] {
	return t.on(PublisherReleased, callback)
}

// OnDisconnected registers a callback that will be notified when a [Publisher] is disconnected.
func (t *publisherLifecycleListener[V]) OnDisconnected(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V] {
	return t.on(PublisherDisconnected, callback)
}

// OnConnected registers a callback that will be notified when a [Publisher] is connected.
func (t *publisherLifecycleListener[V]) OnConnected(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V] {
	return t.on(PublisherConnected, callback)
}

// OnChannelsFreed registers a callback that will be notified when a [Publisher] has channels freed.
func (t *publisherLifecycleListener[V]) OnChannelsFreed(callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V] {
	return t.on(PublisherChannelsFreed, callback)
}

func (t *publisherLifecycleListener[V]) getEmitter() *eventEmitter[PublisherLifecycleEventType, PublisherLifecycleEvent[V]] {
	return t.emitter
}

// on registers a callback for the specified [PublisherLifecycleEventType].
func (t *publisherLifecycleListener[V]) on(event PublisherLifecycleEventType, callback func(PublisherLifecycleEvent[V])) PublisherLifecycleListener[V] {
	t.emitter.on(event, callback)
	return t
}

func (tp *topicPublisher[V]) AddLifecycleListener(listener PublisherLifecycleListener[V]) error {
	if tp.isClosed {
		return ErrPublisherClosed
	}

	tp.addLifecycleListener(listener)
	return nil
}

func (tp *topicPublisher[V]) RemoveLifecycleListener(listener PublisherLifecycleListener[V]) error {
	if tp.isClosed {
		return ErrPublisherClosed
	}

	tp.removeLifecycleListener(listener)
	return nil
}

// addLifecycleListener adds the specified [PublisherLifecycleListener].
func (tp *topicPublisher[V]) addLifecycleListener(listener PublisherLifecycleListener[V]) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()

	for _, e := range tp.lifecycleListenersV1 {
		if *e == listener {
			return
		}
	}
	tp.lifecycleListenersV1 = append(tp.lifecycleListenersV1, &listener)
}

// removeLifecycleListener removes the specified [TopicLifecycleListener].
func (tp *topicPublisher[V]) removeLifecycleListener(listener PublisherLifecycleListener[V]) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()

	idx := -1
	listeners := tp.lifecycleListenersV1
	for i, c := range listeners {
		if *c == listener {
			idx = i
			break
		}
	}
	if idx != -1 {
		result := append(listeners[:idx], listeners[idx+1:]...)
		tp.lifecycleListenersV1 = result
	}
}

func newPublisherLifecycleEvent[V any](nt Publisher[V], eventType PublisherLifecycleEventType) PublisherLifecycleEvent[V] {
	return &publisherLifecycleEvent[V]{source: nt, eventType: eventType}
}

// generatePublisherLifecycleEvent emits the publisher lifecycle events.
func (tp *topicPublisher[V]) generatePublisherLifecycleEvent(client interface{}, eventType PublisherLifecycleEventType) {
	listeners := tp.lifecycleListenersV1

	if pub, ok := client.(Publisher[V]); ok || client == nil {
		event := newPublisherLifecycleEvent[V](pub, eventType)
		for _, l := range listeners {
			e := *l
			e.getEmitter().emit(eventType, event)
		}
		// TODO:
		//if eventType == TopicDestroyed {
		//	_ = releaseTopicInternal[V](context.Background(), bt, false)
		//}
	}
}

type PublisherEventSubmitter interface {
	generatePublisherLifecycleEvent(client interface{}, eventType PublisherLifecycleEventType)
}
