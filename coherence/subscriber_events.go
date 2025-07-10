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
	// SubscriberDisconnected raised when a subscriber] is disconnected.
	SubscriberDisconnected      SubscriberLifecycleEventType = "subscriber_disconnected"
	SubscriberReleased          SubscriberLifecycleEventType = "subscriber_released"
	SubscriberDestroyed         SubscriberLifecycleEventType = "subscriber_destroyed"
	SubscriberUnsubscribed      SubscriberLifecycleEventType = "subscriber_unsubscribed"
	SubscriberChannelHead       SubscriberLifecycleEventType = "subscriber_channel_head"
	SubscriberChannelPopulated  SubscriberLifecycleEventType = "subscriber_channel_populated"
	SubscriberChannelsLost      SubscriberLifecycleEventType = "subscriber_channel_lost"
	SubscriberChannelAllocation SubscriberLifecycleEventType = "subscriber_channel_allocation"
	SubscriberGroupDestroyed    SubscriberLifecycleEventType = "subscriber_group_destroyed"
)

type SubscriberLifecycleEventType string

type SubscriberLifecycleEvent[V any] interface {
	Source() Subscriber[V]
	Type() SubscriberLifecycleEventType
}

type subscriberLifecycleEvent[V any] struct {
	// source the source of the event
	source Subscriber[V]

	// Type of this event's SubscriberLifecycleEventType
	eventType SubscriberLifecycleEventType
}

// Type returns the [SubscriberLifecycleEventType] for this [SubscriberLifecycleEvent].
func (l *subscriberLifecycleEvent[V]) Type() SubscriberLifecycleEventType {
	return l.eventType
}

// Source returns the source of this [SubscriberLifecycleEvent].
func (l *subscriberLifecycleEvent[V]) Source() Subscriber[V] {
	return l.source
}

// String returns a string representation of a MapLifecycleEvent.
func (l *subscriberLifecycleEvent[V]) String() string {
	return fmt.Sprintf("subscriberLifecycleEvent{source=%v, type=%s}", l.Source(), l.Type())
}

// SubscriberLifecycleListener allows registering callbacks to be notified when lifecycle events
// occur against a [Subscriber].
type SubscriberLifecycleListener[V any] interface {
	// OnAny registers a callback that will be notified when any [Subscriber] event occurs.
	OnAny(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	// OnDestroyed registers a callback that will be notified when a [Subscriber] is destroyed.
	OnDestroyed(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	// OnReleased registers a callback that will be notified when a [Subscriber] is released.
	OnReleased(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	getEmitter() *eventEmitter[SubscriberLifecycleEventType, SubscriberLifecycleEvent[V]]
}

// NewSubscriberLifecycleListener creates and returns a pointer to a new [SubscriberLifecycleListener] instance.
func NewSubscriberLifecycleListener[V any]() SubscriberLifecycleListener[V] {
	return &subscriberLifecycleListener[V]{newEventEmitter[SubscriberLifecycleEventType, SubscriberLifecycleEvent[V]]()}
}

type subscriberLifecycleListener[V any] struct { //lint:ignore U1000 - required due to linter issues with generics
	emitter *eventEmitter[SubscriberLifecycleEventType, SubscriberLifecycleEvent[V]]
}

// OnAny registers a callback that will be notified when any [NamedTopic] event occurs.
func (t *subscriberLifecycleListener[V]) OnAny(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.OnDestroyed(callback).OnReleased(callback)
}

// OnDestroyed registers a callback that will be notified when a[NamedTopic] is destroyed.
func (t *subscriberLifecycleListener[V]) OnDestroyed(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberDestroyed, callback)
}

// OnReleased registers a callback that will be notified when a[NamedTopic] is released.
func (t *subscriberLifecycleListener[V]) OnReleased(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberReleased, callback)
}

func (t *subscriberLifecycleListener[V]) getEmitter() *eventEmitter[SubscriberLifecycleEventType, SubscriberLifecycleEvent[V]] {
	return t.emitter
}

// on registers a callback for the specified [TopicLifecycleEventType].
func (t *subscriberLifecycleListener[V]) on(event SubscriberLifecycleEventType, callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	t.emitter.on(event, callback)
	return t
}

func (ts *topicSubscriber[V]) AddLifecycleListener(listener SubscriberLifecycleListener[V]) error {
	if ts.isClosed {
		return ErrSubscriberClosed
	}

	ts.addLifecycleListener(listener)
	return nil
}

func (ts *topicSubscriber[V]) RemoveLifecycleListener(listener SubscriberLifecycleListener[V]) error {
	if ts.isClosed {
		return ErrSubscriberClosed
	}

	ts.removeLifecycleListener(listener)
	return nil
}

// addLifecycleListener adds the specified [SubscriberLifecycleListener].
func (ts *topicSubscriber[V]) addLifecycleListener(listener SubscriberLifecycleListener[V]) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	for _, e := range ts.lifecycleListenersV1 {
		if *e == listener {
			return
		}
	}
	ts.lifecycleListenersV1 = append(ts.lifecycleListenersV1, &listener)
}

// removeLifecycleListener removes the specified [TopicLifecycleListener].
func (ts *topicSubscriber[V]) removeLifecycleListener(listener SubscriberLifecycleListener[V]) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	idx := -1
	listeners := ts.lifecycleListenersV1
	for i, c := range listeners {
		if *c == listener {
			idx = i
			break
		}
	}
	if idx != -1 {
		result := append(listeners[:idx], listeners[idx+1:]...)
		ts.lifecycleListenersV1 = result
	}
}

func newSubscriberLifecycleEvent[V any](nt Subscriber[V], eventType SubscriberLifecycleEventType) SubscriberLifecycleEvent[V] {
	return &subscriberLifecycleEvent[V]{source: nt, eventType: eventType}
}

// generateTopicLifecycleEvent emits the queue lifecycle events.
func (ts *topicSubscriber[V]) generateSubscriberLifecycleEvent(client interface{}, eventType SubscriberLifecycleEventType) {
	listeners := ts.lifecycleListenersV1

	if sub, ok := client.(NamedTopic[V]); ok || client == nil {
		event := newSubscriberLifecycleEvent[V](sub, eventType)
		for _, l := range listeners {
			e := *l
			e.getEmitter().emit(eventType, event)
		}
		//
		//if eventType == TopicDestroyed {
		//	_ = releaseTopicInternal[V](context.Background(), bt, false)
		//}
	}
}

type SubscriberEventSubmitter interface {
	generateSubscriberLifecycleEvent(client interface{}, eventType SubscriberLifecycleEventType)
}
