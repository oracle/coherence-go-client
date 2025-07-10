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

// String returns a string representation of a [SubscriberLifecycleEvent].\.
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

	// OnDisconnected registers a callback that will be notified when a [Subscriber] is disconnected.
	OnDisconnected(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	// OnUnsubscribed registers a callback that will be notified when a [Subscriber] is unsubscribed.
	OnUnsubscribed(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	// OnChannelsLost registers a callback that will be notified when a [Subscriber] loses channels.
	OnChannelsLost(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	// OnChannelHeadChanged registers a callback that will be notified when head position of a channel has changed for a [Subscriber].
	OnChannelHeadChanged(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	// OnChannelPopulated registers a callback that will be notified when a channel is populated for a [Subscriber].
	OnChannelPopulated(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	// OnChannelAllocated registers a callback that will be notified when a channel is allocated for a [Subscriber].
	OnChannelAllocated(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	// OnSubscriberGroupDestroyed registers a callback that will be notified when a subscriber group is destroyed for a [Subscriber].
	OnSubscriberGroupDestroyed(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V]

	getEmitter() *eventEmitter[SubscriberLifecycleEventType, SubscriberLifecycleEvent[V]]
}

// NewSubscriberLifecycleListener creates and returns a pointer to a new [SubscriberLifecycleListener] instance.
func NewSubscriberLifecycleListener[V any]() SubscriberLifecycleListener[V] {
	return &subscriberLifecycleListener[V]{newEventEmitter[SubscriberLifecycleEventType, SubscriberLifecycleEvent[V]]()}
}

type subscriberLifecycleListener[V any] struct { //lint:ignore U1000 - required due to linter issues with generics
	emitter *eventEmitter[SubscriberLifecycleEventType, SubscriberLifecycleEvent[V]]
}

// OnAny registers a callback that will be notified when any [Subscriber] event occurs.
func (t *subscriberLifecycleListener[V]) OnAny(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.OnDestroyed(callback).OnReleased(callback).OnDisconnected(callback).
		OnUnsubscribed(callback).OnChannelsLost(callback).OnChannelHeadChanged(callback).
		OnChannelPopulated(callback).OnSubscriberGroupDestroyed(callback).OnChannelAllocated(callback)
}

// OnDestroyed registers a callback that will be notified when a [Subscriber] is destroyed.
func (t *subscriberLifecycleListener[V]) OnDestroyed(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberDestroyed, callback)
}

// OnReleased registers a callback that will be notified when a [Subscriber] is released.
func (t *subscriberLifecycleListener[V]) OnReleased(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberReleased, callback)
}

// OnDisconnected registers a callback that will be notified when a [Subscriber] is disconnected.
func (t *subscriberLifecycleListener[V]) OnDisconnected(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberDisconnected, callback)
}

// OnUnsubscribed registers a callback that will be notified when a [Subscriber] is unsubscribed.
func (t *subscriberLifecycleListener[V]) OnUnsubscribed(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberUnsubscribed, callback)
}

// OnChannelsLost registers a callback that will be notified when a [Subscriber] loses channels.
func (t *subscriberLifecycleListener[V]) OnChannelsLost(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberChannelsLost, callback)
}

// OnChannelHeadChanged registers a callback that will be notified when head position of a channel has changed for a [Subscriber].
func (t *subscriberLifecycleListener[V]) OnChannelHeadChanged(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberChannelHead, callback)
}

// OnChannelPopulated registers a callback that will be notified when a channel is populated for a [Subscriber].
func (t *subscriberLifecycleListener[V]) OnChannelPopulated(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberChannelPopulated, callback)
}

// OnChannelAllocated registers a callback that will be notified when a channel is allocated for a [Subscriber].
func (t *subscriberLifecycleListener[V]) OnChannelAllocated(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberChannelAllocation, callback)
}

// OnSubscriberGroupDestroyed registers a callback that will be notified when a subscriber group is destroyed for a [Subscriber].
func (t *subscriberLifecycleListener[V]) OnSubscriberGroupDestroyed(callback func(SubscriberLifecycleEvent[V])) SubscriberLifecycleListener[V] {
	return t.on(SubscriberGroupDestroyed, callback)
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

// generateSubscriberLifecycleEvent emits the subscriber lifecycle events.
func (ts *topicSubscriber[V]) generateSubscriberLifecycleEvent(client interface{}, eventType SubscriberLifecycleEventType) {
	listeners := ts.lifecycleListenersV1

	if sub, ok := client.(Subscriber[V]); ok || client == nil {
		event := newSubscriberLifecycleEvent[V](sub, eventType)
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

type SubscriberEventSubmitter interface {
	generateSubscriberLifecycleEvent(client interface{}, eventType SubscriberLifecycleEventType)
}
