/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"fmt"
)

const (
	// TopicDestroyed raised when a queue is destroyed usually as a result of a call to NamedTopic.Destroy().
	TopicDestroyed TopicLifecycleEventType = "topic_destroyed"

	// TopicReleased raised when a topic is released but the session.
	TopicReleased TopicLifecycleEventType = "topic_released"
)

type TopicLifecycleEventType string

type TopicLifecycleEvent[V any] interface {
	Source() NamedTopic[V]
	Type() TopicLifecycleEventType
}

type topicLifecycleEvent[V any] struct {
	// source the source of the event
	source NamedTopic[V]

	// Type of this event's TopicLifecycleEventType
	eventType TopicLifecycleEventType
}

// Type returns the TopicLifecycleEvent for this [TopicLifecycleEventType].
func (l *topicLifecycleEvent[V]) Type() TopicLifecycleEventType {
	return l.eventType
}

// Source returns the source of this [QueueLifecycleEvent].
func (l *topicLifecycleEvent[V]) Source() NamedTopic[V] {
	return l.source
}

// String returns a string representation of a MapLifecycleEvent.
func (l *topicLifecycleEvent[V]) String() string {
	return fmt.Sprintf("TopicLifecycleEvent{source=%v, type=%s}", l.Source().GetName(), l.Type())
}

// TopicLifecycleListener allows registering callbacks to be notified when lifecycle events
// (destroyed, released) occur against a [NamedTopic].
type TopicLifecycleListener[V any] interface {
	// OnAny registers a callback that will be notified when any [NamedTopic] event occurs.
	OnAny(callback func(TopicLifecycleEvent[V])) TopicLifecycleListener[V]

	// OnDestroyed registers a callback that will be notified when a [NamedTopic] is destroyed.
	OnDestroyed(callback func(TopicLifecycleEvent[V])) TopicLifecycleListener[V]

	// OnReleased registers a callback that will be notified when a [NamedTopic] is released.
	OnReleased(callback func(TopicLifecycleEvent[V])) TopicLifecycleListener[V]

	getEmitter() *eventEmitter[TopicLifecycleEventType, TopicLifecycleEvent[V]]
}

// NewTopicLifecycleListener creates and returns a pointer to a new [TopicLifecycleListener] instance.
func NewTopicLifecycleListener[V any]() TopicLifecycleListener[V] {
	return &topicLifecycleListener[V]{newEventEmitter[TopicLifecycleEventType, TopicLifecycleEvent[V]]()}
}

type topicLifecycleListener[V any] struct { //lint:ignore U1000 - required due to linter issues with generics
	emitter *eventEmitter[TopicLifecycleEventType, TopicLifecycleEvent[V]]
}

// OnAny registers a callback that will be notified when any [NamedTopic] event occurs.
func (t *topicLifecycleListener[V]) OnAny(callback func(TopicLifecycleEvent[V])) TopicLifecycleListener[V] {
	return t.OnDestroyed(callback).OnReleased(callback)
}

// OnDestroyed registers a callback that will be notified when a[NamedTopic] is destroyed.
func (t *topicLifecycleListener[V]) OnDestroyed(callback func(TopicLifecycleEvent[V])) TopicLifecycleListener[V] {
	return t.on(TopicDestroyed, callback)
}

// OnReleased registers a callback that will be notified when a[NamedTopic] is released.
func (t *topicLifecycleListener[V]) OnReleased(callback func(TopicLifecycleEvent[V])) TopicLifecycleListener[V] {
	return t.on(TopicReleased, callback)
}

func (t *topicLifecycleListener[V]) getEmitter() *eventEmitter[TopicLifecycleEventType, TopicLifecycleEvent[V]] {
	return t.emitter
}

// on registers a callback for the specified [TopicLifecycleEventType].
func (t *topicLifecycleListener[V]) on(event TopicLifecycleEventType, callback func(TopicLifecycleEvent[V])) TopicLifecycleListener[V] {
	t.emitter.on(event, callback)
	return t
}

func (bt *baseTopicsClient[V]) AddLifecycleListener(listener TopicLifecycleListener[V]) error {
	if bt.isDestroyed || bt.isReleased {
		return ErrTopicDestroyedOrReleased
	}

	bt.addLifecycleListener(listener)
	return nil
}

func (bt *baseTopicsClient[V]) RemoveLifecycleListener(listener TopicLifecycleListener[V]) error {
	if bt.isDestroyed || bt.isReleased {
		return ErrTopicDestroyedOrReleased
	}

	bt.removeLifecycleListener(listener)
	return nil
}

// addLifecycleListener adds the specified [TopicLifecycleListener].
func (bt *baseTopicsClient[V]) addLifecycleListener(listener TopicLifecycleListener[V]) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	for _, e := range bt.lifecycleListenersV1 {
		if *e == listener {
			return
		}
	}
	bt.lifecycleListenersV1 = append(bt.lifecycleListenersV1, &listener)
}

// removeLifecycleListener removes the specified [TopicLifecycleListener].
func (bt *baseTopicsClient[V]) removeLifecycleListener(listener TopicLifecycleListener[V]) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	idx := -1
	listeners := bt.lifecycleListenersV1
	for i, c := range listeners {
		if *c == listener {
			idx = i
			break
		}
	}
	if idx != -1 {
		result := append(listeners[:idx], listeners[idx+1:]...)
		bt.lifecycleListenersV1 = result
	}
}

func newTopicLifecycleEvent[V any](nt NamedTopic[V], eventType TopicLifecycleEventType) TopicLifecycleEvent[V] {
	return &topicLifecycleEvent[V]{source: nt, eventType: eventType}
}

// generateTopicLifecycleEvent emits the topic lifecycle events.
func (bt *baseTopicsClient[V]) generateTopicLifecycleEvent(client interface{}, eventType TopicLifecycleEventType) {
	listeners := bt.lifecycleListenersV1

	if namedT, ok := client.(NamedTopic[V]); ok || client == nil {
		event := newTopicLifecycleEvent[V](namedT, eventType)
		for _, l := range listeners {
			e := *l
			e.getEmitter().emit(eventType, event)
		}

		if eventType == TopicDestroyed {
			_ = releaseTopicInternal[V](context.Background(), bt, false)
		}
	}
}

type TopicEventSubmitter interface {
	generateTopicLifecycleEvent(client interface{}, eventType TopicLifecycleEventType)
}
