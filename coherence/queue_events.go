/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import "fmt"

const (
	// QueueDestroyed raised when a queue is destroyed usually as a result of a call to NamedQueue.Destroy().
	QueueDestroyed QueueLifecycleEventType = "queue_destroyed"

	// QueueTruncated raised when a queue is truncated.
	QueueTruncated QueueLifecycleEventType = "queue_truncated"

	// QueueReleased raised when a queue is released but the session.
	QueueReleased QueueLifecycleEventType = "queue_released"
)

// QueueLifecycleEventType describes an event that may be raised during the lifecycle of a queue.
type QueueLifecycleEventType string

type QueueLifecycleEvent[V any] interface {
	Source() NamedQueue[V]
	Type() QueueLifecycleEventType
}

func newQueueLifecycleEvent[V any](nq NamedQueue[V], eventType QueueLifecycleEventType) QueueLifecycleEvent[V] {
	return &queueLifecycleEvent[V]{source: nq, eventType: eventType}
}

type queueLifecycleEvent[V any] struct {
	// source the source of the event
	source NamedQueue[V]

	// Type of this event's QueueLifecycleEventType
	eventType QueueLifecycleEventType
}

// Type returns the MapLifecycleEventType for this [QueueLifecycleEvent].
func (l *queueLifecycleEvent[V]) Type() QueueLifecycleEventType {
	return l.eventType
}

// Source returns the source of this [QueueLifecycleEvent].
func (l *queueLifecycleEvent[V]) Source() NamedQueue[V] {
	return l.source
}

// String returns a string representation of a MapLifecycleEvent.
func (l *queueLifecycleEvent[V]) String() string {
	return fmt.Sprintf("QueueLifecycleEvent{source=%v, type=%s}", l.Source().GetName(), l.Type())
}

// QueueLifecycleListener allows registering callbacks to be notified when lifecycle events
// (truncated, released or destroyed) occur against a [NamedQueue].
type QueueLifecycleListener[V any] interface {
	OnAny(callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V]
	OnDestroyed(callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V]
	OnTruncated(callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V]
	OnReleased(callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V]
	getEmitter() *eventEmitter[QueueLifecycleEventType, QueueLifecycleEvent[V]]
}

type queueLifecycleListener[V any] struct { //lint:ignore U1000 - required due to linter issues with generics
	emitter *eventEmitter[QueueLifecycleEventType, QueueLifecycleEvent[V]]
}

// NewQueueLifecycleListener creates and returns a pointer to a new [QueueLifecycleListener] instance.
func NewQueueLifecycleListener[V any]() QueueLifecycleListener[V] {
	return &queueLifecycleListener[V]{newEventEmitter[QueueLifecycleEventType, QueueLifecycleEvent[V]]()}
}

// on registers a callback for the specified [QueueLifecycleEventType].
func (q *queueLifecycleListener[V]) on(event QueueLifecycleEventType, callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V] {
	q.emitter.on(event, callback)
	return q
}

// OnDestroyed registers a callback that will be notified when a [NamedQueue] is destroyed.
func (q *queueLifecycleListener[V]) OnDestroyed(callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V] {
	return q.on(QueueDestroyed, callback)
}

// OnReleased registers a callback that will be notified when a [NamedQueue] is released.
func (q *queueLifecycleListener[V]) OnReleased(callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V] {
	return q.on(QueueReleased, callback)
}

// OnTruncated registers a callback that will be notified when a [NamedMap] is truncated.
func (q *queueLifecycleListener[V]) OnTruncated(callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V] {
	return q.on(QueueTruncated, callback)
}

func (q *queueLifecycleListener[V]) getEmitter() *eventEmitter[QueueLifecycleEventType, QueueLifecycleEvent[V]] {
	return q.emitter
}

// OnAny registers a callback that will be notified when any [NamedMap] event occurs.
func (q *queueLifecycleListener[V]) OnAny(callback func(QueueLifecycleEvent[V])) QueueLifecycleListener[V] {
	return q.OnTruncated(callback).OnDestroyed(callback).OnReleased(callback)
}

// addLifecycleListener adds the specified [QueueLifecycleListener].
func (bq *baseQueueClient[V]) addLifecycleListener(listener QueueLifecycleListener[V]) {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	for _, e := range bq.lifecycleListenersV1 {
		if *e == listener {
			return
		}
	}
	bq.lifecycleListenersV1 = append(bq.lifecycleListenersV1, &listener)
}

// removeLifecycleListener removes the specified [QueueLifecycleListener].
func (bq *baseQueueClient[V]) removeLifecycleListener(listener QueueLifecycleListener[V]) {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	idx := -1
	listeners := bq.lifecycleListenersV1
	for i, c := range listeners {
		if *c == listener {
			idx = i
			break
		}
	}
	if idx != -1 {
		result := append(listeners[:idx], listeners[idx+1:]...)
		bq.lifecycleListenersV1 = result
	}
}
