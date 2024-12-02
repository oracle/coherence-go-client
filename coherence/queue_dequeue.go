/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	pb1 "github.com/oracle/coherence-go-client/proto/v1"
)

var (
	_ NamedDequeue[string] = &namedDequeue[string]{}
)

type NamedDequeue[V any] interface {
	NamedQueue[V]

	// OfferHead inserts the specific value at the head of this queue if it is possible to do
	// so immediately without violating capacity restrictions. If queue is full then
	// [ErrQueueFailedCapacity] is returned, if error is nil the element was added to the queue.
	OfferHead(ctx context.Context, value V) error

	// PollTail retrieves and removes the tail of this queue. If error is nil and the returned
	// value and error is nil this means that there was no entry on the head of the queue.
	PollTail(ctx context.Context) (*V, error)

	// PeekTail retrieves, but does not remove, the tail of this queue. If error is nil and nil value
	// return then there is no entry on the tail of the queue.
	PeekTail(ctx context.Context) (*V, error)
}

type namedDequeue[V any] struct {
	*baseQueueClient[V]
}

func GetNamedDeQueue[V any](ctx context.Context, session *Session, queueName string) (NamedDequeue[V], error) {
	var (
		existingQueue interface{}
		ok            bool
		err           error
		queueType     = Dequeue
	)

	session.mapMutex.Lock()

	if existingQueue, ok = session.queues[queueName]; ok {
		defer session.mapMutex.Unlock()

		existing, ok2 := existingQueue.(NamedDequeue[V])
		if !ok2 {
			return nil, getExistingError("NamedDequeue", queueName)
		}

		if existing.GetType() != queueType {
			return nil, getDifferentQueueTypeError(queueName, queueType)
		}

		session.debug("using existing NameDequeue: %v", existing)
		return existing, nil
	}

	session.queues[queueName] = nil
	session.mapMutex.Unlock()

	bq, err := ensureQueueInternal[V](ctx, session, queueName, queueType)
	if err != nil {
		return nil, err
	}

	queue := &namedDequeue[V]{
		baseQueueClient: bq,
	}

	session.queues[queueName] = queue

	return queue, nil
}

func ensureQueueInternal[V any](ctx context.Context, session *Session, queueName string, queueType NamedQueueType) (*baseQueueClient[V], error) {
	if err := ensureV1StreamManagerQueue(session); err != nil {
		return nil, err
	}

	queueID, err := session.v1StreamManagerQueue.ensureQueue(ctx, queueName, queueType)
	if err != nil {
		return nil, err
	}

	return newBaseQueueClient[V](ctx, session, queueName, queueType, *queueID)
}

func (nd *namedDequeue[V]) Clear(ctx context.Context) error {
	if nd.isDestroyed || nd.isReleased {
		return ErrQueueDestroyedOrReleased
	}

	return nd.baseQueueClient.session.v1StreamManagerQueue.genericQueueRequest(ctx, pb1.NamedQueueRequestType_Clear, nd.name)
}

func (nd *namedDequeue[V]) Destroy(ctx context.Context) error {
	if nd.isDestroyed || nd.isReleased {
		return ErrQueueDestroyedOrReleased
	}

	return releaseInternal(ctx, nd.baseQueueClient, true)
}

func (nd *namedDequeue[V]) IsEmpty(ctx context.Context) (bool, error) {
	if nd.isDestroyed || nd.isReleased {
		return false, ErrQueueDestroyedOrReleased
	}

	return nd.baseQueueClient.session.v1StreamManagerQueue.genericBoolValueQueue(ctx, pb1.NamedQueueRequestType_IsEmpty, nd.name)
}

func (nd *namedDequeue[V]) IsReady(ctx context.Context) (bool, error) {
	if nd.isDestroyed || nd.isReleased {
		return false, ErrQueueDestroyedOrReleased
	}

	return nd.baseQueueClient.session.v1StreamManagerQueue.genericBoolValueQueue(ctx, pb1.NamedQueueRequestType_IsReady, nd.name)
}

func (nd *namedDequeue[V]) Release() {
	_ = releaseInternal[V](context.Background(), nd.baseQueueClient, false)
}

func (nd *namedDequeue[V]) Size(ctx context.Context) (int32, error) {
	if nd.isDestroyed || nd.isReleased {
		return 0, ErrQueueDestroyedOrReleased
	}

	return nd.baseQueueClient.session.v1StreamManagerQueue.sizeQueue(ctx, nd.name)
}

func (nd *namedDequeue[V]) AddLifecycleListener(listener QueueLifecycleListener[V]) error {
	if nd.isDestroyed || nd.isReleased {
		return ErrQueueDestroyedOrReleased
	}

	nd.baseQueueClient.addLifecycleListener(listener)
	return nil
}

func (nd *namedDequeue[V]) RemoveLifecycleListener(listener QueueLifecycleListener[V]) error {
	if nd.isDestroyed || nd.isReleased {
		return ErrQueueDestroyedOrReleased
	}

	nd.baseQueueClient.removeLifecycleListener(listener)
	return nil
}

func (nd *namedDequeue[V]) OfferTail(ctx context.Context, value V) error {
	return offerInternal[V](ctx, nd.baseQueueClient, value, pb1.NamedQueueRequestType_OfferTail)
}

func (nd *namedDequeue[V]) PeekHead(ctx context.Context) (*V, error) {
	return peekOrPollHead[V](ctx, nd.baseQueueClient, pb1.NamedQueueRequestType_PeekHead)
}

func (nd *namedDequeue[V]) PollHead(ctx context.Context) (*V, error) {
	return peekOrPollHead[V](ctx, nd.baseQueueClient, pb1.NamedQueueRequestType_PollHead)
}

func (nd *namedDequeue[V]) OfferHead(ctx context.Context, value V) error {
	return offerInternal[V](ctx, nd.baseQueueClient, value, pb1.NamedQueueRequestType_OfferHead)
}

func (nd *namedDequeue[V]) PollTail(ctx context.Context) (*V, error) {
	return peekOrPollHead[V](ctx, nd.baseQueueClient, pb1.NamedQueueRequestType_PollTail)
}

func (nd *namedDequeue[V]) PeekTail(ctx context.Context) (*V, error) {
	return peekOrPollHead[V](ctx, nd.baseQueueClient, pb1.NamedQueueRequestType_PeekTail)
}

func (nd *namedDequeue[V]) GetName() string {
	return nd.name
}

func (nd *namedDequeue[V]) GetType() NamedQueueType {
	return Dequeue
}
