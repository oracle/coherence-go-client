/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"errors"
	"fmt"
	pb1 "github.com/oracle/coherence-go-client/proto/v1"
)

const (
	Queue      NamedQueueType = NamedQueueType(pb1.NamedQueueType_Queue)
	PagedQueue NamedQueueType = NamedQueueType(pb1.NamedQueueType_PagedQueue)
	Dequeue    NamedQueueType = NamedQueueType(pb1.NamedQueueType_Deque)
)

var (
	_ NamedQueue[string] = &namedQueue[string]{}

	ErrQueueFailedCapacity = errors.New("the queue has reached capacity, unable to offer")
	ErrQueueFailedOffer    = errors.New("did not return success for offer")
	ErrQueueTimedOut       = errors.New("operation timed out for Poll() or Peek()")
)

// NamedQueue defines a non-blocking Queue implementation.
type NamedQueue[V any] interface {
	// Clear clears all the entries from the queue.
	Clear(ctx context.Context) error

	// Destroy destroys this queue on the server and releases all resources. After this operation it is no longer usable.
	Destroy(ctx context.Context) error

	// IsEmpty returns true if this queue is empty.
	IsEmpty(ctx context.Context) (bool, error)

	// IsReady returns true if this queue is ready to receive requests.
	IsReady(ctx context.Context) (bool, error)

	// Size returns the current size of this queue.
	Size(ctx context.Context) (int32, error)

	// OfferTail inserts the specified value to the end of this queue if it is possible to do
	// so immediately without violating capacity restrictions. If queue is full then
	// [ErrQueueFailedCapacity] is returned, if error is nil the element was added to the queue.
	OfferTail(ctx context.Context, value V) error

	// PeekHead retrieves, but does not remove, the head of this queue. If error is nil and nil value
	// return then there is no entry on the head of the queue.
	PeekHead(ctx context.Context) (*V, error)

	// PollHead retrieves and removes the head of this queue. If error is nil and the returned
	// value and error is nil this means that there was no entry on the head of the queue.
	PollHead(ctx context.Context) (*V, error)

	// GetName returns the cache name used for this queue.
	GetName() string

	// Release releases a queue and removes any resources associated with it on the client side.
	Release()

	// GetType returns the type of the [NamedQueue].
	GetType() NamedQueueType
}

type baseQueueClient[V any] struct {
	queueType       NamedQueueType
	session         *Session
	valueSerializer Serializer[V]
	name            string
	ctx             context.Context
	queueID         int32
}

type namedQueue[V any] struct {
	*baseQueueClient[V]
}

// GetNamedQueue returns a new [NamedQueue].
func GetNamedQueue[V any](ctx context.Context, session *Session, queueName string, queueType NamedQueueType) (NamedQueue[V], error) {
	var (
		existingQueue interface{}
		ok            bool
		queueID       *int32
		err           error
	)

	if queueType == Dequeue {
		return nil, errors.New("to create a Dequeue, please use GetNamedDequeue")
	}

	// protect updates to maps
	session.mapMutex.Lock()

	// check to see if we already have an entry for the queue
	if existingQueue, ok = session.queues[queueName]; ok {
		defer session.mapMutex.Unlock()

		existing, ok2 := existingQueue.(NamedQueue[V])
		if !ok2 {
			// the casting failed so return an error indicating the queue exists with different type mappings
			return nil, getExistingError("NamedQueue", queueName)
		}

		if existing.GetType() != queueType {
			return nil, getDifferentQueueTypeError(queueName, queueType)
		}

		session.debug("using existing NamedQueue: %v", existing)
		return existing, nil
	}

	// put a place-holder incase second go routine gets here
	session.queues[queueName] = nil
	session.mapMutex.Unlock()

	// ensure the queue
	queueID, err = session.v1StreamManagerQueue.ensureQueue(ctx, queueName, queueType)
	if err != nil {
		return nil, err
	}

	bq, err := newBaseQueueClient[V](ctx, session, queueName, queueType, *queueID)
	if err != nil {
		return nil, err
	}

	queue := &namedQueue[V]{
		baseQueueClient: bq,
	}

	session.queues[queueName] = queue

	return queue, nil
}

func newBaseQueueClient[V any](ctx context.Context, session *Session, queueName string, queueType NamedQueueType, queueID int32) (*baseQueueClient[V], error) {
	if session.closed {
		return nil, ErrClosed
	}

	bq := baseQueueClient[V]{
		queueType:       queueType,
		ctx:             ctx,
		name:            queueName,
		queueID:         queueID,
		session:         session,
		valueSerializer: NewSerializer[V](session.sessOpts.Format),
	}

	return &bq, nil
}

// NamedQueue

func (nq *namedQueue[V]) Clear(ctx context.Context) error {
	return nq.baseQueueClient.session.v1StreamManagerQueue.genericQueueRequest(ctx, pb1.NamedQueueRequestType_Clear, nq.name)
}

func (nq *namedQueue[V]) Destroy(ctx context.Context) error {
	return releaseInternal[V](ctx, nq.baseQueueClient, true)
}

func releaseInternal[V any](ctx context.Context, bc *baseQueueClient[V], destroy bool) error {
	// protect updates to maps
	bc.session.mapMutex.Lock()
	defer bc.session.mapMutex.Unlock()

	if destroy {
		newCtx, cancel := bc.session.ensureContext(ctx)
		if cancel != nil {
			defer cancel()
		}

		err := bc.session.v1StreamManagerQueue.genericQueueRequest(newCtx, pb1.NamedQueueRequestType_Destroy, bc.name)
		if err != nil {
			return err
		}
	}

	delete(bc.session.queues, bc.name)
	bc.session.queueIDMap.Remove(bc.name)

	return nil
}

func (nq *namedQueue[V]) IsEmpty(ctx context.Context) (bool, error) {
	return nq.baseQueueClient.session.v1StreamManagerQueue.genericBoolValueQueue(ctx, pb1.NamedQueueRequestType_IsEmpty, nq.name)
}

func (nq *namedQueue[V]) IsReady(ctx context.Context) (bool, error) {
	return nq.baseQueueClient.session.v1StreamManagerQueue.genericBoolValueQueue(ctx, pb1.NamedQueueRequestType_IsEmpty, nq.name)
}

func (nq *namedQueue[V]) GetName() string {
	return nq.name
}

func (nq *namedQueue[V]) GetType() NamedQueueType {
	return nq.queueType
}

func (nq *namedQueue[V]) Release() {
	_ = releaseInternal[V](context.Background(), nq.baseQueueClient, false)
}

func (nq *namedQueue[V]) Size(ctx context.Context) (int32, error) {
	return nq.baseQueueClient.session.v1StreamManagerQueue.sizeQueue(ctx, nq.name)
}

func (nq *namedQueue[V]) OfferTail(ctx context.Context, value V) error {
	return offerTailInternal[V](ctx, nq.baseQueueClient, value)
}

func offerTailInternal[V any](ctx context.Context, bq *baseQueueClient[V], value V) error {
	binValue, err := bq.valueSerializer.Serialize(value)
	if err != nil {
		return err
	}

	streamManager := bq.session.v1StreamManagerQueue

	req, err := streamManager.newOfferTail(pb1.NamedQueueRequestType_OfferTail, bq.name, binValue)
	if err != nil {
		return err
	}

	requestType, err := streamManager.submitQueueRequest(req, pb1.NamedQueueRequestType_OfferTail)
	if err != nil {
		return err
	}

	newCtx, cancel := bq.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer streamManager.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return err1
	}

	var message = &pb1.QueueOfferResult{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("queueOfferResult", err)
		return err
	}
	// we should have a QueueOfferResult, check this succeeded
	if message.Succeeded {
		return nil
	}

	return ErrQueueFailedOffer
}

func (nq *namedQueue[V]) PeekHead(ctx context.Context) (*V, error) {
	return peekOrPollHead[V](ctx, nq.baseQueueClient, pb1.NamedQueueRequestType_PeekHead)
}

func (nq *namedQueue[V]) PollHead(ctx context.Context) (*V, error) {
	return peekOrPollHead[V](ctx, nq.baseQueueClient, pb1.NamedQueueRequestType_PollHead)
}

func peekOrPollHead[V any](ctx context.Context, bq *baseQueueClient[V], reqType pb1.NamedQueueRequestType) (*V, error) {
	streamManager := bq.session.v1StreamManagerQueue

	req, err := streamManager.newWrapperProxyQueueRequest(bq.name, reqType, nil)
	if err != nil {
		return nil, err
	}

	requestType, err := streamManager.submitQueueRequest(req, reqType)
	if err != nil {
		return nil, err
	}

	newCtx, cancel := bq.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer streamManager.cleanupRequest(req.Id)

	result, err := waitForResponse(newCtx, requestType.ch)
	if err != nil {
		return nil, err
	}

	var message = &pb1.OptionalValue{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("optionalValue", err)
		return nil, err
	}

	// It not present nil and nil means there was no value for peek for poll
	if !message.Present {
		return nil, nil
	}

	return bq.valueSerializer.Deserialize(message.Value)
}

func (nq *namedQueue[V]) String() string {
	return fmt.Sprintf("NamedQueue{name=%s, type=%v, queueID=%v}", nq.name, nq.queueType, nq.queueID)
}

func (qt NamedQueueType) String() string {
	if qt == Queue {
		return "Queue"
	}
	if qt == PagedQueue {
		return "PagedQueue"
	}
	return "Dequeue"
}
