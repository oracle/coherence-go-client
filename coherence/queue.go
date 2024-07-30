/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/processors"
	"sync"
	"time"
)

const (
	longMaxValue              int64 = 9223372036854775807
	longMinValue              int64 = -9223372036854775808
	offerResultSuccess              = 1
	offerResultFailedCapacity       = 2
)

var (
	_ NamedQueue[string]         = &namedQueue[string]{}
	_ NamedBlockingQueue[string] = &namedBlockingQueue[string]{}

	ErrQueueFailedCapacity = errors.New("the queue has reached capacity, unable to offer")
	ErrQueueFailedOffer    = errors.New("did not return success for offer")
	ErrQueueTimedOut       = errors.New("operation timed out for Poll() or Peek()")
)

// NamedQueue defines a non-blocking Queue implementation.
type NamedQueue[V any] interface {
	// Offer inserts the specified value to the end of this queue if it is possible to do
	// so immediately without violating capacity restrictions. If queue is full then
	// [ErrQueueFailedCapacity] is returned, if error is nil the element was added to the queue.
	Offer(value V) error

	// Poll retrieves and removes the head of this queue. If error is nil and the returned
	// value and error is nil this means that there was no entry on the head of the queue.
	Poll() (*V, error)

	// Peek retrieves, but does not remove, the head of this queue. If error is nil and nil value
	// return then there is no entry on the head of the queue.
	Peek() (*V, error)

	// GetName returns the cache name used for the queue.
	GetName() string

	// Size returns the current size of the queue.
	Size() (int, error)

	// Close closes a queue and removes any resources associated with it on the client side.
	Close() error
}

// NamedBlockingQueue defines a blocking Queue implementation.
type NamedBlockingQueue[V any] interface {
	// Offer inserts the specified value to the end of the queue if it is possible to do
	// so immediately without violating capacity restrictions. If queue is full then
	// [ErrQueueFailedCapacity] is returned, if error is nil the element was added to the queue.
	Offer(value V) error

	// Poll retrieves and removes the head of this queue within the specified timeout.
	// If error is [ErrQueueTimedOut] this means the operation failed to get a value within the timeout.
	// If error is nil, then the value on the head of the queue is returned.
	Poll(timeout time.Duration) (*V, error)

	// Peek retrieves, but does not remove, the head of this queue within the specified timeout.
	// If error is [ErrQueueTimedOut] this means the operation failed to get a value within the timeout.
	// If error is nil, then the value on the head of the queue is returned.
	Peek(timeout time.Duration) (*V, error)

	// GetName returns the cache name used for the queue.
	GetName() string

	// Size returns the current size of the queue.
	Size() (int, error)

	// Close closes a queue and removes any resources associated with it on the client side.
	Close() error
}

// QueueKey defines the key of a queue entry. Exported only for serialization.
type QueueKey struct {
	Class string `json:"@class"`
	Hash  int    `json:"hash"`
	ID    int64  `json:"id"`
}

// QueueOfferResult defines the result of a queue Offer(). Exported only for serialization.
type QueueOfferResult struct {
	Class  string `json:"@class"`
	ID     int64  `json:"id"`
	Result int    `json:"result"`
}

// QueuePollResult defines the result of a queue Poll(). Exported only for serialization.
type QueuePollResult[V any] struct {
	Class   string `json:"@class"`
	ID      int64  `json:"id"`
	Element V      `json:"element"`
	Present bool   `json:"present"`
}

type baseQueueClient[V any] struct {
	cache         NamedMap[QueueKey, V]
	queueNameHash int
	ctx           context.Context
}

type namedQueue[V any] struct {
	*baseQueueClient[V]
}

type namedBlockingQueue[V any] struct {
	*baseQueueClient[V]
	queueCacheListener *queueCacheListener[V]
	notifyMutex        sync.Mutex
	notifier           *queueNotifier
}

// GetNamedQueue returns a new [NamedQueue].
func GetNamedQueue[V any](ctx context.Context, session *Session, queueName string) (NamedQueue[V], error) {
	var (
		existingQueue interface{}
		ok            bool
	)

	// protect updates to maps
	session.mapMutex.Lock()

	// check to see if we already have an entry for the queue
	if existingQueue, ok = session.queues[queueName]; ok {
		defer session.mapMutex.Unlock()

		existing, ok2 := existingQueue.(*NamedQueue[V])
		if !ok2 {
			// the casting failed so return an error indicating the queue exists with different type mappings
			return nil, getExistingError("NamedQueue", queueName)
		}

		session.debug("using existing NamedQueue", existing)
		return *existing, nil
	}

	// put a place-holder incase second go routine gets here
	session.queues[queueName] = nil
	session.mapMutex.Unlock()

	bq, err := newBaseQueueClient[V](ctx, session, queueName)
	if err != nil {
		return nil, err
	}

	queue := &namedQueue[V]{
		baseQueueClient: bq,
	}

	session.queues[queueName] = queue

	return queue, nil
}

// GetBlockingNamedQueue returns a new [NamedBlockingQueue].
func GetBlockingNamedQueue[V any](ctx context.Context, session *Session, queueName string) (NamedBlockingQueue[V], error) {
	var (
		existingQueue interface{}
		ok            bool
	)

	// protect updates to maps
	session.mapMutex.Lock()

	// check to see if we already have an entry for the queue
	if existingQueue, ok = session.queues[queueName]; ok {
		defer session.mapMutex.Unlock()
		existing, ok2 := existingQueue.(*NamedBlockingQueue[V])
		if !ok2 {
			// the casting failed so return an error indicating the queue exists with different type mappings
			return nil, getExistingError("NamedBlockingQueue", queueName)
		}

		session.debug("using existing NamedBlockingQueue", existing)
		return *existing, nil
	}

	// put a place-holder incase second go routine gets here
	session.queues[queueName] = nil
	session.mapMutex.Unlock()

	bq, err := newBaseQueueClient[V](ctx, session, queueName)
	if err != nil {
		return nil, err
	}
	session.mapMutex.Lock()
	defer session.mapMutex.Unlock()

	queue := &namedBlockingQueue[V]{
		baseQueueClient: bq,
		notifier:        newQueueNotifier(),
	}

	listener := newQueueCacheListener[V](queue)

	queue.queueCacheListener = listener

	err = bq.cache.AddListener(ctx, listener.listener)
	if err != nil {
		return nil, err
	}

	session.queues[queueName] = queue

	return queue, nil
}

func newBaseQueueClient[V any](ctx context.Context, session *Session, queueName string) (*baseQueueClient[V], error) {
	if session.closed {
		return nil, ErrClosed
	}

	bq := baseQueueClient[V]{}
	namedMap, hash, err := setupNamedMap[V](ctx, session, queueName)
	if err != nil {
		return nil, err
	}

	bq.cache = namedMap
	bq.ctx = ctx
	bq.queueNameHash = hash

	return &bq, nil
}

func setupNamedMap[V any](ctx context.Context, session *Session, queueName string) (NamedMap[QueueKey, V], int, error) {
	namedMap, err := GetNamedMap[QueueKey, V](session, queueName)
	if err != nil {
		return nil, 0, err
	}

	// add QueueKeyExtractor index
	err = AddIndex[QueueKey, V](ctx, namedMap, extractors.QueueKeyExtractor[V](), true)
	if err != nil {
		return nil, 0, err
	}

	// get the queue name hash
	var (
		hash *int
		key  = createQueueKey(1, 1) // mock
	)
	hash, err = Invoke[QueueKey, V, int](ctx, namedMap, key, processors.QueueNameHashProcessor(queueName))
	if err != nil {
		return nil, 0, err
	}

	return namedMap, *hash, nil
}

// NamedQueue

func (nq *namedQueue[V]) Offer(value V) error {
	return offer(nq.baseQueueClient, value, longMaxValue)
}

func (nq *namedQueue[V]) Poll() (*V, error) {
	return poll(nq.baseQueueClient, longMaxValue)
}

func (nq *namedQueue[V]) Peek() (*V, error) {
	return peek(nq.baseQueueClient, longMaxValue)
}

func (nq *namedQueue[V]) Close() error {
	return nil
}

func (nq *namedQueue[V]) GetName() string {
	return nq.cache.Name()
}

func (nq *namedQueue[V]) Size() (int, error) {
	return nq.cache.Size(nq.ctx)
}

// NamedBlockingQueue

func (bq *namedBlockingQueue[V]) Offer(value V) error {
	return offer(bq.baseQueueClient, value, longMaxValue)
}

// Poll attempts to call Poll() with the specified timeout. If err == [ErrQueueTimedOut] this means
// the operation timed out. If error is non nil, this indicates an error, otherwise
// the Poll() was successful and the pointer to the value is returned.
func (bq *namedBlockingQueue[V]) Poll(timeout time.Duration) (*V, error) {
	return bq.peekOrPoll(true, timeout)
}

func (bq *namedBlockingQueue[V]) Peek(timeout time.Duration) (*V, error) {
	return bq.peekOrPoll(false, timeout)
}

func (bq *namedBlockingQueue[V]) peekOrPoll(isPoll bool, timeout time.Duration) (*V, error) {
	for {
		var (
			err   error
			value *V
		)

		// lock while we are polling, specifically don't defer unlock
		bq.notifyMutex.Lock()
		if isPoll {
			value, err = poll(bq.baseQueueClient, longMaxValue)
		} else {
			value, err = peek(bq.baseQueueClient, longMaxValue)
		}
		bq.notifyMutex.Unlock()

		if err != nil {
			return nil, err
		}
		if value != nil {
			return value, nil
		}

		// no value, so wait on either event or timeout, subscribe for notification if any new entries arrive
		// mutex is placed around the subscribe(), unsubscribe() and notifyAll() to ensure we don't try and read
		// from a closed channel
		id, ch := bq.notifier.subscribe()

		select {
		case <-ch:
			// new item added, unsubscribe and attempt to poll() or peek() again,
			bq.notifier.unsubscribe(id)
		case <-time.After(timeout):
			// timeout
			bq.notifier.unsubscribe(id)
			return nil, ErrQueueTimedOut
		}
	}
}

func (bq *namedBlockingQueue[V]) Close() error {
	err := bq.cache.RemoveListener(bq.ctx, bq.queueCacheListener.listener)
	if err != nil {
		return err
	}

	return nil
}

func (bq *namedBlockingQueue[V]) GetName() string {
	return bq.cache.Name()
}

func (bq *namedBlockingQueue[V]) Size() (int, error) {
	return bq.cache.Size(bq.ctx)
}

// offer offers a value to the head or tail. id = longMaxValue for tail and
// id = longMinValue.
func offer[V any](nq *baseQueueClient[V], value V, id int64) error {
	key := createQueueKey(nq.queueNameHash, id)

	result, err := Invoke[QueueKey, V, QueueOfferResult](nq.ctx, nq.cache, key, processors.QueueOfferProcessor[V](value))
	if err != nil {
		return err
	}

	if result.Result == offerResultFailedCapacity {
		return ErrQueueFailedCapacity
	}

	if result.Result == offerResultSuccess {
		return nil
	}

	return ErrQueueFailedOffer
}

func poll[V any](nq *baseQueueClient[V], id int64) (*V, error) {
	key := createQueueKey(nq.queueNameHash, id)

	result, err := Invoke[QueueKey, V, QueuePollResult[V]](nq.ctx, nq.cache, key, processors.QueuePollProcessor())
	if err != nil {
		return nil, err
	}
	if result.Present {
		return &result.Element, nil
	}
	return nil, nil
}

func peek[V any](nq *baseQueueClient[V], id int64) (*V, error) {
	key := createQueueKey(nq.queueNameHash, id)

	result, err := Invoke[QueueKey, V, QueuePollResult[V]](nq.ctx, nq.cache, key, processors.QueuePeekProcessor())
	if err != nil {
		return nil, err
	}
	if result.Present {
		return &result.Element, nil
	}
	return nil, nil
}

// queueCacheListener is a [MapListener] to be called when any updates are done to the queue.
// this is for blocking clients.
type queueCacheListener[V any] struct {
	listener   MapListener[QueueKey, V]
	namedQueue *namedBlockingQueue[V]
}

func newQueueCacheListener[V any](namedQueue *namedBlockingQueue[V]) *queueCacheListener[V] {
	listener := queueCacheListener[V]{
		listener:   NewMapListener[QueueKey, V](),
		namedQueue: namedQueue,
	}

	listener.listener.OnInserted(func(_ MapEvent[QueueKey, V]) {
		// notify all registered listeners that an entry has been added to the Queue
		namedQueue.notifier.notifyAll()
	})

	return &listener
}

func createQueueKey(hash int, id int64) QueueKey {
	return QueueKey{Class: "internal.net.queue.model.QueueKey", ID: id, Hash: hash}
}

type queueNotifier struct {
	sync.Mutex
	listeners map[string]chan struct{}
}

func newQueueNotifier() *queueNotifier {
	return &queueNotifier{
		listeners: make(map[string]chan struct{}, 0),
	}
}

// subscribe subscribes to receive notifications about new queue messages.
func (qn *queueNotifier) subscribe() (string, chan struct{}) {
	qn.Lock()
	defer qn.Unlock()

	id := uuid.New().String()
	ch := make(chan struct{})
	qn.listeners[id] = ch
	return id, ch
}

// notifyAll() notifies all listeners registered.
func (qn *queueNotifier) notifyAll() {
	qn.Lock()
	defer qn.Unlock()

	for _, v := range qn.listeners {
		v <- struct{}{}
	}
}

// unsubscribe unsubscribes from receiving notifications for a uuid.
func (qn *queueNotifier) unsubscribe(uuid string) {
	qn.Lock()
	defer qn.Unlock()

	if v, ok := qn.listeners[uuid]; ok {
		close(v)
		delete(qn.listeners, uuid)
	}
}
