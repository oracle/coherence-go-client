/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"container/list"
	"context"
	"sync"
)

var (
	_ keyPageIterator[int, string]   = &streamedKeyIteratorV1[int, string]{}
	_ entryPageIterator[int, string] = &streamedEntryIteratorV1[int, string]{}
	_ valuePageIterator[int, string] = &streamedValueIteratorV1[int, string]{}
)

type streamedKeyIteratorV1[K comparable, V any] struct {
	exhausted bool
	dataList  *list.List
	ctx       context.Context
	bc        *baseClient[K, V]
	cookie    []byte
	sync.RWMutex
}

func newKeyPageIteratorV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) keyPageIterator[K, V] {
	iter := &streamedKeyIteratorV1[K, V]{
		exhausted: false,
		dataList:  list.New(),
		ctx:       ctx,
		bc:        bc,
		cookie:    make([]byte, 0),
	}

	return iter
}

func (it *streamedKeyIteratorV1[K, V]) Next() (*K, error) {
	it.Lock()
	defer it.Unlock()

	// attempt to get next page of data if we have none
	if it.dataList.Len() == 0 && !it.exhausted {
		if err := it.getNextPage(); err != nil {
			// could not get next page of data
			return nil, err
		}
	}

	// if we are exhausted and no data, indicate this in error
	if it.exhausted && it.dataList.Len() == 0 {
		return nil, ErrDone
	}

	// we have at least 1 entry in dataList
	first := it.dataList.Front()
	key := first.Value.(K)

	// remove the first entry
	it.dataList.Remove(first)

	return &key, nil
}

// getNextPage retrieves the next page of data from the gRPC connection and sets the
// current state to reflect the result of the operation.
func (it *streamedKeyIteratorV1[K, V]) getNextPage() error {
	var (
		err   = it.bc.ensureClientConnection()
		key   *K
		first = true
	)

	if err != nil {
		return err
	}

	newCtx, cancel := it.bc.session.ensureContext(it.ctx)
	if cancel != nil {
		defer cancel()
	}

	ch, err := it.bc.session.v1StreamManagerCache.keyPage(newCtx, it.bc.name, it.cookie)
	if err != nil {
		return err
	}

	for v := range ch {
		if first {
			// first entry will always be the cookie so store back in 'it.cookie' to be sent with next request
			it.cookie = v.Cookie
			if it.cookie == nil {
				it.exhausted = true
			}
			first = false
			continue
		}

		// no error so deserialize the key and value
		if key, err = it.bc.keySerializer.Deserialize(v.Key); err != nil {
			return err
		}

		it.dataList.PushBack(*key)
	}

	if it.dataList.Len() == 0 {
		it.exhausted = true
	}

	return nil
}

func newEntryPageIteratorV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) entryPageIterator[K, V] {
	iter := &streamedEntryIteratorV1[K, V]{
		exhausted: false,
		dataList:  list.New(),
		ctx:       ctx,
		bc:        bc,
		cookie:    make([]byte, 0),
	}

	return iter
}

type streamedEntryIteratorV1[K comparable, V any] struct {
	exhausted bool
	dataList  *list.List
	ctx       context.Context
	bc        *baseClient[K, V]
	cookie    []byte
	sync.RWMutex
}

func (it *streamedEntryIteratorV1[K, V]) Next() (*Entry[K, V], error) {
	it.Lock()
	defer it.Unlock()

	// attempt to get next page of data if we have none
	if it.dataList.Len() == 0 && !it.exhausted {
		if err := it.getNextPage(); err != nil {
			// could not get next page of data
			return nil, err
		}
	}

	// if we are exhausted and no data, indicate this in error
	if it.exhausted && it.dataList.Len() == 0 {
		return nil, ErrDone
	}

	// we have at least 1 entry in dataList
	first := it.dataList.Front()
	entry := first.Value
	entryValue := entry.(Entry[K, V])

	// remove the first entry
	it.dataList.Remove(first)

	return &entryValue, nil
}

// getNextPage retrieves the next page of data from the gRPC connection and sets the
// current state to reflect the result of the operation.
func (it *streamedEntryIteratorV1[K, V]) getNextPage() error {
	var (
		err   = it.bc.ensureClientConnection()
		first = true
		key   *K
		value *V
	)

	if err != nil {
		return err
	}

	newCtx, cancel := it.bc.session.ensureContext(it.ctx)
	if cancel != nil {
		defer cancel()
	}

	// reset the data
	it.dataList = list.New()

	ch, err := it.bc.session.v1StreamManagerCache.keyAndValuePage(newCtx, it.bc.name, it.cookie)
	if err != nil {
		return err
	}

	for v := range ch {
		if first {
			// first entry will always be the cookie so store back in 'it.cookie' to be sent with next request
			it.cookie = v.Cookie
			if it.cookie == nil {
				it.exhausted = true
			}
			first = false
			continue
		}

		// no error so deserialize the key and value
		if key, err = it.bc.keySerializer.Deserialize(v.Key); err != nil {
			return err
		}

		if value, err = it.bc.valueSerializer.Deserialize(v.Value); err != nil {
			return err
		}

		it.dataList.PushBack(Entry[K, V]{Key: *key, Value: *value})
	}

	if it.dataList.Len() == 0 {
		it.exhausted = true
	}

	return nil
}

type streamedValueIteratorV1[K comparable, V any] struct {
	entryIterator *streamedEntryIteratorV1[K, V]
}

func newValuePageIteratorV1[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) valuePageIterator[K, V] {
	internalIter := &streamedEntryIteratorV1[K, V]{
		exhausted: false,
		dataList:  list.New(),
		ctx:       ctx,
		bc:        bc,
		cookie:    make([]byte, 0),
	}

	// use an streamedEntryIterator but only return the value
	iter := &streamedValueIteratorV1[K, V]{entryIterator: internalIter}

	return iter
}

func (it *streamedValueIteratorV1[K, V]) Next() (*V, error) {
	// call the entry iterator but only return the value
	entry, err := it.entryIterator.Next()

	if err == ErrDone {
		return nil, ErrDone
	}

	return &entry.Value, nil
}
