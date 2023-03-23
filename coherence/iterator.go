/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"container/list"
	"context"
	"errors"
	pb "github.com/oracle/coherence-go-client/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"io"
	"sync"
)

var (
	_ KeyPageIterator[int, string]   = &streamedKeyIterator[int, string]{}
	_ EntryPageIterator[int, string] = &streamedEntryIterator[int, string]{}
	_ ValuePageIterator[int, string] = &streamedValueIterator[int, string]{}

	// ErrDone indicates that there are no more entries to return.
	ErrDone = errors.New("iterator done")
)

// KeyPageIterator defines an iterator of type K which is used to return
// values from KeySet() function. This iterator pages data internally so that
// if this iterator is called on a large cache it does not end up returning
// every key at once and potentially causing memory pressure.
//
// You can keep calling .Next() until err returns coherence.ErrDone, which indicates no more keys to iterate.
type KeyPageIterator[K comparable, V any] interface {
	// Next returns the next key and error, if error is ErrDone then this means
	// there are no more keys, otherwise it means an actual error.
	Next() (*K, error)
}

type streamedKeyIterator[K comparable, V any] struct {
	KeyPageIterator[K, V]
	exhausted bool
	dataList  *list.List
	ctx       context.Context
	bc        *baseClient[K, V]
	cookie    []byte
	sync.RWMutex
}

func newKeyPageIterator[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) KeyPageIterator[K, V] {
	iter := &streamedKeyIterator[K, V]{
		exhausted: false,
		dataList:  list.New(),
		ctx:       ctx,
		bc:        bc,
		cookie:    make([]byte, 0),
	}

	return iter
}

func (it *streamedKeyIterator[K, V]) Next() (*K, error) {
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
func (it *streamedKeyIterator[K, V]) getNextPage() error {
	var (
		err      = it.bc.ensureClientConnection()
		client   pb.NamedCacheService_NextKeySetPageClient
		m        = new(wrapperspb.BytesValue)
		response *K
		first    = true
	)

	if err != nil {
		return err
	}

	request := &pb.PageRequest{Scope: it.bc.sessionOpts.Scope, Cache: it.bc.name, Format: it.bc.format, Cookie: it.cookie}

	if client, err = it.bc.client.NextKeySetPage(it.ctx, request); err != nil {
		return err
	}

	// reset the data
	it.dataList = list.New()

	for {
		m, err = client.Recv()
		if err == io.EOF {
			// end of stream
			break
		} else if err != nil {
			return err
		}

		if first {
			// first entry will always be the cookie so store back in 'it.cookie' to be sent with next request
			it.cookie = m.Value
			if it.cookie == nil {
				it.exhausted = true
			}
			first = false
		} else {
			// no error so deserialize the key
			if response, err = it.bc.keySerializer.Deserialize(m.Value); err != nil {
				return err
			}

			it.dataList.PushBack(*response)
		}
	}

	if it.dataList.Len() == 0 {
		it.exhausted = true
	}

	return nil
}

// EntryPageIterator defines an iterator of type Entry which is used to return
// entries from EntrySet() function. This iterator pages data internally so that
// if this iterator is called on a large cache it does not end up returning
// every entry at once and potentially causing memory pressure.
//
// You can keep calling .Next() until err returns coherence.ErrDone, which indicates no more entries to iterate.
type EntryPageIterator[K comparable, V any] interface {
	// Next returns the next entry and error, if error is ErrDone then this means
	// there are no more entries, otherwise it means an actual error.
	Next() (*Entry[K, V], error)
}

func newEntryPageIterator[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) EntryPageIterator[K, V] {
	iter := &streamedEntryIterator[K, V]{
		exhausted: false,
		dataList:  list.New(),
		ctx:       ctx,
		bc:        bc,
		cookie:    make([]byte, 0),
	}

	return iter
}

type streamedEntryIterator[K comparable, V any] struct {
	EntryPageIterator[K, V]
	exhausted bool
	dataList  *list.List
	ctx       context.Context
	bc        *baseClient[K, V]
	cookie    []byte
	sync.RWMutex
}

func (it *streamedEntryIterator[K, V]) Next() (*Entry[K, V], error) {
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
func (it *streamedEntryIterator[K, V]) getNextPage() error {
	var (
		err    = it.bc.ensureClientConnection()
		client pb.NamedCacheService_NextEntrySetPageClient
		m      = new(pb.EntryResult)
		key    *K
		value  *V
		first  = true
	)

	if err != nil {
		return err
	}

	request := &pb.PageRequest{Scope: it.bc.sessionOpts.Scope, Cache: it.bc.name, Format: it.bc.format, Cookie: it.cookie}

	if client, err = it.bc.client.NextEntrySetPage(it.ctx, request); err != nil {
		return err
	}

	// reset the data
	it.dataList = list.New()

	for {
		m, err = client.Recv()
		if err == io.EOF {
			// end of stream
			break
		} else if err != nil {
			return err
		}

		if first {
			// first entry will always be the cookie so store back in 'it.cookie' to be sent with next request
			it.cookie = m.Cookie
			if it.cookie == nil {
				it.exhausted = true
			}
			first = false
		} else {
			// no error so deserialize the key
			if key, err = it.bc.keySerializer.Deserialize(m.Key); err != nil {
				return err
			}

			if value, err = it.bc.valueSerializer.Deserialize(m.Value); err != nil {
				return err
			}

			it.dataList.PushBack(Entry[K, V]{Key: *key, Value: *value})
		}
	}

	if it.dataList.Len() == 0 {
		it.exhausted = true
	}

	return nil
}

// ValuePageIterator defines an iterator of type V which is used to return
// values from Values() function. This iterator pages data internally so that
// if this iterator is called on a large cache it does not end up returning
// every value at once and potentially causing memory pressure.
//
// You can keep calling .Next() until err returns coherence.ErrDone, which indicates no more entries to iterate.
type ValuePageIterator[K comparable, V any] interface {
	// Next returns the next value and error, if error is ErrDone then this means
	// there are no more keys, otherwise it means an actual error.
	Next() (*V, error)
}

type streamedValueIterator[K comparable, V any] struct {
	ValuePageIterator[K, V]
	entryIterator *streamedEntryIterator[K, V]
}

func newValuePageIterator[K comparable, V any](ctx context.Context, bc *baseClient[K, V]) ValuePageIterator[K, V] {
	internalIter := &streamedEntryIterator[K, V]{
		exhausted: false,
		dataList:  list.New(),
		ctx:       ctx,
		bc:        bc,
		cookie:    make([]byte, 0),
	}

	// use an streamedEntryIterator but only return the value
	iter := &streamedValueIterator[K, V]{entryIterator: internalIter}

	return iter
}

func (it *streamedValueIterator[K, V]) Next() (*V, error) {
	// call the entry iterator but only return the value
	entry, err := it.entryIterator.Next()

	if err == ErrDone {
		return nil, ErrDone
	}

	return &entry.Value, nil
}
