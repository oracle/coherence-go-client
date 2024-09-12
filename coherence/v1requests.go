/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	pb1 "github.com/oracle/coherence-go-client/proto/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"time"
)

var (
	emptyByte = make([]byte, 0)
)

func (m *streamManagerV1) newInitRequest() *pb1.ProxyRequest {
	req := pb1.InitRequest{
		ClientUuid:               m.session.sessionID[:],
		Scope:                    m.session.sessOpts.Scope,
		Format:                   m.session.sessOpts.Format,
		ProtocolVersion:          protocolVersion,
		SupportedProtocolVersion: protocolVersion,
		Protocol:                 string(m.proxyProtocol),
	}

	pr := pb1.ProxyRequest{
		Id: m.session.NextRequestID(),
		Request: &pb1.ProxyRequest_Init{
			Init: &req,
		},
	}

	return &pr
}

func (m *streamManagerV1) newEnsureCacheRequest(cache string) (*pb1.ProxyRequest, error) {
	req := &pb1.EnsureCacheRequest{
		Cache: cache,
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest("", pb1.NamedCacheRequestType_EnsureCache, anyReq)
}

func (m *streamManagerV1) newGenericNamedCacheRequest(cache string, requestType pb1.NamedCacheRequestType) (*pb1.ProxyRequest, error) {
	return m.newWrapperProxyRequest(cache, requestType, nil)
}

func (m *streamManagerV1) newGetRequest(cache string, key []byte) (*pb1.ProxyRequest, error) {
	return m.newSingleValueBasedRequest(pb1.NamedCacheRequestType_Get, cache, key)
}

func (m *streamManagerV1) newRemoveRequest(cache string, key []byte) (*pb1.ProxyRequest, error) {
	return m.newSingleValueBasedRequest(pb1.NamedCacheRequestType_Remove, cache, key)
}

func (m *streamManagerV1) newContainsKeyRequest(cache string, key []byte) (*pb1.ProxyRequest, error) {
	return m.newSingleValueBasedRequest(pb1.NamedCacheRequestType_ContainsKey, cache, key)
}

func (m *streamManagerV1) newContainsValueRequest(cache string, value []byte) (*pb1.ProxyRequest, error) {
	return m.newSingleValueBasedRequest(pb1.NamedCacheRequestType_ContainsValue, cache, value)
}

// newSingleValueBasedRequest creates a request where the message contains a single bytes value.
func (m *streamManagerV1) newSingleValueBasedRequest(reqType pb1.NamedCacheRequestType, cache string, key []byte) (*pb1.ProxyRequest, error) {
	keyBytes := wrapperspb.Bytes(key)

	anyReq, err := anypb.New(keyBytes)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, reqType, anyReq)
}

func (m *streamManagerV1) newRemoveMappingRequest(cache string, key []byte, value []byte) (*pb1.ProxyRequest, error) {
	return m.newKeyAndValueBasedRequest(pb1.NamedCacheRequestType_RemoveMapping, cache, key, value)
}

func (m *streamManagerV1) newReplaceRequest(cache string, key []byte, value []byte) (*pb1.ProxyRequest, error) {
	return m.newKeyAndValueBasedRequest(pb1.NamedCacheRequestType_Replace, cache, key, value)
}

func (m *streamManagerV1) newContainsEntryRequest(cache string, key []byte, value []byte) (*pb1.ProxyRequest, error) {
	return m.newKeyAndValueBasedRequest(pb1.NamedCacheRequestType_ContainsEntry, cache, key, value)
}

// newKeyAndValueBasedRequest creates a request where the message contains a BinaryKeyAndValue
func (m *streamManagerV1) newKeyAndValueBasedRequest(reqType pb1.NamedCacheRequestType, cache string, key []byte, value []byte) (*pb1.ProxyRequest, error) {
	request := &pb1.BinaryKeyAndValue{
		Key:   key,
		Value: value,
	}

	anyReq, err := anypb.New(request)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, reqType, anyReq)
}

func (m *streamManagerV1) newPutRequest(reqType pb1.NamedCacheRequestType, cache string, key []byte, value []byte, ttl time.Duration) (*pb1.ProxyRequest, error) {
	millis := ttl.Milliseconds()
	putRequest := &pb1.PutRequest{
		Key:   key,
		Value: value,
		Ttl:   &millis,
	}

	anyReq, err := anypb.New(putRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, reqType, anyReq)
}

func (m *streamManagerV1) newIndexRequest(cache string, add bool, binExtractor []byte, sorted *bool, binComparator []byte) (*pb1.ProxyRequest, error) {
	indexRequest := &pb1.IndexRequest{
		Add:        add,
		Extractor:  binExtractor,
		Sorted:     sorted,
		Comparator: binComparator,
	}

	anyReq, err := anypb.New(indexRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_Index, anyReq)
}

func (m *streamManagerV1) newAggregateRequest(cache string, agent []byte, keysOrFilter *pb1.KeysOrFilter) (*pb1.ProxyRequest, error) {
	aggregateRequest := &pb1.ExecuteRequest{
		Agent: agent,
		Keys:  keysOrFilter,
	}

	anyReq, err := anypb.New(aggregateRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_Aggregate, anyReq)
}

func (m *streamManagerV1) newPutAllRequest(cache string, entries []*pb1.BinaryKeyAndValue, ttl time.Duration) (*pb1.ProxyRequest, error) {
	millis := ttl.Milliseconds()
	putAllRequest := &pb1.PutAllRequest{
		Entries: entries,
		Ttl:     &millis,
	}

	anyReq, err := anypb.New(putAllRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_PutAll, anyReq)
}

func (m *streamManagerV1) newGetAllRequest(cache string, keys [][]byte) (*pb1.ProxyRequest, error) {
	getAllRequest := &pb1.CollectionOfBytesValues{
		Values: keys,
	}

	anyReq, err := anypb.New(getAllRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_GetAll, anyReq)
}

func (m *streamManagerV1) newInvokeRequest(cache string, agent []byte, keysOrFilter *pb1.KeysOrFilter) (*pb1.ProxyRequest, error) {
	invokeRequest := &pb1.ExecuteRequest{
		Agent: agent,
		Keys:  keysOrFilter,
	}

	anyReq, err := anypb.New(invokeRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_Invoke, anyReq)
}

func (m *streamManagerV1) newEntrySetRequest(cache string, filter []byte) (*pb1.ProxyRequest, error) {
	entrySetRequest := &pb1.QueryRequest{
		Filter: filter,
	}

	anyReq, err := anypb.New(entrySetRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_QueryEntries, anyReq)
}

func (m *streamManagerV1) newValuesFilterRequest(cache string, filter []byte) (*pb1.ProxyRequest, error) {
	entrySetRequest := &pb1.QueryRequest{
		Filter: filter,
	}

	anyReq, err := anypb.New(entrySetRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_QueryValues, anyReq)
}

func (m *streamManagerV1) newKeySetRequest(cache string, filter []byte) (*pb1.ProxyRequest, error) {
	entrySetRequest := &pb1.QueryRequest{
		Filter: filter,
	}

	anyReq, err := anypb.New(entrySetRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_QueryKeys, anyReq)
}

func (m *streamManagerV1) newReplaceMappingRequest(cache string, key []byte, prevValue []byte, newValue []byte) (*pb1.ProxyRequest, error) {
	replaceMappingRequest := &pb1.ReplaceMappingRequest{
		Key:           key,
		PreviousValue: prevValue,
		NewValue:      newValue,
	}

	anyReq, err := anypb.New(replaceMappingRequest)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyRequest(cache, pb1.NamedCacheRequestType_ReplaceMapping, anyReq)
}

func (m *streamManagerV1) newProxyRequest(message *anypb.Any) *pb1.ProxyRequest {
	return &pb1.ProxyRequest{
		Id: m.session.NextRequestID(),
		Request: &pb1.ProxyRequest_Message{
			Message: message,
		},
	}
}

func newNamedCacheRequest(cacheID *int32, reqType pb1.NamedCacheRequestType, message *anypb.Any) (*anypb.Any, error) {
	req := &pb1.NamedCacheRequest{
		Type:    reqType,
		CacheId: cacheID,
		Message: message,
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return nil, err
	}
	return anyReq, nil
}

func (m *streamManagerV1) newWrapperProxyRequest(cache string, requestType pb1.NamedCacheRequestType, message *anypb.Any) (*pb1.ProxyRequest, error) {
	var cacheID *int32

	// validate the cache ID if it is not a ensure cache request
	if cache != "" {
		cacheID = m.session.getCacheID(cache)
		if cacheID == nil {
			return nil, getCacheIDMessage(cache)
		}
	}

	ncRequest, err := newNamedCacheRequest(cacheID, requestType, message)

	if err != nil {
		return nil, err
	}

	return m.newProxyRequest(ncRequest), nil
}
