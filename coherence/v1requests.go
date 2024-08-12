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

	return m.newWrapperProxyRequest(nil, pb1.NamedCacheRequestType_EnsureCache, anyReq)
}

func (m *streamManagerV1) newGenericNamedCacheRequest(cache string, requestType pb1.NamedCacheRequestType) (*pb1.ProxyRequest, error) {
	// retrieve the cache ID
	cacheID := m.session.getCacheID(cache)
	if cacheID == nil {
		return nil, getCacheIDMessage(cache)
	}

	return m.newWrapperProxyRequest(cacheID, requestType, nil)
}

func (m *streamManagerV1) newGetRequest(cache string, key []byte) (*pb1.ProxyRequest, error) {
	keyBytes := wrapperspb.Bytes(key)

	anyReq, err := anypb.New(keyBytes)
	if err != nil {
		return nil, err
	}

	// retrieve the cache ID
	cacheID := m.session.getCacheID(cache)
	if cacheID == nil {
		return nil, getCacheIDMessage(cache)
	}

	return m.newWrapperProxyRequest(cacheID, pb1.NamedCacheRequestType_Get, anyReq)
}

func (m *streamManagerV1) newPutRequest(cache string, key []byte, value []byte, ttl time.Duration) (*pb1.ProxyRequest, error) {
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

	// retrieve the cache ID
	cacheID := m.session.getCacheID(cache)
	if cacheID == nil {
		return nil, getCacheIDMessage(cache)
	}

	return m.newWrapperProxyRequest(cacheID, pb1.NamedCacheRequestType_Put, anyReq)
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

func (m *streamManagerV1) newWrapperProxyRequest(cacheID *int32, requestType pb1.NamedCacheRequestType, message *anypb.Any) (*pb1.ProxyRequest, error) {
	ncRequest, err := newNamedCacheRequest(cacheID, requestType, message)

	if err != nil {
		return nil, err
	}

	return m.newProxyRequest(ncRequest), nil
}
