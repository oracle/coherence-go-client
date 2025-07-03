/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	pb1topics "github.com/oracle/coherence-go-client/v2/proto/topics"
	pb1 "github.com/oracle/coherence-go-client/v2/proto/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type NamedQueueType int

// ensureQueue issues the ensure queue request. This must be done before any requests to access queues can be issued.
func (m *streamManagerV1) ensureQueue(ctx context.Context, queue string, queueType NamedQueueType) (*int32, error) {
	return m.ensure(ctx, queue, m.session.queueIDMap, queueType)
}

// submitQueueRequest submits a request to the stream manager and returns named queue request.
func (m *streamManagerV1) submitQueueRequest(req *pb1.ProxyRequest, requestType pb1.NamedQueueRequestType) (proxyRequestChannel, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// create a channel for the response
	ch := make(chan responseMessage)

	r := proxyRequestChannel{ch: ch}

	// save the request in the map keyed by request id
	m.requests[req.Id] = r
	m.session.debugConnection("id: %v submit queue request: %v %v", req.Id, requestType, req)

	return r, m.eventStream.grpcStream.Send(req)
}

// submitTopicRequest submits a request to the stream manager and returns named topic request.
func (m *streamManagerV1) submitTopicRequest(req *pb1.ProxyRequest, requestType pb1topics.TopicServiceRequestType) (proxyRequestChannel, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// create a channel for the response
	ch := make(chan responseMessage)

	r := proxyRequestChannel{ch: ch}

	// save the request in the map keyed by request id
	m.requests[req.Id] = r
	m.session.debugConnection("id: %v submit queue request: %v %v", req.Id, requestType, req)

	return r, m.eventStream.grpcStream.Send(req)
}

// genericQueueRequest issues a generic request that is further defined by the reqType.
func (m *streamManagerV1) genericQueueRequest(ctx context.Context, reqType pb1.NamedQueueRequestType, queue string) error {
	req, err := m.newGenericNamedQueueRequest(queue, reqType)
	if err != nil {
		return err
	}

	requestType, err := m.submitQueueRequest(req, reqType)
	if err != nil {
		return err
	}

	// we must now wait for a response
	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// remove the entry from the channel
	defer m.cleanupRequest(req.Id)

	if reqType == pb1.NamedQueueRequestType_Destroy {
		// special case for destroy
		return nil
	}

	_, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return err1
	}

	return nil
}

// size returns the size of a cache
func (m *streamManagerV1) sizeQueue(ctx context.Context, queue string) (int32, error) {
	req, err := m.newGenericNamedQueueRequest(queue, pb1.NamedQueueRequestType_Size)
	if err != nil {
		return 0, err
	}

	requestType, err := m.submitQueueRequest(req, pb1.NamedQueueRequestType_Size)
	if err != nil {
		return 0, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// remove the entry from the channel
	defer m.cleanupRequest(req.Id)

	return m.returnSizeRequest(newCtx, requestType.ch)
}

func (m *streamManagerV1) newOfferTail(reqType pb1.NamedQueueRequestType, queue string, value []byte) (*pb1.ProxyRequest, error) {
	bytesValue := &wrapperspb.BytesValue{Value: value}

	anyReq, err := anypb.New(bytesValue)
	if err != nil {
		return nil, err
	}

	return m.newWrapperProxyQueueRequest(queue, reqType, anyReq)
}

func (m *streamManagerV1) returnSizeRequest(newCtx context.Context, ch chan responseMessage) (int32, error) {
	result, err := waitForResponse(newCtx, ch)
	if err != nil {
		return 0, err
	}

	var message = &wrapperspb.Int32Value{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("sizeResponse", err)
		return 0, err
	}

	return message.Value, nil
}

// genericBoolValue returns a boolean value based upon a request type.
func (m *streamManagerV1) genericBoolValueQueue(ctx context.Context, reqType pb1.NamedQueueRequestType, cache string) (bool, error) {
	req, err := m.newGenericNamedQueueRequest(cache, reqType)
	if err != nil {
		return false, err
	}

	requestType, err := m.submitQueueRequest(req, reqType)
	if err != nil {
		return false, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return false, err1
	}

	return unwrapBool(result)
}
