/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"errors"
	"fmt"
	"github.com/oracle/coherence-go-client/proto/v1"
	pb1 "github.com/oracle/coherence-go-client/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

type V1ProxyProtocol string

const (
	protocolVersion                      = 1
	cacheServiceProtocol V1ProxyProtocol = "CacheService"
)

// responseMessages is a response received by a waiting client.
type responseMessage struct {
	message            *anypb.Any
	namedCacheResponse *pb1.NamedCacheResponse
	complete           bool
	err                *string
}

func (rm responseMessage) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("responseMessage{complete=%v, err=%v", rm.complete, rm.err))

	if rm.namedCacheResponse != nil {
		sb.WriteString(fmt.Sprintf(", cacheId=%v", rm.namedCacheResponse.CacheId))
	}

	sb.WriteString("}")
	return sb.String()
}

// namedCacheRequest holds the type of request and response messages channel.
type namedCacheRequest struct {
	requestType pb1.NamedCacheRequestType
	ch          chan responseMessage
}

type streamManagerV1 struct {
	session       *Session
	mutex         sync.RWMutex
	eventStream   *eventStreamV1
	proxyProtocol V1ProxyProtocol
	requests      map[int64]namedCacheRequest
}

type eventStreamV1 struct {
	grpcStream v1.ProxyService_SubChannelClient
	cancel     func()
}

func newStreamManagerV1(session *Session, proxyProtocol V1ProxyProtocol) (*streamManagerV1, error) {
	manager := streamManagerV1{
		session:       session,
		proxyProtocol: proxyProtocol,
		requests:      make(map[int64]namedCacheRequest, 0),
	}

	_, err := manager.ensureStream()
	if err != nil {
		return nil, err
	}
	return &manager, nil
}

func (m *streamManagerV1) ensureStream() (*eventStreamV1, error) {
	if m.eventStream == nil {
		m.mutex.Lock()
		defer m.mutex.Unlock()

		ctx, cancel := context.WithCancel(context.Background())

		proxyClient := pb1.NewProxyServiceClient(m.session.conn)
		grpcStream, err := proxyClient.SubChannel(ctx)

		if err != nil {
			cancel()
			return nil, err
		}

		v1EventsStream := eventStreamV1{grpcStream: grpcStream, cancel: cancel}
		m.eventStream = &v1EventsStream

		// generate and send init request
		initRequest := m.newInitRequest()

		err = m.eventStream.grpcStream.Send(initRequest)
		if err != nil {
			cancel()
			return nil, err
		}

		// we must receive a proxy response
		proxyResponse, err := m.eventStream.grpcStream.Recv()
		if err != nil || proxyResponse == nil {
			m.session.debugGrpc("received error", err)
			cancel()
			return nil, err
		}

		// check that we received an InitResponse
		response := proxyResponse.GetInit()
		if response == nil {
			if err != nil {
				cancel()
				return nil, errors.New("did not receive an InitResponse")
			}
		}

		m.session.debugGrpc(getInitDescription(response))

		// goroutine to handle MapEventResponse instances returned
		// from event stream
		go func(m *streamManagerV1) {
			for {
				response1, err1 := m.eventStream.grpcStream.Recv()
				if err1 == io.EOF {
					return
				}
				if err1 != nil {
					statusLocal := status.Code(err1)
					if statusLocal != codes.Canceled {
						// only log if it's not a cancelled error as this is just the client closing
						log.Printf("event stream recv failed: %s\n", err1)
					}
					cancel()
					return
				}

				id := response1.GetId()
				//response1.GetResponse()
				if h := response1.GetHeartbeat(); h != nil {
					m.session.debugGrpc("received heartbeat", h)
				} else {
					var resp responseMessage

					if c := response1.GetComplete(); c != nil {
						resp.complete = true
					}
					if e := response1.GetError(); e != nil {
						resp.err = &e.Message
					}
					if msg := response1.GetMessage(); msg != nil {
						resp.message = msg
					}

					// unpack the type of response message
					if resp.message != nil {
						var message proto.Message //nolint:gosimple
						message = &pb1.NamedCacheResponse{}
						if err1 = resp.message.UnmarshalTo(message); err1 != nil {
							log.Println(getUnmarshallError("namedCacheResponse", err1))
						} else {
							// determine the type of message
							switch responseMsg := message.(type) {
							case *pb1.NamedCacheResponse:
								resp.namedCacheResponse = responseMsg
							}
						}
					}

					// process the incoming message and generate a response to the chanel
					// of the listening client waiting for the events
					m.processNamedCacheResponse(id, &resp)
				}
			}
		}(m)
	}

	return m.eventStream, nil
}

func (m *streamManagerV1) processNamedCacheResponse(reqID int64, resp *responseMessage) {
	if reqID == 0 {
		// this is a map event or cache lifecycle event so dispatch it
		// TODO: Dispatch event
		m.session.debugGrpc(resp)

		switch resp.namedCacheResponse.Type {
		case pb1.ResponseType_Destroyed:
			log.Printf("id=%v, destroyed, %v", reqID, resp)
		case pb1.ResponseType_Truncated:
			log.Printf("id=%v, truncated, %v", reqID, resp)
		case pb1.ResponseType_MapEvent:
			log.Printf("id=%v, mapevent, %v", reqID, resp)
		}

		return
	}

	m.session.debugGrpc("id", reqID, "process namedCacheResponse", resp)

	// write the response to the channel for the originating request
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if e, ok := m.requests[reqID]; ok {
		// request exists
		e.ch <- *resp
	} else {
		log.Printf("found request %v (%v) in response but no request exists", *resp, reqID)
	}
}

// submitRequest submits a request to the stream manager and returns named cache request.
func (m *streamManagerV1) submitRequest(req *pb1.ProxyRequest, requestType pb1.NamedCacheRequestType) (namedCacheRequest, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// create a channel for the response
	ch := make(chan responseMessage)

	r := namedCacheRequest{ch: ch, requestType: requestType}

	// save the request in the map keyed by request id
	m.requests[req.Id] = r

	m.session.debugGrpc("id", req.Id, "submit request", r.requestType)

	return r, m.eventStream.grpcStream.Send(req)
}

// ensureCache issues the ensure cache request. This must be done before any requests to access caches can be issues.
func (m *streamManagerV1) ensureCache(ctx context.Context, cache string) (*int32, error) {
	var (
		cacheID *int32
		err     error
	)
	req, err := m.newEnsureCacheRequest(cache)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_EnsureCache)
	if err != nil {
		return nil, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch, true)
	if err1 != nil {
		return nil, err1
	}

	var message = &wrapperspb.Int32Value{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("getResponse", err)
		return nil, err
	}

	// store the cache id in the session
	cacheID = &message.Value
	m.session.mapMutex.Lock()
	m.session.cacheIDMap[cache] = *cacheID
	m.session.mapMutex.Unlock()

	return cacheID, nil
}

// clearCache issues a truncate cache request.
func (m *streamManagerV1) truncateCache(ctx context.Context, cache string) error {
	return m.genericCacheRequest(ctx, pb1.NamedCacheRequestType_Truncate, cache)
}

// clearCache issues a clear cache request.
func (m *streamManagerV1) clearCache(ctx context.Context, cache string) error {
	return m.genericCacheRequest(ctx, pb1.NamedCacheRequestType_Clear, cache)
}

// destroyCache issues a clear cache request.
func (m *streamManagerV1) destroyCache(ctx context.Context, cache string) error {
	err := m.genericCacheRequest(ctx, pb1.NamedCacheRequestType_Destroy, cache)

	if err != nil {
		return err
	}

	// remove the cache from the map
	m.session.mapMutex.Lock()
	delete(m.session.cacheIDMap, cache)
	m.session.mapMutex.Unlock()

	return nil
}

// genericCacheRequest issues a generic request that is further defined by the reqType.
func (m *streamManagerV1) genericCacheRequest(ctx context.Context, reqType pb1.NamedCacheRequestType, cache string) error {
	var (
		err error
	)
	req, err := m.newGenericNamedCacheRequest(cache, reqType)
	if err != nil {
		return err
	}

	requestType, err := m.submitRequest(req, reqType)
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

	_, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return err1
	}

	return nil
}

// size returns the size of a cache
func (m *streamManagerV1) size(ctx context.Context, cache string) (int32, error) {
	var (
		err   error
		value int32
	)
	req, err := m.newGenericNamedCacheRequest(cache, pb1.NamedCacheRequestType_Size)
	if err != nil {
		return 0, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_Size)
	if err != nil {
		return 0, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// remove the entry from the channel
	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return value, err1
	}

	// we must have result so process it
	var message = &wrapperspb.Int32Value{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("getResponse", err)
		return value, err
	}

	return message.Value, nil
}

func (m *streamManagerV1) isEmpty(ctx context.Context, cache string) (bool, error) {
	return m.genericBoolValue(ctx, pb1.NamedCacheRequestType_IsEmpty, cache)
}

func (m *streamManagerV1) isReady(ctx context.Context, cache string) (bool, error) {
	return m.genericBoolValue(ctx, pb1.NamedCacheRequestType_IsReady, cache)
}

// genericBoolValue returns a boolean value based upon a request type.
func (m *streamManagerV1) genericBoolValue(ctx context.Context, reqType pb1.NamedCacheRequestType, cache string) (bool, error) {
	var (
		err   error
		value bool
	)
	req, err := m.newGenericNamedCacheRequest(cache, reqType)
	if err != nil {
		return value, err
	}

	requestType, err := m.submitRequest(req, reqType)
	if err != nil {
		return value, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// remove the entry from the channel
	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return value, err1
	}

	// we must have result so process it
	var message = &wrapperspb.BoolValue{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("getResponse", err)
		return value, err
	}

	return message.Value, nil
}

// get issues a get request for a given key and returns the bytes value which could be nil.
func (m *streamManagerV1) get(ctx context.Context, cache string, key []byte) (*[]byte, error) {
	req, err := m.newGetRequest(cache, key)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_Get)
	if err != nil {
		return nil, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// remove the entry from the channel
	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return nil, err1
	}

	var message = &pb1.OptionalValue{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("getResponse", err)
		return nil, err
	}
	// set the return value based upon the optional
	if message.Present {
		return &message.Value, nil
	}
	// no value present, return nil
	return nil, nil
}

// get issues a put request for a given key, value and ttl and returns the old value
func (m *streamManagerV1) put(ctx context.Context, cache string, key []byte, value []byte, ttl time.Duration) (*[]byte, error) {
	req, err := m.newPutRequest(cache, key, value, ttl)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_Put)
	if err != nil {
		return nil, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return nil, err1
	}

	// unpack the optional value
	var message = &wrapperspb.BytesValue{}

	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("putResponse", err)
		cancel()
		return nil, err
	}

	return &message.Value, nil
}

func (m *streamManagerV1) cleanupRequest(reqID int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	requestType := m.requests[reqID]
	delete(m.requests, reqID)
	close(requestType.ch)
}

// defaultFunction returns the default named cache message
var defaultFunction = func(resp responseMessage) *anypb.Any {
	if resp.namedCacheResponse != nil && resp.namedCacheResponse.Message != nil {
		return resp.namedCacheResponse.Message
	}
	return nil
}

// waitForResponse waits for a response to a request and returns the proto message and any error.
func waitForResponse(newCtx context.Context, ch chan responseMessage, ensureCache ...bool) (*anypb.Any, error) {
	var (
		err           error
		result        *anypb.Any
		isEnsureCache = len(ensureCache) != 0
	)

	// wait until we get a complete request or we timeout
	for {
		// wait on the channel
		select {
		case resp := <-ch:
			if resp.err != nil {
				err = fmt.Errorf("error: %v", *resp.err)
			}

			if resp.namedCacheResponse != nil {
				// unpack the result message if we have valid named cache response
				if !isEnsureCache {
					// standard case where we want to return the names cache result message
					result = defaultFunction(resp)
				} else {
					// special case for ensureCache, return the cache id and it will be handled by ensureCache
					result, err = anypb.New(wrapperspb.Int32(resp.namedCacheResponse.CacheId))
				}
			}

			if resp.complete {
				return result, err
			}
		case <-newCtx.Done():
			return result, newCtx.Err()
		}
	}
}

func getInitDescription(r *pb1.InitResponse) string {
	return fmt.Sprintf("initResponse{version=%s, protocolVersion=%d, proxyMemberId=%d, %v}",
		r.GetVersion(), r.GetProtocolVersion(), r.GetProxyMemberId(), string(r.GetUuid()))
}

func getCacheIDMessage(cache string) error {
	return fmt.Errorf("unable to find cache id for cache named [%s]", cache)
}

func getUnmarshallError(message string, err error) error {
	return fmt.Errorf("unable to unpack %v message %v", message, err)
}
