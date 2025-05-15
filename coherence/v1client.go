/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"errors"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/proto/v1"
	pb1 "github.com/oracle/coherence-go-client/v2/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

// V1ProxyProtocol defines the types of proxy protocols such as "CacheService" or "QueueService".
type V1ProxyProtocol string

type logLevel int

var (
	ERROR   logLevel = 1
	WARNING logLevel = 2
	INFO    logLevel = 3 // Default
	DEBUG   logLevel = 4
	ALL     logLevel = 5 // all messages

	// current log level
	currentLogLevel int
)

const (
	protocolVersion                           = 1
	cacheServiceProtocol      V1ProxyProtocol = "CacheService"
	queueServiceProtocol      V1ProxyProtocol = "QueueService"
	errorFormat                               = "error: %v"
	responseDebug                             = "received response: %v"
	namedCacheResponseTypeURL                 = "type.googleapis.com/coherence.cache.v1.NamedCacheResponse"
	namedQueueResponseTypeURL                 = "type.googleapis.com/coherence.concurrent.queue.v1.NamedQueueResponse"
)

// responseMessages is a response received by a waiting client.
type responseMessage struct {
	message            *anypb.Any
	namedCacheResponse *pb1.NamedCacheResponse
	namedQueueResponse *pb1.NamedQueueResponse
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

// proxyRequestChannel holds response messages channel.
type proxyRequestChannel struct {
	ch chan responseMessage
}

// streamManagerV1 holds the data for a gRPC V1 connection.
type streamManagerV1 struct {
	session               *Session
	mutex                 sync.RWMutex
	eventStream           *eventStreamV1
	proxyProtocol         V1ProxyProtocol
	serializer            Serializer[any]
	requests              map[int64]proxyRequestChannel
	serverVersion         string
	serverProtocolVersion int32
	serverProxyMemberID   int32
}

type eventStreamV1 struct {
	grpcStream v1.ProxyService_SubChannelClient
	cancel     func()
}

func newStreamManagerV1(session *Session, proxyProtocol V1ProxyProtocol) (*streamManagerV1, error) {
	manager := streamManagerV1{
		session:       session,
		proxyProtocol: proxyProtocol,
		requests:      make(map[int64]proxyRequestChannel, 0),
		serializer:    NewSerializer[any](session.sessOpts.Format),
	}

	_, err := manager.ensureStream()
	if err != nil {
		return nil, err
	}
	session.firstConnectAttempted = true
	session.hasConnected = true

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
			m.session.debugConnection("error getting SubChannel: %v", err)
			cancel()
			return nil, err
		}

		v1EventsStream := eventStreamV1{grpcStream: grpcStream, cancel: cancel}
		m.eventStream = &v1EventsStream

		// generate and send init request
		initRequest := m.newInitRequest()
		m.session.debugConnection("initRequest: %v", initRequest)

		err = m.eventStream.grpcStream.Send(initRequest)
		if err != nil {
			m.session.debugConnection("error sending initRequest: %v", err)
			cancel()
			return nil, err
		}

		// we must receive a proxy response
		proxyResponse, err := m.eventStream.grpcStream.Recv()
		if err != nil || proxyResponse == nil {
			m.session.debugConnection("error receiving init response: %v", err)
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

		// save the server information received
		m.setServerInfo(response)
		m.session.debugConnection(getInitDescription(response))

		// goroutine to handle MapEventResponse instances returned
		// from event stream
		go func(m *streamManagerV1) {
			for {
				response1, err1 := m.eventStream.grpcStream.Recv()
				if err1 == io.EOF {
					m.session.debugConnection("received EOF, closing")
					cancel()
					return
				}
				if err1 != nil {
					statusLocal := status.Code(err1)
					m.session.debugConnection("closing connection: %v", statusLocal)
					if statusLocal != codes.Canceled {
						// only log if it's not a cancelled error as this is just the client closing
						logMessage(WARNING, "event stream recv failed: %s", err1)
					}
					cancel()
					return
				}

				id := response1.GetId()
				if h := response1.GetHeartbeat(); h != nil {
					m.session.debugConnection("received heartbeat: %v", h)
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

					m.processResponseMessage(id, &resp)
				}
			}
		}(m)
	}

	return m.eventStream, nil
}

func (m *streamManagerV1) processResponseMessage(id int64, resp *responseMessage) {
	if resp.message != nil {
		// Use TypeUrl to determine the message type and unmarshal accordingly
		switch resp.message.TypeUrl {
		case namedCacheResponseTypeURL:
			var namedCacheResponse pb1.NamedCacheResponse
			if err := resp.message.UnmarshalTo(&namedCacheResponse); err != nil {
				logMessage(WARNING, "%v", getUnmarshallError("namedCacheResponse", err))
				return
			}
			resp.namedCacheResponse = &namedCacheResponse
		case namedQueueResponseTypeURL:
			var namedQueueResponse pb1.NamedQueueResponse
			if err := resp.message.UnmarshalTo(&namedQueueResponse); err != nil {
				logMessage(WARNING, "%v", getUnmarshallError("namedQueueResponse", err))
				return
			}
			resp.namedQueueResponse = &namedQueueResponse

		default:
			logMessage(WARNING, "Unknown message type: %v", resp.message.TypeUrl)
			return
		}
	}

	m.processResponse(id, resp)
}

// logMessage logs a message only if the level <= currentLogLevel
func logMessage(level logLevel, format string, args ...any) {
	if int(level) <= currentLogLevel {
		log.Println(getLogMessage(level, format, args...))
	}
}

func getLogMessage(level logLevel, format string, args ...any) string {
	return fmt.Sprintf("%v ", level) + fmt.Sprintf(format, args...)
}

func (l logLevel) String() string {
	switch l {
	case ERROR:
		return "ERROR"
	case WARNING:
		return "WARNING"
	case INFO:
		return "INFO"
	case DEBUG:
		return "DEBUG"
	case ALL:
		return "ALL"
	default:
		return fmt.Sprintf("logLevel(%d)", int(l))
	}
}

func (m *streamManagerV1) setServerInfo(r *pb1.InitResponse) {
	m.serverVersion = r.GetVersion()
	m.serverProtocolVersion = r.GetProtocolVersion()
	m.serverProxyMemberID = r.GetProxyMemberId()
}

func (m *streamManagerV1) String() string {
	return fmt.Sprintf("Coherence version: %s, serverProtocolVersion: %d, proxyMemberId: %d", m.serverVersion, m.serverProtocolVersion, m.serverProxyMemberID)
}

func (m *streamManagerV1) processResponse(reqID int64, resp *responseMessage) {
	if reqID == 0 && resp.namedCacheResponse != nil {
		// this is a map event or cache lifecycle event for named cache so dispatch it
		m.session.debugConnection("Event, CacheId: %v, Type %v", resp.namedCacheResponse.CacheId, resp.namedCacheResponse.Type)

		// for a map event we must get the cache name and then looking the base client, so
		// we can send the event to the right place
		cacheID := resp.namedCacheResponse.CacheId
		if cacheID == 0 {
			logMessage(WARNING, "received an event %v with cacheID = 0", resp.namedCacheResponse.Type)
			return
		}

		cacheName := m.session.getCacheNameFromCacheID(cacheID)
		if cacheName == nil {
			logMessage(WARNING, "unable to find cache name from cache id %v it may have been released or destroyed", cacheID)
			return
		}

		// determine if the cache is a map or cache
		var client interface{}

		client = m.session.getNamedCacheClient(*cacheName)
		if client == nil {
			client = m.session.getNamedMapClient(*cacheName)
		}

		if eventSubmitter, ok := client.(EventSubmitter); ok {
			switch resp.namedCacheResponse.Type {
			case pb1.ResponseType_Destroyed:
				eventSubmitter.generateMapLifecycleEvent(client, Destroyed)
				m.session.debugConnection("id: %v %v %v", reqID, Destroyed, resp)
			case pb1.ResponseType_Truncated:
				eventSubmitter.generateMapLifecycleEvent(client, Truncated)
				m.session.debugConnection("id: %v %v %v", reqID, Truncated, resp)
			case pb1.ResponseType_MapEvent:
				mapEventMessage, err1 := unwrapMapEvent(resp.namedCacheResponse)
				if err1 != nil {
					logMessage(WARNING, "unable to unwrap MapEvent: %v", err1)
				} else {
					m.session.debugConnection("received mapEvent: %v", mapEventMessage)
					eventSubmitter.generateMapEvent(client, mapEventMessage)
				}
			}
		}

		return
	}

	// process queue event
	if reqID == 0 && resp.namedQueueResponse != nil {

		// find the queueName from the queueID
		queueName := m.session.queueIDMap.KeyFromValue(resp.namedQueueResponse.QueueId)
		if queueName == nil {
			m.session.debugConnection("cannot find queue for queue id %v", queueName)
		} else {
			if existingQueue, ok := m.session.queues[*queueName]; ok {
				if queueEventSubmitter, ok2 := existingQueue.(QueueEventSubmitter); ok2 {
					switch resp.namedQueueResponse.Type {
					case pb1.NamedQueueResponseType_Destroyed:
						queueEventSubmitter.generateQueueLifecycleEvent(existingQueue, QueueDestroyed)
					case pb1.NamedQueueResponseType_Truncated:
						queueEventSubmitter.generateQueueLifecycleEvent(existingQueue, QueueTruncated)
					}
				}
			}
		}
		return
	}

	m.session.debugConnection("id: %v Response: %v", reqID, resp)

	// received a named cache or queue response, so write the response to the channel for the originating request
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if e, ok := m.requests[reqID]; ok {
		// request exists
		e.ch <- *resp
	} else {
		m.session.debugConnection("found request %v (%v) in response but no request exists", *resp, reqID)
	}
}

// submitRequest submits a request to the stream manager and returns named cache request.
func (m *streamManagerV1) submitRequest(req *pb1.ProxyRequest, requestType pb1.NamedCacheRequestType) (proxyRequestChannel, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// create a channel for the response
	ch := make(chan responseMessage)

	r := proxyRequestChannel{ch: ch}

	// save the request in the map keyed by request id
	m.requests[req.Id] = r

	m.session.debugConnection("id: %v submit request: %v %v", req.Id, requestType, req)

	return r, m.eventStream.grpcStream.Send(req)
}

// ensureCache issues the ensure cache request. This must be done before any requests to access caches can be issued.
func (m *streamManagerV1) ensureCache(ctx context.Context, cache string) (*int32, error) {
	return m.ensure(ctx, cache, m.session.cacheIDMap /** -1 signifies cache **/, -1)
}

// ensure ensures a queue or cache.
func (m *streamManagerV1) ensure(ctx context.Context, name string, IDMap safeMap[string, int32], queueType NamedQueueType) (*int32, error) {
	var (
		ID          *int32
		err         error
		req         *v1.ProxyRequest
		isQueue     = queueType >= 0
		requestType proxyRequestChannel
	)

	if isQueue {
		req, err = m.newEnsureQueueRequest(name, queueType)
	} else {
		// cache
		req, err = m.newEnsureCacheRequest(name)
	}

	if err != nil {
		return nil, err
	}

	if isQueue {
		requestType, err = m.submitQueueRequest(req, pb1.NamedQueueRequestType_EnsureQueue)
	} else {
		requestType, err = m.submitRequest(req, pb1.NamedCacheRequestType_EnsureCache)
	}

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
		err = getUnmarshallError("ensure response", err)
		return nil, err
	}

	// store the cache id in the session , no lock required as already locked
	ID = &message.Value
	IDMap.Add(name, *ID)

	return ID, nil
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
	m.session.cacheIDMap.Remove(cache)

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

	if reqType == pb1.NamedCacheRequestType_Destroy {
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
func (m *streamManagerV1) size(ctx context.Context, cache string) (int32, error) {
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

	return m.returnSizeRequest(newCtx, requestType.ch)
}

func (m *streamManagerV1) isEmpty(ctx context.Context, cache string) (bool, error) {
	return m.genericBoolValue(ctx, pb1.NamedCacheRequestType_IsEmpty, cache)
}

func (m *streamManagerV1) isReady(ctx context.Context, cache string) (bool, error) {
	return m.genericBoolValue(ctx, pb1.NamedCacheRequestType_IsReady, cache)
}

// genericBoolValue returns a boolean value based upon a request type.
func (m *streamManagerV1) genericBoolValue(ctx context.Context, reqType pb1.NamedCacheRequestType, cache string) (bool, error) {
	req, err := m.newGenericNamedCacheRequest(cache, reqType)
	if err != nil {
		return false, err
	}

	requestType, err := m.submitRequest(req, reqType)
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

// mapListenerRequest issues a map listener request to add or remove a key or filter listener.
func (m *streamManagerV1) mapListenerRequest(ctx context.Context, cache string, subscribe bool, keyOrFilter *pb1.KeyOrFilter,
	lite bool, synchronous bool, priming bool, filterID int64) error {

	req, err := m.newMapListenerRequest(cache, subscribe, keyOrFilter, filterID, lite, synchronous, priming, nil)
	if err != nil {
		return err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_MapListener)
	if err != nil {
		return err
	}

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

func (m *streamManagerV1) replaceMapping(ctx context.Context, cache string, key []byte, prevValue []byte, newValue []byte) (bool, error) {
	req, err := m.newReplaceMappingRequest(cache, key, prevValue, newValue)
	if err != nil {
		return false, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_ReplaceMapping)
	if err != nil {
		return false, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// remove the entry from the channel
	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return false, err1
	}

	return unwrapBool(result)
}

// remove issues a get request for a given key and returns the bytes value which could be nil.
func (m *streamManagerV1) remove(ctx context.Context, cache string, key []byte) (*[]byte, error) {
	req, err := m.newRemoveRequest(cache, key)
	if err != nil {
		return nil, err
	}

	return m.genericRequest(ctx, req, pb1.NamedCacheRequestType_Remove)
}

func (m *streamManagerV1) genericRequest(ctx context.Context, req *pb1.ProxyRequest, namedCacheReqType pb1.NamedCacheRequestType) (*[]byte, error) {
	requestType, err := m.submitRequest(req, namedCacheReqType)
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

	return unwrapBytes(result)
}

func (m *streamManagerV1) aggregate(ctx context.Context, cache string, agent []byte, keyOrFilter *pb1.KeysOrFilter) (*[]byte, error) {
	req, err := m.newAggregateRequest(cache, agent, keyOrFilter)
	if err != nil {
		return nil, err
	}

	return m.genericRequest(ctx, req, pb1.NamedCacheRequestType_Aggregate)
}

func (m *streamManagerV1) replace(ctx context.Context, cache string, key []byte, value []byte) (*[]byte, error) {
	req, err := m.newReplaceRequest(cache, key, value)
	if err != nil {
		return nil, err
	}

	return m.genericRequest(ctx, req, pb1.NamedCacheRequestType_Replace)
}

func (m *streamManagerV1) removeMapping(ctx context.Context, cache string, key []byte, value []byte) (bool, error) {
	req, err := m.newRemoveMappingRequest(cache, key, value)
	if err != nil {
		return false, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_RemoveMapping)
	if err != nil {
		return false, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// remove the entry from the channel
	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return false, err1
	}

	return unwrapBool(result)
}

func (m *streamManagerV1) containsValue(ctx context.Context, cache string, value []byte) (bool, error) {
	req, err := m.newContainsValueRequest(cache, value)
	if err != nil {
		return false, err
	}
	return m.containsRequest(ctx, pb1.NamedCacheRequestType_ContainsValue, req)
}

func (m *streamManagerV1) containsEntry(ctx context.Context, cache string, key []byte, value []byte) (bool, error) {
	req, err := m.newContainsEntryRequest(cache, key, value)
	if err != nil {
		return false, err
	}
	return m.containsRequest(ctx, pb1.NamedCacheRequestType_ContainsEntry, req)
}

// containsKey issues a containsKey request for a given key and returns a bool indicating if the key exists.
func (m *streamManagerV1) containsKey(ctx context.Context, cache string, key []byte) (bool, error) {
	req, err := m.newContainsKeyRequest(cache, key)
	if err != nil {
		return false, err
	}

	return m.containsRequest(ctx, pb1.NamedCacheRequestType_ContainsKey, req)
}

// containsRequest creates containKeys or containsValue requests.
func (m *streamManagerV1) containsRequest(ctx context.Context, reqType pb1.NamedCacheRequestType, req *pb1.ProxyRequest) (bool, error) {
	requestType, err := m.submitRequest(req, reqType)
	if err != nil {
		return false, err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	// remove the entry from the channel
	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return false, err1
	}

	return unwrapBool(result)
}

func (m *streamManagerV1) put(ctx context.Context, cache string, key []byte, value []byte, ttl time.Duration) (*[]byte, error) {
	return m.putGenericRequest(ctx, pb1.NamedCacheRequestType_Put, cache, key, value, ttl)
}

func (m *streamManagerV1) putAll(ctx context.Context, cache string, entries []*pb1.BinaryKeyAndValue, ttl time.Duration) error {
	req, err := m.newPutAllRequest(cache, entries, ttl)
	if err != nil {
		return err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_PutAll)
	if err != nil {
		return err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	_, err = waitForResponse(newCtx, requestType.ch)
	return err
}

func (m *streamManagerV1) addIndex(ctx context.Context, cache string, binExtractor []byte, sorted *bool, binComparator []byte) error {
	return m.index(ctx, cache, true, binExtractor, sorted, binComparator)
}

func (m *streamManagerV1) removeIndex(ctx context.Context, cache string, binExtractor []byte) error {
	return m.index(ctx, cache, false, binExtractor, nil, emptyByte)
}

func (m *streamManagerV1) index(ctx context.Context, cache string, add bool, binExtractor []byte, sorted *bool, binComparator []byte) error {
	req, err := m.newIndexRequest(cache, add, binExtractor, sorted, binComparator)
	if err != nil {
		return err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_Index)
	if err != nil {
		return err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	_, err = waitForResponse(newCtx, requestType.ch)
	return err
}

func (m *streamManagerV1) putIfAbsent(ctx context.Context, cache string, key []byte, value []byte) (*[]byte, error) {
	return m.putGenericRequest(ctx, pb1.NamedCacheRequestType_PutIfAbsent, cache, key, value, 0)
}

// putGenericRequest created a generic put requests, used by put and putIfAbsent.
func (m *streamManagerV1) putGenericRequest(ctx context.Context, reqType pb1.NamedCacheRequestType, cache string, key []byte, value []byte, ttl time.Duration) (*[]byte, error) {
	req, err := m.newPutRequest(reqType, cache, key, value, ttl)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, reqType)
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

	return unwrapBytes(result)
}

// BinaryKeyAndValue is an internal type exported only for serialization.
type BinaryKeyAndValue struct {
	Key    []byte
	Value  []byte
	Err    error
	Cookie []byte
}

// BinaryKey is an internal type exported only for serialization.
type BinaryKey struct {
	Key    []byte
	Err    error
	Cookie []byte
}

// BinaryValue is an internal type exported only for serialization.
type BinaryValue struct {
	Value  []byte
	Err    error
	Cookie []byte
}

func (m *streamManagerV1) getAll(ctx context.Context, cache string, keys [][]byte) (<-chan BinaryKeyAndValue, error) {
	req, err := m.newGetAllRequest(cache, keys)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_GetAll)
	if err != nil {
		return nil, err
	}

	return m.getStreamingResponseKeyAndValue(ctx, requestType, req.Id)
}

func (m *streamManagerV1) keyAndValuePage(ctx context.Context, cache string, cookie []byte) (<-chan BinaryKeyAndValue, error) {
	req, err := m.newPageOfEntriesRequest(cache, cookie)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_PageOfEntries)
	if err != nil {
		return nil, err
	}

	return m.getStreamingResponseKeyAndValue(ctx, requestType, req.Id, true)
}

func (m *streamManagerV1) keyPage(ctx context.Context, cache string, cookie []byte) (<-chan BinaryKey, error) {
	req, err := m.newPageOfKeysRequest(cache, cookie)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_PageOfKeys)
	if err != nil {
		return nil, err
	}

	return m.getStreamingResponseKey(ctx, requestType, req.Id, true)
}

func (m *streamManagerV1) invoke(ctx context.Context, cache string, agent []byte, keyOrFilter *pb1.KeysOrFilter) (<-chan BinaryKeyAndValue, error) {
	req, err := m.newInvokeRequest(cache, agent, keyOrFilter)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_Invoke)
	if err != nil {
		return nil, err
	}

	return m.getStreamingResponseKeyAndValue(ctx, requestType, req.Id)
}

func (m *streamManagerV1) entrySetFilter(ctx context.Context, cache string, filter []byte, comparator []byte) (<-chan BinaryKeyAndValue, error) {
	req, err := m.newEntrySetRequest(cache, filter, comparator)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_QueryEntries)
	if err != nil {
		return nil, err
	}

	return m.getStreamingResponseKeyAndValue(ctx, requestType, req.Id)
}

func (m *streamManagerV1) keySetFilter(ctx context.Context, cache string, filter []byte) (<-chan BinaryKey, error) {
	req, err := m.newKeySetRequest(cache, filter)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_QueryKeys)
	if err != nil {
		return nil, err
	}

	return m.getStreamingResponseKey(ctx, requestType, req.Id)
}

func (m *streamManagerV1) valuesFilter(ctx context.Context, cache string, filter []byte, comparator []byte) (<-chan BinaryValue, error) {
	req, err := m.newValuesFilterRequest(cache, filter, comparator)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_QueryValues)
	if err != nil {
		return nil, err
	}

	return m.getStreamingResponseValue(ctx, requestType, req.Id)
}

func (m *streamManagerV1) getStreamingResponseKeyAndValue(ctx context.Context, requestType proxyRequestChannel, reqID int64, paged ...bool) (<-chan BinaryKeyAndValue, error) {
	isPaged := false
	if len(paged) == 1 {
		// if paged is true the first response will always be the cookie
		isPaged = paged[0]
	}

	// create a channel that will be populated by the streaming request
	ch := make(chan BinaryKeyAndValue)

	newCtx, cancel := m.session.ensureContext(ctx)

	// this channel will receive entries back from the stream
	respChannel := waitForStreamingResponse(newCtx, requestType.ch)

	go func() {
		isFirst := true

		defer m.cleanupRequest(reqID)
		if cancel != nil {
			defer cancel()
		}

		// nolint:gosimple
		for {
			select {
			case resp := <-respChannel:
				// received a streamed response of type pb1.BinaryKeyAndValue
				var response = BinaryKeyAndValue{}
				if resp.err != nil {
					response.Err = fmt.Errorf(errorFormat, resp.err)
				}

				if resp.message != nil {
					// check if we are paged, and it is the first response as this will be the cookie
					if isPaged && isFirst {
						isFirst = false
						cookie, err1 := unwrapBytes(resp.namedCacheResponse.Message)
						if err1 != nil {
							response.Err = err1
						} else {
							// store the cookie
							response.Cookie = *cookie
						}
					} else {
						// otherwise this is a standard key and value
						key, value, err1 := unwrapBinaryKeyAndValue(resp.namedCacheResponse.Message)
						if err1 != nil {
							response.Err = err1
						} else {
							response.Key = *key
							response.Value = *value
						}
					}
					m.session.debugConnection(responseDebug, resp.message)
					ch <- response
				}

				if resp.complete {
					close(ch)
					return
				}
			case <-newCtx.Done():
				errDone := newCtx.Err()
				if !errors.Is(errDone, context.Canceled) {
					ch <- BinaryKeyAndValue{Err: newCtx.Err()}
				}
				return
			}
		}
	}()

	return ch, nil
}

func (m *streamManagerV1) getStreamingResponseValue(ctx context.Context, requestType proxyRequestChannel, reqID int64, paged ...bool) (<-chan BinaryValue, error) {
	isPaged := false
	if len(paged) == 1 {
		// if paged is true the first response will always be the cookie
		isPaged = paged[0]
	}

	// create a channel that will be populated by the streaming request
	ch := make(chan BinaryValue)

	newCtx, cancel := m.session.ensureContext(ctx)

	// this channel will receive entries back from the stream
	respChannel := waitForStreamingResponse(newCtx, requestType.ch)

	go func() {
		isFirst := true

		defer m.cleanupRequest(reqID)
		if cancel != nil {
			defer cancel()
		}

		// nolint:gosimple
		for {
			select {
			case resp := <-respChannel:
				// received a streamed response of type pb1.BinaryValue
				var response = BinaryValue{}
				if resp.err != nil {
					response.Err = fmt.Errorf(errorFormat, resp.err)
				}

				if resp.message != nil {
					// check if we are paged, and it is the first response as this will be the cookie
					if isPaged && isFirst {
						isFirst = false
						cookie, err1 := unwrapBytes(resp.namedCacheResponse.Message)
						if err1 != nil {
							response.Err = err1
						} else {
							// store the cookie
							response.Cookie = *cookie
						}
					} else {
						value, err1 := unwrapBytes(resp.namedCacheResponse.Message)
						if err1 != nil {
							response.Err = err1
						} else {
							response.Value = *value
						}
					}
					m.session.debugConnection(responseDebug, resp.message)
					ch <- response
				}

				if resp.complete {
					close(ch)
					return
				}
			case <-newCtx.Done():
				errDone := newCtx.Err()
				if !errors.Is(errDone, context.Canceled) {
					ch <- BinaryValue{Err: newCtx.Err()}
				}
				return
			}
		}
	}()

	return ch, nil
}

func (m *streamManagerV1) getStreamingResponseKey(ctx context.Context, requestType proxyRequestChannel, reqID int64, paged ...bool) (<-chan BinaryKey, error) {
	isPaged := false
	if len(paged) == 1 {
		// if paged is true the first response will always be the cookie
		isPaged = paged[0]
	}

	// create a channel that will be populated by the streaming request
	ch := make(chan BinaryKey)

	newCtx, cancel := m.session.ensureContext(ctx)

	// this channel will receive entries back from the stream
	respChannel := waitForStreamingResponse(newCtx, requestType.ch)

	go func() {
		isFirst := true

		defer m.cleanupRequest(reqID)
		if cancel != nil {
			defer cancel()
		}

		// nolint:gosimple
		for {
			select {
			case resp := <-respChannel:
				// received a streamed response of type pb1.BytesValue
				var response = BinaryKey{}
				if resp.err != nil {
					response.Err = fmt.Errorf(errorFormat, resp.err)
				}

				if resp.message != nil {
					// check if we are paged, and it is the first response as this will be the cookie
					if isPaged && isFirst {
						isFirst = false
						cookie, err1 := unwrapBytes(resp.namedCacheResponse.Message)
						if err1 != nil {
							response.Err = err1
						} else {
							// store the cookie
							response.Cookie = *cookie
						}
					} else {
						key, err1 := unwrapBytes(resp.namedCacheResponse.Message)
						if err1 != nil {
							response.Err = err1
						} else {
							response.Key = *key
						}
					}
					m.session.debugConnection(responseDebug, resp.message)
					ch <- response
				}

				if resp.complete {
					close(ch)
					return
				}
			case <-newCtx.Done():
				errDone := newCtx.Err()
				if !errors.Is(errDone, context.Canceled) {
					ch <- BinaryKey{Err: errDone}
				}

				return
			}
		}
	}()

	return ch, nil
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
	if resp.namedQueueResponse != nil && resp.namedQueueResponse.Message != nil {
		return resp.namedQueueResponse.Message
	}
	return nil
}

// waitForResponse waits for a response to a request and returns the proto message and any error.
func waitForResponse(newCtx context.Context, ch chan responseMessage, ensure ...bool) (*anypb.Any, error) {
	var (
		err      error
		result   *anypb.Any
		isEnsure = len(ensure) != 0
	)

	// wait until we get a complete request, or we time out
	// nolint:gosimple
	for {
		// wait on the channel
		select {
		case resp := <-ch:
			if resp.err != nil {
				err = fmt.Errorf(errorFormat, *resp.err)
				// force complete on error
				resp.complete = true
			}

			if resp.namedCacheResponse != nil || resp.namedQueueResponse != nil {
				// unpack the result message if we have valid named cache response
				if !isEnsure {
					// standard case where we want to return the named cache/queue result message
					result = defaultFunction(resp)
				} else {
					// special case for ensure, return the cache id or queue id, and it will be handled by ensure
					if resp.namedCacheResponse != nil {
						result, err = anypb.New(wrapperspb.Int32(resp.namedCacheResponse.CacheId))
					} else {
						result, err = anypb.New(wrapperspb.Int32(resp.namedQueueResponse.QueueId))
					}
				}
			}

			if resp.complete {
				return result, err
			}
		case <-newCtx.Done():
			errDone := newCtx.Err()
			if !errors.Is(errDone, context.Canceled) {
				return result, newCtx.Err()
			}
			return result, nil
		}
	}
}

// waitForStreamingResponse
func waitForStreamingResponse(newCtx context.Context, ch chan responseMessage) <-chan responseMessage {
	var chMessage = make(chan responseMessage) // channel to send back to caller

	go func() {
		// wait until we get a complete request, or we time out
		for {
			// wait on the channel
			select {
			case resp := <-ch:
				var response = responseMessage{}
				if resp.err != nil {
					response.err = resp.err
				}

				if resp.namedCacheResponse != nil {
					// unpack the result message if we have valid named cache response
					response.message = defaultFunction(resp)
				}

				// write the request to the response channel
				chMessage <- resp
				if resp.complete {
					return
				}
			case <-newCtx.Done(): // timeout or cancel
				errDone := newCtx.Err()
				if !errors.Is(errDone, context.Canceled) {
					e := fmt.Sprintf("%v", newCtx.Err())
					chMessage <- responseMessage{
						err:      &e,
						complete: true,
					}
				}

				return
			}
		}
	}()

	return chMessage
}

// addLifecycleListener adds the specified [MapLifecycleListener].
func (bc *baseClient[K, V]) addLifecycleListener(listener MapLifecycleListener[K, V]) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	for _, e := range bc.lifecycleListenersV1 {
		if *e == listener {
			return
		}
	}
	bc.lifecycleListenersV1 = append(bc.lifecycleListenersV1, &listener)
}

// removeLifecycleListener removes the specified [MapLifecycleListener].
func (bc *baseClient[K, V]) removeLifecycleListener(listener MapLifecycleListener[K, V]) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	idx := -1
	listeners := bc.lifecycleListenersV1
	for i, c := range listeners {
		if *c == listener {
			idx = i
			break
		}
	}
	if idx != -1 {
		result := append(listeners[:idx], listeners[idx+1:]...)
		bc.lifecycleListenersV1 = result
	}
}

func unwrapBool(result *anypb.Any) (bool, error) {
	var message = &wrapperspb.BoolValue{}
	if err := result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("unwrapBool", err)
		return false, err
	}

	return message.Value, nil
}

func unwrapBytes(result *anypb.Any) (*[]byte, error) {
	var message = &wrapperspb.BytesValue{}

	if err := result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("unwrapBytes", err)
		return nil, err
	}

	return &message.Value, nil
}

func unwrapMapEvent(result *pb1.NamedCacheResponse) (*pb1.MapEventMessage, error) {
	var message = &pb1.MapEventMessage{}

	if err := result.Message.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("unwrapMapEvent", err)
		return nil, err
	}

	return message, nil
}

func unwrapBinaryKeyAndValue(result *anypb.Any) (*[]byte, *[]byte, error) {
	var message = &pb1.BinaryKeyAndValue{}
	if err := result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("unwrapBinaryKeyAndValue", err)
		return nil, nil, err
	}

	return &message.Key, &message.Value, nil
}

func getInitDescription(r *pb1.InitResponse) string {
	return fmt.Sprintf("initResponse{version=%s, protocolVersion=%d, proxyMemberId=%d}",
		r.GetVersion(), r.GetProtocolVersion(), r.GetProxyMemberId())
}

func getCacheIDMessage(cache string) error {
	return fmt.Errorf("unable to find cache id for cache named [%s]", cache)
}

func getQueueIDMessage(cache string) error {
	return fmt.Errorf("unable to find queue id for queue named [%s]", cache)
}

func getUnmarshallError(message string, err error) error {
	return fmt.Errorf("unable to unpack %v message %v", message, err)
}
