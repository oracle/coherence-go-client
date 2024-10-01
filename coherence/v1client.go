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

// streamManagerV1 holds the data for a gRPC V1 connection.
type streamManagerV1 struct {
	session       *Session
	mutex         sync.RWMutex
	eventStream   *eventStreamV1
	proxyProtocol V1ProxyProtocol
	serializer    Serializer[any]
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
		serializer:    NewSerializer[any](session.sessOpts.Format),
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
		m.session.debugGrpc("Event, CacheId=", resp.namedCacheResponse.CacheId, ", Type=", resp.namedCacheResponse.Type)

		// for a map event we must get the cache name and then looking the base client so
		// we can send the event to the right place
		cacheID := resp.namedCacheResponse.CacheId
		if cacheID == 0 {
			log.Printf("received an event %v with cacheID = 0", resp.namedCacheResponse.Type)
			return
		}

		cacheName := m.session.getCacheNameFromCacheID(cacheID)
		if cacheName == nil {
			log.Printf("unable to find cache name from cache id %v", cacheID)
			return
		}

		// determine if the cache is a map or cache
		var client interface{}

		client = m.session.getNamedCacheClient(*cacheName)
		if client == nil {
			client = m.session.getNamedMapClient(*cacheName)
		}

		if eventSubmitter, ok := client.(EventSubmitter); ok {
			m.session.debugGrpc("found client", client)
			switch resp.namedCacheResponse.Type {
			case pb1.ResponseType_Destroyed:
				eventSubmitter.generateMapLifecycleEvent(client, Destroyed)
				log.Printf("id=%v, destroyed, %v", reqID, resp)
				// TODO: need to actually destroy the cache reference, where can we do this where it is safe?
			case pb1.ResponseType_Truncated:
				eventSubmitter.generateMapLifecycleEvent(client, Truncated)
				log.Printf("id=%v, truncated, %v", reqID, resp)
			case pb1.ResponseType_MapEvent:
				// convert to MapEventMessage
				mapEventMessage, err1 := unwrapMapEvent(resp.message)
				if err1 != nil {
					log.Printf("unable to unwrap MapEvent: %v", err1)
				} else {
					eventSubmitter.generateMapEvent(client, mapEventMessage)
					log.Printf("id=%v, mapevent, %v", reqID, mapEventMessage)
				}
			}
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

	// store the cache id in the session , no lock required as already locked
	cacheID = &message.Value
	//m.session.mapMutex.Lock()
	m.session.cacheIDMap[cache] = *cacheID
	//m.session.mapMutex.Unlock()

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
	m.session.cacheIDMapMutex.Lock()
	delete(m.session.cacheIDMap, cache)
	m.session.cacheIDMapMutex.Unlock()

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

	var message = &wrapperspb.Int32Value{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("sizeResponse", err)
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

	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return value, err1
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

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_Remove)
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

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_Aggregate)
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

func (m *streamManagerV1) replace(ctx context.Context, cache string, key []byte, value []byte) (*[]byte, error) {
	req, err := m.newReplaceRequest(cache, key, value)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_Replace)
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

type BinaryKeyAndValue struct {
	Key    []byte
	Value  []byte
	Err    error
	Cookie []byte
}

type BinaryKey struct {
	Key    []byte
	Err    error
	Cookie []byte
}

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

func (m *streamManagerV1) entrySetFilter(ctx context.Context, cache string, filter []byte) (<-chan BinaryKeyAndValue, error) {
	req, err := m.newEntrySetRequest(cache, filter)
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

func (m *streamManagerV1) valuesFilter(ctx context.Context, cache string, filter []byte) (<-chan BinaryValue, error) {
	req, err := m.newValuesFilterRequest(cache, filter)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitRequest(req, pb1.NamedCacheRequestType_QueryValues)
	if err != nil {
		return nil, err
	}

	return m.getStreamingResponseValue(ctx, requestType, req.Id)
}

func (m *streamManagerV1) getStreamingResponseKeyAndValue(ctx context.Context, requestType namedCacheRequest, reqID int64, paged ...bool) (<-chan BinaryKeyAndValue, error) {
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
					response.Err = fmt.Errorf("error: %v", resp.err)
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
						// otherwise this is s standard key and value
						key, value, err1 := unwrapBinaryKeyAndValue(resp.namedCacheResponse.Message)
						if err1 != nil {
							response.Err = err1
						} else {
							response.Key = *key
							response.Value = *value
						}
					}
					m.session.debugGrpc("received response", resp.message)
					ch <- response
				}

				if resp.complete {
					close(ch)
					break
				}
			}
		}
	}()

	return ch, nil
}

func (m *streamManagerV1) getStreamingResponseValue(ctx context.Context, requestType namedCacheRequest, reqID int64, paged ...bool) (<-chan BinaryValue, error) {
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
					response.Err = fmt.Errorf("error: %v", resp.err)
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
					m.session.debugGrpc("received response", resp.message)
					ch <- response
				}

				if resp.complete {
					close(ch)
					break
				}
			}
		}
	}()

	return ch, nil
}

func (m *streamManagerV1) getStreamingResponseKey(ctx context.Context, requestType namedCacheRequest, reqID int64, paged ...bool) (<-chan BinaryKey, error) {
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
					response.Err = fmt.Errorf("error: %v", resp.err)
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
					m.session.debugGrpc("received response", resp.message)
					ch <- response
				}

				if resp.complete {
					close(ch)
					break
				}
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
	return nil
}

// waitForResponse waits for a response to a request and returns the proto message and any error.
func waitForResponse(newCtx context.Context, ch chan responseMessage, ensureCache ...bool) (*anypb.Any, error) {
	var (
		err           error
		result        *anypb.Any
		isEnsureCache = len(ensureCache) != 0
	)

	// wait until we get a complete request, or we time out
	// nolint:gosimple
	for {
		// wait on the channel
		select {
		case resp := <-ch:
			if resp.err != nil {
				err = fmt.Errorf("error: %v", *resp.err)
				// force complete on error
				resp.complete = true
			}

			if resp.namedCacheResponse != nil {
				// unpack the result message if we have valid named cache response
				if !isEnsureCache {
					// standard case where we want to return the names cache result message
					result = defaultFunction(resp)
				} else {
					// special case for ensureCache, return the cache id, and it will be handled by ensureCache
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
			case <-newCtx.Done():
				e := fmt.Sprintf("%v", newCtx.Err())
				chMessage <- responseMessage{
					err: &e,
				}
				close(chMessage)
			}
		}
	}()

	return chMessage
}

// addLifecycleListener adds the specified [MapLifecycleListener].
func (bc *baseClient[K, V]) addLifecycleListener(listener MapLifecycleListener[K, V]) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	for _, e := range bc.lifecycleListeners {
		if *e == listener {
			return
		}
	}
	bc.lifecycleListeners = append(bc.lifecycleListeners, &listener)
}

// removeLifecycleListener removes the specified [MapLifecycleListener].
func (bc *baseClient[K, V]) removeLifecycleListener(listener MapLifecycleListener[K, V]) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	idx := -1
	listeners := bc.lifecycleListeners
	for i, c := range listeners {
		if *c == listener {
			idx = i
			break
		}
	}
	if idx != -1 {
		result := append(listeners[:idx], listeners[idx+1:]...)
		bc.lifecycleListeners = result
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

func unwrapMapEvent(result *anypb.Any) (*pb1.MapEventMessage, error) {
	var message = &pb1.MapEventMessage{}

	if err := result.UnmarshalTo(message); err != nil {
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

func getUnmarshallError(message string, err error) error {
	return fmt.Errorf("unable to unpack %v message %v", message, err)
}
