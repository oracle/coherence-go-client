/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"errors"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/publisher"
	"github.com/oracle/coherence-go-client/v2/coherence/subscriber"
	"github.com/oracle/coherence-go-client/v2/coherence/subscribergroup"
	"github.com/oracle/coherence-go-client/v2/coherence/topic"
	pb1topics "github.com/oracle/coherence-go-client/v2/proto/topics"
	pb1 "github.com/oracle/coherence-go-client/v2/proto/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"strings"
	"sync"
)

const (
	defaultChannelCount = 17
)

var (
	_ Publisher[string]  = &topicPublisher[string]{}
	_ NamedTopic[string] = &baseTopicsClient[string]{}
	_ Subscriber[string] = &topicSubscriber[string]{}

	ErrTopicDestroyedOrReleased = errors.New("this topic has been destroyed or released")
	ErrTopicsNoSupported        = errors.New("the coherence server version must support protocol version 1 or above to use topic")
	ErrTopicFull                = errors.New("unable to publish, this topic is full")
	ErrPublisherClosed          = errors.New("publisher has been closed and is no longer usable")
	ErrSubscriberClosed         = errors.New("subscriber has been closed and is no longer usable")
)

// NamedTopic defines the APIs to interact with Coherence topic allowing to publish and
// subscribe from Go client.
//
// The type parameter is V = type of the value.
type NamedTopic[V any] interface {
	// Destroy destroys this topic on the server and releases all resources. After this operation it is no longer usable on the client or server.
	Destroy(ctx context.Context) error

	// Close closes a topic and releases the resources associated with it on the client only.
	Close(ctx context.Context) error

	// CreatePublisher creates a Publisher with the specified options.
	CreatePublisher(ctx context.Context, options ...func(o *publisher.Options)) (Publisher[V], error)

	// CreateSubscriber creates a Subscriber with the specified options.
	// Note: If you wish to create a Subscriber with a transformer, you should use the helper
	// function CreatSubscriberWithTransformer.
	CreateSubscriber(ctx context.Context, options ...func(o *subscriber.Options)) (Subscriber[V], error)

	// CreateSubscriberGroup creates a subscriber group with the given options.
	CreateSubscriberGroup(ctx context.Context, subscriberGroup string, options ...func(o *subscribergroup.Options)) error

	// DestroySubscriberGroup destroys a subscriber group.
	// TODO: Not viable to implement?
	DestroySubscriberGroup(ctx context.Context, subscriberGroup string) error

	// AddLifecycleListener adds a [TopicLifecycleListener] to this topic.
	AddLifecycleListener(listener TopicLifecycleListener[V]) error

	// RemoveLifecycleListener removes a [TopicLifecycleListener] from this topic.
	RemoveLifecycleListener(listener TopicLifecycleListener[V]) error

	// GetName returns the name of this topic.
	GetName() string
}

// Publisher provides the means to publish messages to a [NamedTopic].
type Publisher[V any] interface {
	GetProxyID() int32
	GetPublisherID() int64
	GetChannelCount() int32

	// Publish publishes a message and returns a status.
	Publish(ctx context.Context, value V) (*publisher.PublishStatus, error)

	// Close closes a publisher and releases all resources associated with it in the client
	// and on the server.
	Close(ctx context.Context) error
}

// Subscriber subscribes directly to a [NamedTopic], or to a subscriber group of a [NamedTopic].
type Subscriber[V any] interface {
	// Close closes a subscriber and releases all resources associated with it in the client
	// and on the server.
	Close(ctx context.Context) error

	// DestroySubscriberGroup destroys a subscriber group created by this subscriber.
	DestroySubscriberGroup(ctx context.Context, subscriberGroup string) error

	// AddLifecycleListener adds a [SubscriberLifecycleListener] to this subscriber.
	AddLifecycleListener(listener SubscriberLifecycleListener[V]) error

	// RemoveLifecycleListener removes a [SubscriberLifecycleListener] from this subscriber.
	RemoveLifecycleListener(listener SubscriberLifecycleListener[V]) error
}

// GetNamedTopic gets a [NamedTopic] of the generic type specified or if a topic already exists with the
// same type parameters, it will return it otherwise it will create a new one.
func GetNamedTopic[V any](ctx context.Context, session *Session, topicName string, options ...func(cache *topic.Options)) (NamedTopic[V], error) {
	var (
		existingTopic interface{}
		ok            bool
		err           error
	)

	topicOptions := &topic.Options{
		ChannelCount: defaultChannelCount,
	}

	// apply any cache options
	for _, f := range options {
		f(topicOptions)
	}

	// protect updates to maps
	session.mapMutex.Lock()

	// check to see if we already have an entry for the queue
	if existingTopic, ok = session.topics[topicName]; ok {
		defer session.mapMutex.Unlock()

		existing, ok2 := existingTopic.(NamedTopic[V])
		if !ok2 {
			// the casting failed so return an error indicating the queue exists with different type mappings
			return nil, getExistingError("NamedTopic", topicName)
		}

		// check any topic options

		session.debug("using existing NamedTopic: %v", existing)
		return existing, nil
	}

	session.topics[topicName] = nil
	session.mapMutex.Unlock()

	bt, err := ensureTopicInternal[V](ctx, session, topicName, topicOptions)
	if err != nil {
		return nil, err
	}

	newTopic := &namedTopic[V]{baseTopicsClient: bt}

	session.topics[topicName] = newTopic

	return newTopic, nil
}

type topicPublisher[V any] struct {
	session              *Session
	isClosed             bool
	namedTopic           *baseTopicsClient[V] // may be nil if created outside topic
	topicName            string
	proxyID              int32
	publisherID          int64
	channelCount         int32
	options              *publisher.Options
	valueSerializer      Serializer[V]
	mutex                *sync.RWMutex
	lifecycleListenersV1 []*PublisherLifecycleListener[V]
}

func (tp *topicPublisher[V]) GetProxyID() int32 {
	return tp.proxyID
}

func (tp *topicPublisher[V]) GetPublisherID() int64 {
	return tp.publisherID
}

func (tp *topicPublisher[V]) GetChannelCount() int32 {
	return tp.channelCount
}

func (tp *topicPublisher[V]) Publish(ctx context.Context, value V) (*publisher.PublishStatus, error) {
	if tp.isClosed {
		return nil, ErrPublisherClosed
	}

	publishChannel := tp.ensureTopicChannel()

	binValue, err := tp.valueSerializer.Serialize(value)
	if err != nil {
		return nil, err
	}

	return tp.session.v1StreamManagerTopics.publishToTopic(ctx, tp.proxyID, publishChannel, binValue)
}

func (tp *topicPublisher[V]) Close(ctx context.Context) error {
	if tp.isClosed {
		return ErrPublisherClosed
	}
	tp.isClosed = true
	tp.session.publisherIDMap.Remove(tp.publisherID)

	tp.session.mapMutex.Lock()
	delete(tp.session.publishers, tp.publisherID)

	tp.session.mapMutex.Unlock()
	tp.generatePublisherLifecycleEvent(tp, PublisherReleased)

	return tp.session.v1StreamManagerTopics.destroyPublisherOrSubscriber(ctx, tp.proxyID, pb1topics.TopicServiceRequestType_DestroyPublisher)
}

func (tp *topicPublisher[V]) String() string {
	return fmt.Sprintf("publisher{publisherID=%d, topicName=%s, proxyID=%d, channelCount=%d, %v, destroyed=%v}",
		tp.publisherID, tp.topicName, tp.proxyID, tp.channelCount, tp.options, tp.isClosed)
}

// ensureTopicChannel returns the channel to publish to based upon the [publisher.Options].
func (tp *topicPublisher[V]) ensureTopicChannel() int32 {
	return tp.options.GetOrdering().GetPublishHash() % tp.channelCount
}

type baseTopicsClient[V any] struct {
	session              *Session
	valueSerializer      Serializer[V]
	name                 string
	ctx                  context.Context
	topicID              int32
	isDestroyed          bool
	isReleased           bool
	mutex                *sync.RWMutex
	topicOpts            *topic.Options
	lifecycleListenersV1 []*TopicLifecycleListener[V]
}

type namedTopic[V any] struct {
	*baseTopicsClient[V]
}

func (bt *baseTopicsClient[V]) Destroy(ctx context.Context) error {
	return releaseTopicInternal[V](ctx, bt, true)
}

func (bt *baseTopicsClient[V]) GetName() string {
	return bt.name
}

func (bt *baseTopicsClient[V]) Close(ctx context.Context) error {
	return releaseTopicInternal[V](ctx, bt, false)
}

func (bt *baseTopicsClient[V]) String() string {
	return fmt.Sprintf("topic{name=%s, topicID=%d, isReleased=%v, %v}", bt.name, bt.topicID, bt.isReleased, bt.topicOpts)
}

func (bt *baseTopicsClient[V]) CreatePublisher(ctx context.Context, options ...func(cache *publisher.Options)) (Publisher[V], error) {
	return CreatePublisher[V](ctx, bt.session, bt.name, options...)
}

func (bt *baseTopicsClient[V]) CreateSubscriber(ctx context.Context, options ...func(cache *subscriber.Options)) (Subscriber[V], error) {
	return CreateSubscriber[V](ctx, bt.session, bt.name, options...)
}

func (bt *baseTopicsClient[V]) CreateSubscriberGroup(ctx context.Context, subscriberGroup string, options ...func(o *subscribergroup.Options)) error {
	return CreateSubscriberGroup(ctx, bt.session, bt.name, subscriberGroup, options...)
}

func (bt *baseTopicsClient[V]) DestroySubscriberGroup(ctx context.Context, subscriberGroup string) error {
	return DestroySubscriberGroup(ctx, bt.session, subscriberGroup)
}

func newPublisher[V any](session *Session, bt *baseTopicsClient[V], result *publisher.EnsurePublisherResult, topicName string, options *publisher.Options) (Publisher[V], error) {
	tp := &topicPublisher[V]{
		namedTopic:      bt,
		publisherID:     result.PublisherID,
		session:         session,
		options:         options,
		valueSerializer: NewSerializer[V](session.sessOpts.Format),
		topicName:       topicName,
		proxyID:         result.ProxyID,
		channelCount:    result.ChannelCount,
		isClosed:        false,
	}
	session.mapMutex.Lock()
	defer session.mapMutex.Unlock()
	session.publishers[result.PublisherID] = tp

	session.publisherIDMap.Add(tp.publisherID, tp.proxyID)
	return tp, nil
}

func newSubscriber[V any](session *Session, bt *baseTopicsClient[V], result *subscriber.EnsureSubscriberResult, topicName string, options *subscriber.Options) (Subscriber[V], error) {
	ts := &topicSubscriber[V]{
		namedTopic:      bt,
		SubscriberID:    result.SubscriberID,
		UUID:            result.UUID,
		session:         session,
		options:         options,
		valueSerializer: NewSerializer[V](session.sessOpts.Format),
		topicName:       topicName,
		proxyID:         result.ProxyID,
		disconnected:    true,
		isClosed:        false,
	}

	session.mapMutex.Lock()
	defer session.mapMutex.Unlock()
	session.subscribers[result.SubscriberID] = ts

	session.subscriberIDMap.Add(ts.SubscriberID, ts.proxyID)

	return ts, nil
}

type topicSubscriber[V any] struct {
	session              *Session
	namedTopic           *baseTopicsClient[V] // may be nil if created outside topic
	topicName            string
	proxyID              int32
	SubscriberID         int64
	SubscriberGroupID    int64
	channelCount         int32
	options              *subscriber.Options
	valueSerializer      Serializer[V]
	UUID                 string
	mutex                sync.RWMutex
	disconnected         bool
	isClosed             bool
	lifecycleListenersV1 []*SubscriberLifecycleListener[V]
}

func (ts *topicSubscriber[V]) String() string {
	return fmt.Sprintf("subscriber{subscriberID=%d, subscriberGroup=%d, topicName=%s, channelCount=%d, options=%v}", ts.SubscriberID, ts.SubscriberGroupID,
		ts.topicName, ts.channelCount, ts.options)
}

func (ts *topicSubscriber[V]) Close(ctx context.Context) error {
	if ts.isClosed {
		return ErrSubscriberClosed
	}
	err := ts.ensureConnected()
	if err != nil {
		return err
	}

	ts.isClosed = true
	ts.session.subscriberIDMap.Remove(ts.SubscriberID)

	ts.session.mapMutex.Lock()
	delete(ts.session.publishers, ts.SubscriberID)
	ts.session.mapMutex.Unlock()

	ts.generateSubscriberLifecycleEvent(ts, SubscriberReleased)

	return ts.session.v1StreamManagerTopics.destroyPublisherOrSubscriber(ctx, ts.proxyID, pb1topics.TopicServiceRequestType_DestroySubscriber)
}

func (ts *topicSubscriber[V]) DestroySubscriberGroup(ctx context.Context, subscriberGroup string) error {
	return ts.session.v1StreamManagerTopics.destroySubscriberGroup(ctx, ts.proxyID, subscriberGroup)
}

func (ts *topicSubscriber[V]) isDisconnected() bool {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	return ts.disconnected
}

// ensureConnected ensures the topic is connected.
func (ts *topicSubscriber[V]) ensureConnected() error {
	if !ts.isDisconnected() {
		return nil
	}

	return nil
}

func newBaseTopicsClient[V any](ctx context.Context, session *Session, queueName string, topicID int32, topicOpts *topic.Options) (*baseTopicsClient[V], error) {
	if session.closed {
		return nil, ErrClosed
	}

	bq := baseTopicsClient[V]{
		ctx:             ctx,
		name:            queueName,
		topicID:         topicID,
		session:         session,
		valueSerializer: NewSerializer[V](session.sessOpts.Format),
		mutex:           &sync.RWMutex{},
		topicOpts:       topicOpts,
	}

	return &bq, nil
}

func ensurePublisherOptions(options ...func(cache *publisher.Options)) *publisher.Options {
	publisherOptions := &publisher.Options{
		ChannelCount: defaultChannelCount,
		Ordering:     &publisher.OrderByDefault{},
	}

	// apply any cache options
	for _, f := range options {
		f(publisherOptions)
	}

	return publisherOptions
}

// CreatePublisher creates a topic publisher when provided a topicName. You do not have to had created
// a topic, but it is equivalent to called CreatePublisher on a [NamedTopic].
func CreatePublisher[V any](ctx context.Context, session *Session, topicName string, options ...func(cache *publisher.Options)) (Publisher[V], error) {
	publisherOptions := ensurePublisherOptions(options...)

	if session.v1StreamManagerTopics == nil {
		if err := ensureV1StreamManagerTopics(session); err != nil {
			return nil, err
		}
	}

	result, err := session.v1StreamManagerTopics.ensurePublisher(ctx, topicName, publisherOptions.ChannelCount)
	if err != nil {
		return nil, err
	}

	return newPublisher[V](session, nil, result, topicName, publisherOptions)
}

// CreatSubscriberWithTransformer creates a subscriber which will transform the value from the topic using
// the supplied extractor.
func CreatSubscriberWithTransformer[E any](ctx context.Context, session *Session, topicName string,
	extractor extractors.ValueExtractor[any, E], options ...func(cache *subscriber.Options)) (Subscriber[E], error) {

	binExtractor, err := session.genericSerializer.Serialize(extractor)
	if err != nil {
		return nil, err
	}

	options = append(options, subscriber.WithTransformer(binExtractor))

	return CreateSubscriber[E](ctx, session, topicName, options...)
}

// CreateSubscriber creates a topic subscriber.
func CreateSubscriber[V any](ctx context.Context, session *Session, topicName string, options ...func(cache *subscriber.Options)) (Subscriber[V], error) {
	var (
		subscriberOptions = &subscriber.Options{}
		binFilter         []byte
		err               error
	)

	// apply any subscriber options
	for _, f := range options {
		f(subscriberOptions)
	}

	if session.v1StreamManagerTopics == nil {
		if err = ensureV1StreamManagerTopics(session); err != nil {
			return nil, err
		}
	}

	if subscriberOptions.Filter != nil {
		binFilter, err = session.genericSerializer.Serialize(subscriberOptions.Filter)
		if err != nil {
			return nil, err
		}
	}

	result, err := session.v1StreamManagerTopics.ensureSubscriber(ctx, topicName, subscriberOptions.SubscriberGroup, binFilter)
	if err != nil {
		return nil, err
	}

	return newSubscriber[V](session, nil, result, topicName, subscriberOptions)
}

// CreateSubscriberGroup creates a topic subscriber group.
func CreateSubscriberGroup(ctx context.Context, session *Session, topicName string, subscriberGroup string, options ...func(cache *subscribergroup.Options)) error {
	var (
		subscriberGroupOptions = &subscribergroup.Options{}
		binFilter              []byte
		err                    error
	)

	// apply any subscriber options
	for _, f := range options {
		f(subscriberGroupOptions)
	}

	if session.v1StreamManagerTopics == nil {
		if err = ensureV1StreamManagerTopics(session); err != nil {
			return err
		}
	}

	if subscriberGroupOptions.Filter != nil {
		binFilter, err = session.genericSerializer.Serialize(subscriberGroupOptions.Filter)
		if err != nil {
			return err
		}
	}

	return session.v1StreamManagerTopics.ensureSubscriberGroup(ctx, topicName, subscriberGroup, binFilter)
}

// DestroySubscriberGroup destroys a subscriber group.
func DestroySubscriberGroup(ctx context.Context, session *Session, subscriberGroup string) error {
	if session.v1StreamManagerTopics == nil {
		if err := ensureV1StreamManagerTopics(session); err != nil {
			return err
		}
	}

	// TODO: Is this valid???, how can we determine the proxyID if we don't have a subscriber
	return session.v1StreamManagerTopics.destroySubscriberGroup(ctx, 0, subscriberGroup)
}

// ensurePublisher ensures a publisher.
func (m *streamManagerV1) ensurePublisher(ctx context.Context, topicName string, channelCount int32) (*publisher.EnsurePublisherResult, error) {
	req, err := m.newEnsurePublisherRequest(topicName, channelCount)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitTopicRequest(req, pb1topics.TopicServiceRequestType_EnsurePublisher)

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch, false)
	if err1 != nil {
		return nil, err
	}

	var message = &pb1topics.EnsurePublisherResponse{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("ensure response", err)
		return nil, err
	}

	return &publisher.EnsurePublisherResult{
		PublisherID:  message.PublisherId,
		ProxyID:      message.ProxyId,
		ChannelCount: message.ChannelCount,
	}, nil
}

// ensureSubscriber ensures a subscriber.
func (m *streamManagerV1) ensureSubscriber(ctx context.Context, topicName string, subscriberGroup *string, binFilter []byte) (*subscriber.EnsureSubscriberResult, error) {
	req, err := m.newEnsureSubscriberRequest(topicName, subscriberGroup, binFilter)
	if err != nil {
		return nil, err
	}

	requestType, err := m.submitTopicRequest(req, pb1topics.TopicServiceRequestType_EnsureSubscriber)

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	result, err1 := waitForResponse(newCtx, requestType.ch, false)
	if err1 != nil {
		return nil, err
	}

	var message = &pb1topics.EnsureSubscriberResponse{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("ensure subscriber response", err)
		return nil, err
	}

	s := subscriber.EnsureSubscriberResult{ProxyID: message.ProxyId}

	if message.SubscriberId != nil {
		s.SubscriberID = message.SubscriberId.Id
		s.UUID = string(message.SubscriberId.Uuid)
	}

	return &s, nil
}

// ensureSubscriber ensures a subscriber.
func (m *streamManagerV1) ensureSubscriberGroup(ctx context.Context, topicName string, subscriberGroup string, binFilter []byte) error {
	req, err := m.newEnsureSubscriberGroupRequest(topicName, subscriberGroup, binFilter)
	if err != nil {
		return err
	}

	requestType, err := m.submitTopicRequest(req, pb1topics.TopicServiceRequestType_EnsureSubscriberGroup)
	if err != nil {
		return err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	// only a complete for create subscriber group
	_, err = waitForResponse(newCtx, requestType.ch, false)

	return err
}

// destroySubscriberGroup destroys a subscriber.
func (m *streamManagerV1) destroySubscriberGroup(ctx context.Context, proxyID int32, subscriberGroup string) error {
	req, err := m.newDestroySubscriberGroupRequest(proxyID, subscriberGroup)
	if err != nil {
		return err
	}

	requestType, err := m.submitTopicRequest(req, pb1topics.TopicServiceRequestType_DestroySubscriberGroup)
	if err != nil {
		return err
	}

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	// only a complete for destroy subscriber group
	_, err = waitForResponse(newCtx, requestType.ch, false)

	return err
}

// destroyPublisher destroyed a publisher.
func (m *streamManagerV1) destroyPublisherOrSubscriber(ctx context.Context, proxyID int32, reqType pb1topics.TopicServiceRequestType) error {
	req, err := m.newDestroyPublisherOrSubscriberRequest(proxyID, reqType)
	if err != nil {
		return err
	}

	requestType, err := m.submitTopicRequest(req, reqType)

	newCtx, cancel := m.session.ensureContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	defer m.cleanupRequest(req.Id)

	_, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return err
	}

	return nil
}

//
//// ensureSubscriber ensures a subscriber.
//func (m *streamManagerV1) ensureSubscriptionRequest(ctx context.Context, subscriptionID int64, force bool) (*subscriber.EnsureSubscriberResult, error) {
//	req, err := m.newEnsureSubscriptionRequest(subscriptionID, force)
//	if err != nil {
//		return nil, err
//	}
//
//	requestType, err := m.submitTopicRequest(req, pb1topics.TopicServiceRequestType_EnsureSubscription)
//
//	newCtx, cancel := m.session.ensureContext(ctx)
//	if cancel != nil {
//		defer cancel()
//	}
//
//	defer m.cleanupRequest(req.Id)
//
//	result, err1 := waitForResponse(newCtx, requestType.ch, false)
//	if err1 != nil {
//		return nil, err
//	}
//
//	var message = &pb1topics.EnsureSubscriptionRequest{}
//	if err = result.UnmarshalTo(message); err != nil {
//		err = getUnmarshallError("ensure subscriber response", err)
//		return nil, err
//	}
//
//	s := subscriber.EnsureSubscriberResult{ProxyID: message.ProxyId}
//
//	if message.SubscriberId != nil {
//		s.SubscriberID = message.SubscriberId.Id
//		s.UUID = string(message.SubscriberId.Uuid)
//	}
//
//	return &s, nil
//}

// ensureTopic issues the ensure queue request. This must be done before any requests to access queues can be issued.
func (m *streamManagerV1) ensureTopic(ctx context.Context, topicName string) (*int32, error) {
	return m.ensure(ctx, topicName, m.session.topicIDMap, -1, true)
}

func ensureTopicInternal[V any](ctx context.Context, session *Session, topicName string, topicsOpts *topic.Options) (*baseTopicsClient[V], error) {
	if err := ensureV1StreamManagerTopics(session); err != nil {
		return nil, err
	}

	topicID, err := session.v1StreamManagerTopics.ensureTopic(ctx, topicName)
	if err != nil {
		return nil, err
	}

	// ensure channels
	_, err = session.v1StreamManagerTopics.ensureTopicChannels(ctx, topicName, topicsOpts.ChannelCount)
	if err != nil {
		return nil, err
	}

	return newBaseTopicsClient[V](ctx, session, topicName, *topicID, topicsOpts)
}

// ensure ensures a queue or cache.
func (m *streamManagerV1) ensureTopicChannels(ctx context.Context, topicName string, channelCount int32) (*int32, error) {
	var ID *int32

	req, err := m.newEnsureTopicChannelRequest(topicName, channelCount)

	if err != nil {
		return nil, err
	}

	requestType, err := m.submitTopicRequest(req, pb1topics.TopicServiceRequestType_EnsureChannelCount)

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

	return ID, nil
}

// newPublishRequest publishes a message to a topic.
func (m *streamManagerV1) publishToTopic(ctx context.Context, proxyID int32, publishChannel int32, value []byte) (*publisher.PublishStatus, error) {
	req, err := m.newPublishRequest(proxyID, publishChannel, value)

	if err != nil {
		return nil, err
	}

	requestType, err := m.submitTopicRequest(req, pb1topics.TopicServiceRequestType_Publish)

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

	var message = &pb1topics.PublishResult{}
	if err = result.UnmarshalTo(message); err != nil {
		err = getUnmarshallError("ensure response", err)
		return nil, err
	}

	return &publisher.PublishStatus{PublishedChannel: publishChannel, RemainingCapacity: message.RemainingCapacity}, nil
}

func ensureV1StreamManagerTopics(session *Session) error {
	session.connectMutex.Lock()
	defer session.connectMutex.Unlock()
	if session.v1StreamManagerTopics == nil {
		topicsManger, err := newStreamManagerV1(session, topicServiceProtocol)
		if err != nil {
			if strings.Contains(err.Error(), "Method not found") {
				return ErrTopicsNoSupported
			}
			return err
		}
		session.v1StreamManagerTopics = topicsManger
	}

	return nil
}

func releaseTopicInternal[V any](ctx context.Context, bt *baseTopicsClient[V], destroy bool) error {
	if bt.isDestroyed || bt.isReleased {
		return ErrTopicDestroyedOrReleased
	}

	bt.session.mapMutex.Lock()
	defer bt.session.mapMutex.Unlock()

	if destroy {
		newCtx, cancel := bt.session.ensureContext(ctx)
		if cancel != nil {
			defer cancel()
		}

		err := bt.session.v1StreamManagerTopics.genericTopicRequest(newCtx, pb1topics.TopicServiceRequestType_DestroyTopic, bt.name)
		if err != nil {
			return err
		}
		bt.isDestroyed = true
		bt.generateTopicLifecycleEvent(nil, TopicDestroyed)
	} else {
		if existingTopic, ok := bt.session.topics[bt.name]; ok {
			bt.generateTopicLifecycleEvent(existingTopic, TopicReleased)
			bt.isReleased = true
		}
	}

	delete(bt.session.topics, bt.name)
	bt.session.topicIDMap.Remove(bt.name)

	return nil
}

func (m *streamManagerV1) newEnsurePublisherRequest(topicName string, channelCount int32) (*pb1.ProxyRequest, error) {
	req := &pb1topics.EnsurePublisherRequest{
		Topic:        topicName,
		ChannelCount: channelCount,
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return nil, err
	}
	return m.newWrapperProxyTopicsRequest(topicName, pb1topics.TopicServiceRequestType_EnsurePublisher, anyReq)
}

func (m *streamManagerV1) newDestroyPublisherOrSubscriberRequest(proxyID int32, requestType pb1topics.TopicServiceRequestType) (*pb1.ProxyRequest, error) {
	req := &wrapperspb.Int32Value{
		Value: proxyID,
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return nil, err
	}
	return m.newWrapperProxyTopicsRequest("", requestType, anyReq)
}

func (m *streamManagerV1) newEnsureSubscriberRequest(topicName string, subscriberGroup *string, binFilter []byte) (*pb1.ProxyRequest, error) {
	req := &pb1topics.EnsureSubscriberRequest{
		Topic:           topicName,
		Filter:          binFilter,
		SubscriberGroup: subscriberGroup,
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return nil, err
	}
	return m.newWrapperProxyTopicsRequest(topicName, pb1topics.TopicServiceRequestType_EnsureSubscriber, anyReq)
}

func (m *streamManagerV1) newEnsureSubscriberGroupRequest(topicName string, subscriberGroup string, binFilter []byte) (*pb1.ProxyRequest, error) {
	req := &pb1topics.EnsureSubscriberGroupRequest{
		SubscriberGroup: subscriberGroup,
		Filter:          binFilter,
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return nil, err
	}
	return m.newWrapperProxyTopicsRequest(topicName, pb1topics.TopicServiceRequestType_EnsureSubscriberGroup, anyReq)
}

func (m *streamManagerV1) newDestroySubscriberGroupRequest(proxyID int32, subscriberGroup string) (*pb1.ProxyRequest, error) {
	req := &wrapperspb.StringValue{
		Value: subscriberGroup,
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return nil, err
	}
	return m.newWrapperProxyPublisherRequest(proxyID, pb1topics.TopicServiceRequestType_DestroySubscriberGroup, anyReq)

	//return m.newWrapperProxyTopicsRequest("", pb1topics.TopicServiceRequestType_DestroySubscriberGroup, anyReq)
}

//func (m *streamManagerV1) newInitializeSubscriptionRequest(subscriptionID int64, force bool) (*pb1.ProxyRequest, error) {
//	req := &pb1topics.InitializeSubscriptionRequest{
//		SubscriptionId: subscriptionID,
//		ForceReconnect: force,
//	}
//
//	anyReq, err := anypb.New(req)
//	if err != nil {
//		return nil, err
//	}
//	return m.newWrapperProxyTopicsRequest("", pb1topics.TopicServiceRequestType_EnsureSubscription, anyReq)
//}

// genericTopicRequest issues a generic topic request that is further defined by the reqType.
func (m *streamManagerV1) genericTopicRequest(ctx context.Context, reqType pb1topics.TopicServiceRequestType, topic string) error {
	var (
		err       error
		req       *pb1.ProxyRequest
		isDestroy = false
		value     *anypb.Any
	)

	if reqType == pb1topics.TopicServiceRequestType_DestroyTopic {
		isDestroy = true
	}

	if isDestroy {
		// destroy requires sending of the topic name in the message
		value, err = stringToAny(topic)
		if err != nil {
			return err
		}
		req, err = m.newWrapperProxyTopicsRequest(topic, reqType, value)
	} else {
		req, err = m.newGenericNamedTopicRequest(topic, reqType)
	}
	if err != nil {
		return err
	}

	requestType, err := m.submitTopicRequest(req, reqType)
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

	if isDestroy {
		// special case for destroy
		return nil
	}

	_, err1 := waitForResponse(newCtx, requestType.ch)
	if err1 != nil {
		return err1
	}

	return nil
}

func (m *streamManagerV1) newWrapperProxyTopicsRequest(topicName string, requestType pb1topics.TopicServiceRequestType, message *anypb.Any) (*pb1.ProxyRequest, error) {
	var (
		topicID         *int32
		zeroTopic       int32
		noTopicRequired = requestType == pb1topics.TopicServiceRequestType_DestroyTopic ||
			requestType == pb1topics.TopicServiceRequestType_EnsurePublisher ||
			requestType == pb1topics.TopicServiceRequestType_EnsureSubscription ||
			requestType == pb1topics.TopicServiceRequestType_EnsureSubscriber ||
			requestType == pb1topics.TopicServiceRequestType_DestroySubscriberGroup
	)

	// validate the topic ID if it is not an ensure queue request
	if topicName != "" && !noTopicRequired {
		topicID = m.session.getTopicID(topicName)
		if topicID == nil {
			return nil, getTopicIDMessage(topicName)
		}
	}

	// special cases for topic requests that require no topic
	if noTopicRequired {
		topicID = &zeroTopic
	}

	ncRequest, err := newNamedTopicRequest(topicID, requestType, message)

	if err != nil {
		return nil, err
	}

	return m.newProxyRequest(ncRequest), nil
}

func (m *streamManagerV1) newWrapperProxyPublisherRequest(proxyID int32, requestType pb1topics.TopicServiceRequestType, message *anypb.Any) (*pb1.ProxyRequest, error) {
	ncRequest, err := newNamedTopicRequest(&proxyID, requestType, message)

	if err != nil {
		return nil, err
	}

	return m.newProxyRequest(ncRequest), nil
}

func stringToAny(topicName string) (*anypb.Any, error) {
	strValue := wrapperspb.String(topicName)
	return anypb.New(strValue)
}

func newNamedTopicRequest(topicID *int32, reqType pb1topics.TopicServiceRequestType, message *anypb.Any) (*anypb.Any, error) {
	req := &pb1topics.TopicServiceRequest{
		Type:    reqType,
		ProxyId: topicID,
		Message: message,
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return nil, err
	}
	return anyReq, nil
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
	m.session.debugConnection("id: %v submit topic request: %v %v", req.Id, requestType, req)

	return r, m.eventStream.grpcStream.Send(req)
}
