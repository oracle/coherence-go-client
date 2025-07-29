/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package subscriber

import (
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	pb1topics "github.com/oracle/coherence-go-client/v2/proto/topics"
	"time"
)

type ReceiveStatus int32

const (
	ReceiveSuccess             = pb1topics.ReceiveStatus_ReceiveSuccess
	ChannelExhausted           = pb1topics.ReceiveStatus_ChannelExhausted
	ChannelNotAllocatedChannel = pb1topics.ReceiveStatus_ChannelNotAllocatedChannel
	UnknownSubscriber          = pb1topics.ReceiveStatus_UnknownSubscriber
)

// EnsureSubscriberResult contains the result of an ensure subscriber request.
type EnsureSubscriberResult struct {
	ProxyID           int32
	SubscriberID      int64
	SubscriberGroupID int64
	UUID              string
}

// Options provides options for creating a subscriber.
type Options struct {
	SubscriberGroup *string
	Filter          filters.Filter
	Extractor       []byte
	AutoCommit      bool
	MaxMessages     int32
}

// CommitResponse represents th response from a [Subscriber] commit.
type CommitResponse struct {
	Channel  int32
	Position *pb1topics.TopicPosition
	Head     *pb1topics.TopicPosition
}

// ReceiveResponse represents a response from a [Subscriber] receive request.
type ReceiveResponse[V any] struct {
	Channel   int32
	Value     *V
	Position  *pb1topics.TopicPosition
	Timestamp time.Time
}

// TODO: Additional options
//// the optional name of the subscriber group
//SubscriberGroup *string `protobuf:"bytes,2,opt,name=subscriberGroup,proto3,oneof" json:"subscriberGroup,omitempty"`
//// an optional ValueExtractor to convert received messages
//Extractor []byte `protobuf:"bytes,4,opt,name=extractor,proto3,oneof" json:"extractor,omitempty"`
//// True to return an empty value if the topic is empty

// InSubscriberGroup returns a function to set the subscriber group for a [Subscriber].
func InSubscriberGroup(group string) func(options *Options) {
	return func(s *Options) {
		s.SubscriberGroup = &group
	}
}

// WithFilter returns a function to set the [filters.Filter] for a [Subscriber].
func WithFilter(fltr filters.Filter) func(options *Options) {
	return func(s *Options) {
		s.Filter = fltr
	}
}

// WithAutoCommit returns a function to set auto commit to be true for a [Subscriber].
func WithAutoCommit() func(options *Options) {
	return func(s *Options) {
		s.AutoCommit = true
	}
}

// WithMaxMessages returns a function to set the maximum messages for a[Subscriber].
func WithMaxMessages(maxMessages int32) func(options *Options) {
	return func(s *Options) {
		s.MaxMessages = maxMessages
	}
}

// WithTransformer returns a function to set the extractor [Subscriber].
func WithTransformer(extractor []byte) func(options *Options) {
	return func(s *Options) {
		s.Extractor = extractor
	}
}

func (o *Options) String() string {
	return fmt.Sprintf("options{SubscriberGroup=%v, filter=%v}", o.SubscriberGroup, o.Filter)
}
