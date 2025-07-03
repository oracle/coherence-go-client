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

type ReceiveResult[V any] struct {
	Status ReceiveStatus
}

// Options provides options for creating a subscriber.
type Options struct {
	SubscriberGroup *string
	Filter          filters.Filter
}

// TODO: Additional options
//// the optional name of the subscriber group
//SubscriberGroup *string `protobuf:"bytes,2,opt,name=subscriberGroup,proto3,oneof" json:"subscriberGroup,omitempty"`
//// an optional Filter to filter received messages
//Filter []byte `protobuf:"bytes,3,opt,name=filter,proto3,oneof" json:"filter,omitempty"`
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

func (o *Options) String() string {
	return fmt.Sprintf("options{SubscriberGroup=%v, filter=%v}", o.SubscriberGroup, o.Filter)
}
