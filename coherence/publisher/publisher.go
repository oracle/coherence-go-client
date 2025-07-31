/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package publisher

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sync/atomic"
)

var (
	_ OrderingOption = &OrderByDefault{}
	_ OrderingOption = &OrderByRoundRobin{}
	//_ OrderingOption = &OrderByValue{}
)

// PublishStatus provides the result of a publish operation.
type PublishStatus struct {
	PublishedChannel  int32
	RemainingCapacity int32
}

// Options provides options for creating a publisher.
type Options struct {
	ChannelCount int32
	Ordering     OrderingOption
}

func (o Options) String() string {
	return fmt.Sprintf("options{ChannelCount:%d, ordering=%v}", o.ChannelCount, o.Ordering)
}

// EnsurePublisherResult contains the result of an ensure publisher request.
type EnsurePublisherResult struct {
	ProxyID      int32
	PublisherID  int64
	ChannelCount int32
}

func (o Options) GetChannelCount() int32 {
	return o.ChannelCount
}

func (o Options) GetOrdering() OrderingOption {
	return o.Ordering
}

// WithChannelCount returns a function to set the channels for a [Publisher].
func WithChannelCount(channelCount int32) func(options *Options) {
	return func(t *Options) {
		t.ChannelCount = channelCount
	}
}

// WithDefaultOrdering returns a function to set the ordering for a [Publisher].
func WithDefaultOrdering() func(options *Options) {
	return func(t *Options) {
		t.Ordering = &OrderByDefault{}
	}
}

// WithRoundRobinOrdering returns a function to set the ordering to round robin for a [Publisher].
func WithRoundRobinOrdering() func(options *Options) {
	return func(t *Options) {
		t.Ordering = &OrderByRoundRobin{}
	}
}

// OrderingOption defines the type of publisher ordering.
type OrderingOption interface {
	GetPublishHash() int32
}

// OrderByDefault defines default ordering.
type OrderByDefault struct {
}

func (o *OrderByDefault) GetPublishHash() int32 {
	// #nosec G404 -- math/rand is fine here for non-security use
	return rand.Int32()
}

func (o *OrderByDefault) String() string {
	return "default"
}

// OrderByRoundRobin defines default ordering.
type OrderByRoundRobin struct {
	counter int64
}

func (o *OrderByRoundRobin) String() string {
	return "roundRobin"
}

func (o *OrderByRoundRobin) GetPublishHash() int32 {
	newVal := atomic.AddInt64(&o.counter, 1)

	if newVal >= int64(math.MaxInt32) {
		atomic.CompareAndSwapInt64(&o.counter, newVal, 0)
		return 0
	}

	// #nosec G115 -- val is guaranteed to be in int32 range
	return int32(newVal)
}
