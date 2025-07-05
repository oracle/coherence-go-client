/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package topic

import "fmt"

// Options provides options for creating topic.
type Options struct {
	ChannelCount int32
}

// WithChannelCount returns a function to set the default channel count on a [NamedTopic].
func WithChannelCount(channelCount int32) func(cacheOptions *Options) {
	return func(t *Options) {
		t.ChannelCount = channelCount
	}
}

func (o *Options) String() string {
	return fmt.Sprintf("options{ChannelCount=%d}", o.ChannelCount)
}
