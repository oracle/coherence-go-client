/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package subscribergroup

import (
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
)

// Options provides options for creating a subscriber.
type Options struct {
	Filter    filters.Filter
	Extractor []byte
}

// WithFilter returns a function to set the [filters.Filter] for a [Subscriber].
func WithFilter(fltr filters.Filter) func(options *Options) {
	return func(s *Options) {
		s.Filter = fltr
	}
}

// WithTransformer returns a function to set the extractor [Subscriber].
func WithTransformer(extractor []byte) func(options *Options) {
	return func(s *Options) {
		s.Extractor = extractor
	}
}

func (o *Options) String() string {
	return fmt.Sprintf("options{filter=%v}", o.Filter)
}
