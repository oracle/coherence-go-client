/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package publisher

import "testing"

func TestRoundRobinOrdering(t *testing.T) {
	roundRobin := &OrderByRoundRobin{}
	var (
		i int32
	)

	for i = 1; i <= 10; i++ {
		next := roundRobin.GetPublishHash()
		if next != i {
			t.Fatalf("expected next=%d, got %d", i, next)
		}
	}
}

func TestDefaultOrdering(t *testing.T) {
	defaultOrdering := &OrderByDefault{}
	var (
		i int32
	)

	for i = 1; i <= 10; i++ {
		next := defaultOrdering.GetPublishHash()
		if next < 0 {
			t.Fatalf("expected next=%d, got %d", i, next)
		}
	}
}
