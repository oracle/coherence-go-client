/*
 * Copyright (c) 2025, Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package perf

import (
	"github.com/onsi/gomega"
	"testing"
)

// TestPerformance1 tests for discovery
func TestPerformance1(t *testing.T) {
	var (
		g = gomega.NewWithT(t)
	)

	size, err := config.Students.Size(ctx)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(size).To(gomega.Equal(maxStudents))
}
