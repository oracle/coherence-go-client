/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"github.com/onsi/gomega"
	"testing"
)

func TestNewNamedCacheRequest(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(g).ShouldNot(gomega.BeNil())
}
