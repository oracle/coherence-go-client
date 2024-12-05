/*
 * Copyright (c) 2024, Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"testing"
)

// These tests assume a 3 node cluster with grpc proxy has been started and Name Service
// is listening on localhost:7574

// TestNsLookupGrpcAddresses tests NsLookupGrpcAddresses.
func TestNsLookupGrpcAddresses(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	results, err := coherence.NsLookupGrpcAddresses("127.0.0.1:7574")
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(len(results)).To(gomega.Equal(3))

	_, err = coherence.NsLookupGrpcAddresses("rubbish")
	g.Expect(err).To(gomega.HaveOccurred())
}

// TestNsLookupGrpcAddresses tests NsLookupGrpcAddresses.
func TestConnectingUsingNSResolver(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	ctx := context.Background()
	t.Setenv("COHERENCE_RESOLVER_DEBUG", "true")

	session, err := coherence.NewSession(ctx, coherence.WithPlainText(), coherence.WithAddress("coherence:///localhost:7574"))
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	defer session.Close()
	utils.RunNSTestWithNamedMap(ctx, g, session, "grpc-ns-test")
}
