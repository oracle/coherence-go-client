/*
 * Copyright (c) 2024, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
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
	t.Setenv("COHERENCE_LOG_LEVEL", "DEBUG")

	session, err := coherence.NewSession(ctx, coherence.WithPlainText(), coherence.WithAddress("coherence:///localhost:7574"))
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	defer session.Close()
	RunNSTestWithNamedMap(ctx, g, session, "grpc-ns-test")
}

func RunNSTestWithNamedMap(ctx context.Context, g *gomega.WithT, session *coherence.Session, cache string) {
	namedMap, err := coherence.GetNamedMap[string, string](session, cache)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	defer func() {
		_ = namedMap.Destroy(ctx)
	}()

	_, err = namedMap.Put(ctx, "one", "ONE")
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	size, err := namedMap.Size(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(size).To(gomega.Equal(1))
}
