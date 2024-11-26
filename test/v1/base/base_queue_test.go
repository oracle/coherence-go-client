/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package base

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"testing"
)

// TestEnsureCache tests the ensureCache request.
func TestEnsureQueue(t *testing.T) {
	t.Setenv("COHERENCE_MESSAGE_DEBUG", "true")
	g := gomega.NewWithT(t)
	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureQueue(g, session, "test", coherence.Queue)
	_ = ensureQueue(g, session, "test2", coherence.Queue)
	_ = ensureQueue(g, session, "test3", coherence.Queue)
	_ = ensureQueue(g, session, "test4", coherence.PagedQueue)
	_ = ensureQueue(g, session, "test5", coherence.Dequeue)

}

func ensureQueue(g *gomega.WithT, session *coherence.Session, queue string, queueType coherence.NamedQueueType) *int32 {
	ctx := context.Background()

	queueID, err := coherence.TestEnsureQueue(ctx, session, queue, queueType)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(queueID).ShouldNot(gomega.BeNil())

	id := coherence.GetSessionQueueID(session, queue)
	g.Expect(id).ShouldNot(gomega.BeNil())
	g.Expect(*id).To(gomega.Equal(*queueID))
	return queueID
}
