/*
 * Copyright (c) 2023, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"sync/atomic"
	"testing"
)

// TestCacheLifecycle runs tests to ensure correct behaviour when working with session events.
func TestSessionLifecycle(t *testing.T) {
	g := gomega.NewWithT(t)

	t.Setenv("COHERENCE_SESSION_DEBUG", "true")

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	listener := NewAllLifecycleEventsListener()
	session.AddSessionLifecycleListener(listener.listener)

	utils.Sleep(15)

	// close the session
	session.Close()

	f := func() int32 { return listener.getClosedCount() }

	// expected the count to increase
	g.Expect(expect[int32](f, 1, 10)).To(gomega.BeNil())

	// try to use the session, we should not be able to
	_, err = coherence.GetNamedMap[int, string](session, "my-map")
	g.Expect(err).To(gomega.Equal(coherence.ErrClosed))
}

type AllSessionLifecycleEventsListener struct {
	listener    coherence.SessionLifecycleListener
	closedCount int32
}

func (e *AllSessionLifecycleEventsListener) getClosedCount() int32 {
	return e.closedCount
}

func NewAllLifecycleEventsListener() *AllSessionLifecycleEventsListener {
	exampleListener := AllSessionLifecycleEventsListener{
		listener: coherence.NewSessionLifecycleListener(),
	}

	exampleListener.listener.OnAny(func(e coherence.SessionLifecycleEvent) {
		if e.Type() == coherence.Closed {
			atomic.AddInt32(&exampleListener.closedCount, 1)
		}
		fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
	})

	return &exampleListener
}
