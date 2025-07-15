/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package topics

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublisherEventsDestroyOnServer(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	const topicName = "publisher-events"

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	pub, err := topic1.CreatePublisher(context.Background())
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	utils.Sleep(5)

	listener := NewCountingPublisherListener[string](topicName)

	g.Expect(pub.AddLifecycleListener(listener.listener)).ShouldNot(gomega.HaveOccurred())

	_, err = utils.IssueGetRequest(utils.GetTestContext().RestURL + "/destroyTopic/" + topicName)
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	utils.Sleep(5)

	g.Eventually(func() int32 { return listener.destroyCount }).
		WithTimeout(10 * time.Second).Should(gomega.Equal(int32(1)))

	//g.Expect(topic1.Close(context.Background())).Should(gomega.Equal(coherence.ErrTopicDestroyedOrReleased))
}

func TestPublisherEventsDestroyByClient(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		ctx = context.Background()
	)

	const topicName = "publisher-events-client-destroy"

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	pub, err := topic1.CreatePublisher(context.Background())
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	utils.Sleep(5)

	listener := NewCountingPublisherListener[string](topicName)

	g.Expect(pub.AddLifecycleListener(listener.listener)).ShouldNot(gomega.HaveOccurred())

	g.Expect(pub.Close(ctx)).ShouldNot(gomega.HaveOccurred())
	utils.Sleep(1)

	g.Eventually(func() int32 { return listener.releaseCount }).
		WithTimeout(10 * time.Second).Should(gomega.Equal(int32(1)))
}

func NewCountingPublisherListener[V any](name string) *CountingPublisherListener[V] {
	countingListener := CountingPublisherListener[V]{
		name:     name,
		listener: coherence.NewPublisherLifecycleListener[V](),
	}

	countingListener.listener.OnDestroyed(func(_ coherence.PublisherLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.destroyCount, 1)
	}).OnReleased(func(_ coherence.PublisherLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.releaseCount, 1)
	}).OnAny(func(_ coherence.PublisherLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.allCount, 1)
	})

	return &countingListener
}

type CountingPublisherListener[V any] struct {
	listener     coherence.PublisherLifecycleListener[V]
	name         string
	releaseCount int32
	destroyCount int32
	allCount     int32
}
