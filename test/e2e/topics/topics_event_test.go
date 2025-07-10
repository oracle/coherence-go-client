/*
 * Copyright (c) 2024, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package topics

import (
	"context"
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

func TestTopicsEventsDestroyOnServer(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	const topicName = "topics-events"

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	utils.Sleep(5)

	listener := NewCountingTopicListener[string](topicName)

	g.Expect(topic1.AddLifecycleListener(listener.listener)).ShouldNot(gomega.HaveOccurred())

	_, err = utils.IssueGetRequest(utils.GetTestContext().RestURL + "/destroyTopic/" + topicName)
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	utils.Sleep(5)

	g.Eventually(func() int32 { return listener.destroyCount }).
		WithTimeout(10 * time.Second).Should(gomega.Equal(int32(1)))

	g.Expect(topic1.Close(context.Background())).Should(gomega.Equal(coherence.ErrTopicDestroyedOrReleased))
}

func TestTopicsEventsDestroyByClient(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		ctx = context.Background()
	)

	const topicName = "topics-events-client-destroy"

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	utils.Sleep(5)

	listener := NewCountingTopicListener[string](topicName)

	g.Expect(topic1.AddLifecycleListener(listener.listener)).ShouldNot(gomega.HaveOccurred())

	g.Expect(topic1.Destroy(ctx)).ShouldNot(gomega.HaveOccurred())
	utils.Sleep(5)

	g.Eventually(func() int32 { return listener.destroyCount }).
		WithTimeout(10 * time.Second).Should(gomega.Equal(int32(1)))

	g.Expect(topic1.Close(context.Background())).Should(gomega.Equal(coherence.ErrTopicDestroyedOrReleased))
}

func TestTopicsEventsPublisherDestroy(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		ctx = context.Background()
	)

	const topicName = "topics-events-publisher-destroy"

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	utils.Sleep(5)

	sub1, err := topic1.CreateSubscriber(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	fmt.Println("Subscriber created", sub1)

	pub1, err := topic1.CreatePublisher(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	// destroy the topic and we should see some messages for publisher and subscriber
	_, err = utils.IssueGetRequest(utils.GetTestContext().RestURL + "/destroyTopic/" + topicName)
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	utils.Sleep(5)
	fmt.Println("Done")
}

func NewCountingTopicListener[V any](name string) *CountingTopicListener[V] {
	countingListener := CountingTopicListener[V]{
		name:     name,
		listener: coherence.NewTopicLifecycleListener[V](),
	}

	countingListener.listener.OnDestroyed(func(_ coherence.TopicLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.destroyCount, 1)
	}).OnReleased(func(_ coherence.TopicLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.releaseCount, 1)
	}).OnAny(func(_ coherence.TopicLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.allCount, 1)
	})

	return &countingListener
}

type CountingTopicListener[V any] struct {
	listener      coherence.TopicLifecycleListener[V]
	name          string
	truncateCount int32
	destroyCount  int32
	releaseCount  int32
	allCount      int32
}
