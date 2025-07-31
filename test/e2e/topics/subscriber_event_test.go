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
	"github.com/oracle/coherence-go-client/v2/coherence/subscriber"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubscriberEventsDestroyOnServer(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	t.Skip("temporarily disable")

	const (
		topicName           = "subscriber-events"
		subscriberGroupName = "subscriber-events"
	)

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	sub, err := topic1.CreateSubscriber(context.Background(), subscriber.InSubscriberGroup(subscriberGroupName))
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	utils.Sleep(5)

	listener := NewCountingSubscriberListener[string](topicName)

	g.Expect(sub.AddLifecycleListener(listener.listener)).ShouldNot(gomega.HaveOccurred())

	_, err = utils.IssueGetRequest(utils.GetTestContext().RestURL + "/destroyTopic/" + topicName)
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	utils.Sleep(5)

	g.Eventually(func() int32 { return listener.subscriberGrpCount }).
		WithTimeout(10 * time.Second).Should(gomega.Equal(int32(1)))

	//g.Expect(topic1.Close(context.Background())).Should(gomega.Equal(coherence.ErrTopicDestroyedOrReleased))
}

func TestSubscriberEventsDestroyByClient(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		ctx = context.Background()
	)

	const topicName = "subscriber-events-client-destroy"

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	sub, err := topic1.CreateSubscriber(context.Background())
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))

	utils.Sleep(5)

	listener := NewCountingSubscriberListener[string](topicName)

	g.Expect(sub.AddLifecycleListener(listener.listener)).ShouldNot(gomega.HaveOccurred())

	g.Expect(sub.Close(ctx)).ShouldNot(gomega.HaveOccurred())
	utils.Sleep(1)

	g.Eventually(func() int32 { return listener.releaseCount }).
		WithTimeout(10 * time.Second).Should(gomega.Equal(int32(1)))
}

func NewCountingSubscriberListener[V any](name string) *CountingSubscriberListener[V] {
	countingListener := CountingSubscriberListener[V]{
		name:     name,
		listener: coherence.NewSubscriberLifecycleListener[V](),
	}

	countingListener.listener.OnDestroyed(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.destroyCount, 1)
	}).OnReleased(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.releaseCount, 1)
	}).OnDisconnected(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.disconnectedCount, 1)
	}).OnUnsubscribed(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.unsubscribedCount, 1)
	}).OnChannelPopulated(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.populatedCount, 1)
	}).OnChannelAllocated(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.allocatedCount, 1)
	}).OnChannelHeadChanged(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.headChangedCount, 1)
	}).OnSubscriberGroupDestroyed(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.subscriberGrpCount, 1)
	}).OnAny(func(_ coherence.SubscriberLifecycleEvent[V]) {
		atomic.AddInt32(&countingListener.allCount, 1)
	})

	return &countingListener
}

type CountingSubscriberListener[V any] struct {
	listener           coherence.SubscriberLifecycleListener[V]
	name               string
	releaseCount       int32
	disconnectedCount  int32
	unsubscribedCount  int32
	populatedCount     int32
	allocatedCount     int32
	headChangedCount   int32
	subscriberGrpCount int32
	destroyCount       int32
	allCount           int32
}
