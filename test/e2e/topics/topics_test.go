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
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/subscriber"
	"github.com/oracle/coherence-go-client/v2/coherence/topic"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"testing"
	"time"
)

func TestBasicTopicCreatedAndDestroy(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		topic1   coherence.NamedTopic[string]
		ctx      = context.Background()
	)

	const topicName = "my-topic"

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, err = utils.GetSession(coherence.WithRequestTimeout(300 * time.Second))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	// get a NamedQueue with name "my-queue"
	topic1, err = coherence.GetNamedTopic[string](ctx, session1, topicName, topic.WithChannelCount(17))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println(topic1)

	utils.Sleep(5)

	err = topic1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// test again and we should get error
	err = topic1.Destroy(ctx)
	g.Expect(err).Should(gomega.HaveOccurred())
}

func TestBasicTopicAnonPubSub(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		topic1   coherence.NamedTopic[string]
		ctx      = context.Background()
	)

	const topicName = "my-topic-anon"

	session1, err = utils.GetSession(coherence.WithRequestTimeout(300 * time.Second))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	// get a NamedQueue with name "my-queue"
	topic1, err = coherence.GetNamedTopic[string](ctx, session1, topicName, topic.WithChannelCount(17))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println(topic1)

	// create a subscriber first
	sub1, err := topic1.CreateSubscriber(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	pub1, err := topic1.CreatePublisher(context.Background())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	for i := 1; i <= 1_000; i++ {
		status, err2 := pub1.Publish(ctx, fmt.Sprintf("my-value-%d", i))
		g.Expect(err2).ShouldNot(gomega.HaveOccurred())
		g.Expect(status).ShouldNot(gomega.BeNil())
	}

	utils.Sleep(5)

	err = sub1.Close(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = pub1.Close(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = topic1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func TestCreatePubSubWithoutCreatingTopic(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		ctx      = context.Background()
	)

	const topicName = "my-topic-anon"

	session1, err = utils.GetSession(coherence.WithRequestTimeout(300 * time.Second))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	sub1, err := coherence.CreateSubscriber[string](ctx, session1, topicName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	fmt.Println("Subscriber created", sub1)

	pub1, err := coherence.CreatePublisher[string](ctx, session1, topicName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	for i := 1; i <= 1_000; i++ {
		status, err2 := pub1.Publish(ctx, fmt.Sprintf("my-value-%d", i))
		g.Expect(err2).ShouldNot(gomega.HaveOccurred())
		g.Expect(status).ShouldNot(gomega.BeNil())
	}

	utils.Sleep(5)

	err = pub1.Close(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try to close again
	err = pub1.Close(ctx)
	g.Expect(err).Should(gomega.HaveOccurred())

	// get teh topic so we can destroy
	topic1, err := coherence.GetNamedTopic[string](ctx, session1, topicName, topic.WithChannelCount(17))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = topic1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func TestSubscriberWithFilter(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		topic1   coherence.NamedTopic[utils.Person]
		ctx      = context.Background()
	)

	const topicName = "my-topic-anon-filter"

	session1, err = utils.GetSession(coherence.WithRequestTimeout(300 * time.Second))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	topic1, err = coherence.GetNamedTopic[utils.Person](ctx, session1, topicName, topic.WithChannelCount(17))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println(topic1)

	// create a subscriber with a filter
	sub1, err := topic1.CreateSubscriber(ctx, subscriber.WithFilter(filters.GreaterEqual(extractors.Extract[int]("age"), 10)))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	pub1, err := topic1.CreatePublisher(context.Background())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	for i := 1; i <= 1_000; i++ {
		p := utils.Person{
			ID:   i,
			Name: fmt.Sprintf("my-value-%d", i),
			Age:  10 + i,
		}
		status, err2 := pub1.Publish(ctx, p)
		g.Expect(err2).ShouldNot(gomega.HaveOccurred())
		g.Expect(status).ShouldNot(gomega.BeNil())
	}

	utils.Sleep(5)

	err = sub1.Close(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = sub1.Close(ctx)
	g.Expect(err).Should(gomega.HaveOccurred())

	err = topic1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}
