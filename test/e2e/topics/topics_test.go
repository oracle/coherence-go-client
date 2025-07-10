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
	"github.com/oracle/coherence-go-client/v2/coherence/publisher"
	"github.com/oracle/coherence-go-client/v2/coherence/topic"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"testing"
	"time"
)

func TestBasicTopicCreatedAndDestroy(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		ctx = context.Background()
	)

	const topicName = "my-topic"

	t.Setenv("COHERENCE_LOG_LEVEL", "ALL")

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	utils.Sleep(5)

	err = topic1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// test again and we should get error
	err = topic1.Destroy(ctx)
	g.Expect(err).Should(gomega.HaveOccurred())
}

func TestTopicPublish(t *testing.T) {
	g := gomega.NewWithT(t)

	RunTestBasicTopicAnonPubSub(g)
	RunTestBasicTopicAnonPubSub(g, publisher.WithDefaultOrdering())
	RunTestBasicTopicAnonPubSub(g, publisher.WithRoundRobinOrdering())
	RunTestBasicTopicAnonPubSub(g, publisher.WithChannelCount(21))
}

func RunTestBasicTopicAnonPubSub(g *gomega.WithT, options ...func(cache *publisher.Options)) {
	var (
		err error
		ctx = context.Background()
	)

	const topicName = "my-topic-anon"

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	// create a subscriber first
	sub1, err := topic1.CreateSubscriber(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	pub1, err := topic1.CreatePublisher(context.Background())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	publishEntriesString(g, pub1, 1_000)

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

	const topicName = "my-topic-anon-2"

	session1, err = utils.GetSession(coherence.WithRequestTimeout(300 * time.Second))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	sub1, err := coherence.CreateSubscriber[string](ctx, session1, topicName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	fmt.Println("Subscriber created", sub1)

	pub1, err := coherence.CreatePublisher[string](ctx, session1, topicName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	publishEntriesString(g, pub1, 1_000)

	utils.Sleep(5)

	err = pub1.Close(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try to close again
	err = pub1.Close(ctx)
	g.Expect(err).Should(gomega.HaveOccurred())

	// get the topic so we can destroy
	topic1, err := coherence.GetNamedTopic[string](ctx, session1, topicName, topic.WithChannelCount(17))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = topic1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func runTestPerson[E any](g *gomega.WithT, topic1 coherence.NamedTopic[utils.Person], s coherence.Subscriber[E]) {
	ctx := context.Background()

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

	err = s.Close(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = s.Close(ctx)
	g.Expect(err).Should(gomega.HaveOccurred())
}

func runTestString[E any](g *gomega.WithT, topic1 coherence.NamedTopic[string], s coherence.Subscriber[E]) {
	ctx := context.Background()

	pub1, err := topic1.CreatePublisher(context.Background())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	for i := 1; i <= 1_000; i++ {
		status, err2 := pub1.Publish(ctx, fmt.Sprintf("my-value-%d", i))
		g.Expect(err2).ShouldNot(gomega.HaveOccurred())
		g.Expect(status).ShouldNot(gomega.BeNil())
	}

	utils.Sleep(5)

	err = s.Close(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = s.Close(ctx)
	g.Expect(err).Should(gomega.HaveOccurred())
}

func publishEntriesPerson(g *gomega.WithT, pub coherence.Publisher[utils.Person], count int) {
	ctx := context.Background()
	for i := 1; i <= count; i++ {
		p := utils.Person{
			ID:   i,
			Name: fmt.Sprintf("my-value-%d", i),
			Age:  10 + i,
		}
		status, err2 := pub.Publish(ctx, p)
		g.Expect(err2).ShouldNot(gomega.HaveOccurred())
		g.Expect(status).ShouldNot(gomega.BeNil())
	}
}

func publishEntriesString(g *gomega.WithT, pub coherence.Publisher[string], count int) {
	ctx := context.Background()
	for i := 1; i <= count; i++ {
		status, err2 := pub.Publish(ctx, fmt.Sprintf("value-%d", i))
		g.Expect(err2).ShouldNot(gomega.HaveOccurred())
		g.Expect(status).ShouldNot(gomega.BeNil())
	}
}

func getSessionAndTopic[V any](g *gomega.WithT, topicName string) (*coherence.Session, coherence.NamedTopic[V]) {
	session1, err := utils.GetSession(coherence.WithRequestTimeout(300 * time.Second))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	topic1, err := coherence.GetNamedTopic[V](context.Background(), session1, topicName, topic.WithChannelCount(17))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println(topic1)

	return session1, topic1
}
