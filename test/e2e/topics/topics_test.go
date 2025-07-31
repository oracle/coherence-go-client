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
	"github.com/oracle/coherence-go-client/v2/coherence/subscriber"
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

	RunTestBasicTopicAnonPubSub(g, "-1")
	RunTestBasicTopicAnonPubSub(g, "-2", publisher.WithDefaultOrdering())
	RunTestBasicTopicAnonPubSub(g, "-3", publisher.WithRoundRobinOrdering())
	RunTestBasicTopicAnonPubSub(g, "-4", publisher.WithChannelCount(21))
}

func RunTestBasicTopicAnonPubSub(g *gomega.WithT, suffix string, options ...func(cache *publisher.Options)) {
	var (
		err        error
		ctx        = context.Background()
		maxEntries = 100

		processedMessageCount counter
	)

	var topicName = fmt.Sprintf("my-topic-anon%s", suffix)

	session1, topic1 := getSessionAndTopic[string](g, topicName)
	defer session1.Close()

	go func() {
		var (
			values   []*subscriber.ReceiveResponse[string]
			response *subscriber.CommitResponse
		)
		sub1, err1 := topic1.CreateSubscriber(ctx)
		g.Expect(err1).ShouldNot(gomega.HaveOccurred())
		log.Println("Subscriber created", sub1)

		for {
			values, err1 = sub1.Receive(context.Background())
			g.Expect(err1).ShouldNot(gomega.HaveOccurred())
			if len(values) == 0 {
				// nothing on topic
				utils.Sleep(1)
				continue
			}
			response, err1 = sub1.Commit(ctx, values[0].Channel, values[0].Position)

			g.Expect(err1).ShouldNot(gomega.HaveOccurred())
			g.Expect(response).ShouldNot(gomega.BeNil())
			g.Expect(response.Channel).Should(gomega.Equal(values[0].Channel))
			processedMessageCount.Increment()
		}
	}()

	utils.Sleep(5)

	pub1, err := topic1.CreatePublisher(context.Background(), options...)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	publishEntriesString(g, pub1, maxEntries)

	g.Eventually(func() int { return processedMessageCount.Get() }, 120*time.Second).Should(gomega.Equal(maxEntries))

	utils.Sleep(5)

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

func runTestPerson(g *gomega.WithT, topic1 coherence.NamedTopic[utils.Person], count int, options ...func(options *publisher.Options)) {
	ctx := context.Background()

	pub1, err := topic1.CreatePublisher(context.Background(), options...)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	for i := 1; i <= count; i++ {
		p := utils.Person{
			ID:   i,
			Name: fmt.Sprintf("my-value-%d", i),
			Age:  10 + i,
		}
		status, err2 := pub1.Publish(ctx, p)
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
