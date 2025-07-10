/*
 * Copyright (c) 2024, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package topics

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/subscriber"
	"github.com/oracle/coherence-go-client/v2/coherence/subscribergroup"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"testing"
)

func TestSubscriberWithFilter(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		ctx = context.Background()
	)

	const topicName = "my-topic-anon-filter"

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer session1.Close()

	// create a subscriber with a filter
	sub1, err := topic1.CreateSubscriber(ctx, subscriber.WithFilter(filters.GreaterEqual(extractors.Extract[int]("age"), 10)))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	runTestPerson(g, topic1, sub1)
}

func TestSubscriberWithTransformer(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		ctx = context.Background()
	)

	const topicName = "my-topic-anon-transformer"

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer session1.Close()

	extractor := extractors.Extract[string]("name")
	// create a subscriber with a transformer, this
	sub1, err := coherence.CreatSubscriberWithTransformer(ctx, session1, topicName, extractor,
		subscriber.WithFilter(filters.GreaterEqual(extractors.Extract[int]("age"), 10)))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	runTestPerson(g, topic1, sub1)
}

func TestSubscriberWithTransformerAndFilter(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		ctx = context.Background()
	)

	const topicName = "my-topic-anon-transformer"

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer session1.Close()

	extractor := extractors.Extract[string]("name")
	// create a subscriber with a transformer, this
	sub1, err := coherence.CreatSubscriberWithTransformer(ctx, session1, topicName, extractor)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	pub1, err := topic1.CreatePublisher(context.Background())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Publisher created", pub1)

	publishEntriesPerson(g, pub1, 1_000)

	utils.Sleep(5)

	err = sub1.Close(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = sub1.Close(ctx)
	g.Expect(err).Should(gomega.HaveOccurred())

	err = topic1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func TestSubscriberGroupWithinTopic(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		ctx = context.Background()
	)

	const (
		topicName = "my-topic-with-sg"
		subGroup  = "sub-group-1"
	)

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer session1.Close()

	err = topic1.CreateSubscriberGroup(ctx, subGroup)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	sub1, err := topic1.CreateSubscriber(ctx, subscriber.InSubscriberGroup(subGroup))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// destroy the subscriber group from the subscriber only
	g.Expect(sub1.DestroySubscriberGroup(ctx, subGroup)).ShouldNot(gomega.HaveOccurred())

	g.Expect(sub1.Close(ctx)).ShouldNot(gomega.HaveOccurred())

	g.Expect(topic1.Destroy(ctx)).ShouldNot(gomega.HaveOccurred())
}

func TestSubscriberGroupWithinSubscriber(t *testing.T) {
	runTestSubscriberGroup(gomega.NewWithT(t), "sub-group-1")
}

func TestSubscriberGroupWithinSubscriberAndFilter(t *testing.T) {
	runTestSubscriberGroup(gomega.NewWithT(t), "sub-group-2", subscribergroup.WithFilter(filters.GreaterEqual(extractors.Extract[int]("age"), 10)))
}

func runTestSubscriberGroup(g *gomega.WithT, subscriberGroup string, options ...func(o *subscribergroup.Options)) {
	var (
		err error
		ctx = context.Background()
	)

	const (
		topicName = "my-topic-with-sg"
	)

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer session1.Close()

	err = topic1.CreateSubscriberGroup(ctx, subscriberGroup, options...)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	sub1, err := topic1.CreateSubscriber(ctx, subscriber.InSubscriberGroup(subscriberGroup))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// destroy the subscriber group from the subscriber only
	g.Expect(sub1.DestroySubscriberGroup(ctx, subscriberGroup)).ShouldNot(gomega.HaveOccurred())

	g.Expect(sub1.Close(ctx)).ShouldNot(gomega.HaveOccurred())

	g.Expect(topic1.Destroy(ctx)).ShouldNot(gomega.HaveOccurred())
}
