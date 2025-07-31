/*
 * Copyright (c) 2024, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package topics

import (
	"context"
	"errors"
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/publisher"
	"github.com/oracle/coherence-go-client/v2/coherence/subscriber"
	"github.com/oracle/coherence-go-client/v2/coherence/subscribergroup"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"sync"
	"testing"
	"time"
)

var finishedMutex sync.Mutex

func TestSubscriberWithFilter(t *testing.T) {
	var (
		g             = gomega.NewWithT(t)
		err           error
		ctx           = context.Background()
		messageCount  = 100
		actualCount   counter
		timeout       = 60 * time.Second
		expectedCount = 10
		finished      bool
	)

	const topicName = "my-topic-anon-filter"

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer session1.Close()

	// create a subscriber with a filter
	sub1, err := topic1.CreateSubscriber(ctx, subscriber.WithFilter(filters.GreaterEqual(extractors.Extract[int]("age"), 20)))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	go func() {
		results, err1 := receiveMessages(sub1, expectedCount, timeout, &finished, nil)
		log.Printf("Received %d messages: %v", len(results), results)
		actualCount.Set(len(results))
		g.Expect(err1).ShouldNot(gomega.HaveOccurred())
		g.Expect(results).ShouldNot(gomega.BeEmpty())
		g.Expect(len(results)).Should(gomega.Equal(10))
	}()

	utils.Sleep(5)

	go runTestPerson(g, topic1, messageCount)

	utils.Sleep(5)

	g.Eventually(func() int { return actualCount.Get() }, timeout+(time.Second*5)).Should(gomega.Equal(expectedCount))
}

func TestSubscriberWithTransformer(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		err          error
		ctx          = context.Background()
		messageCount = 1_000
		actualCount  counter
		timeout      = 60 * time.Second
		finished     bool
	)

	const topicName = "my-topic-anon-transformer"

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer func() {
		_ = topic1.Destroy(ctx)
		session1.Close()
	}()

	extractor := extractors.Extract[string]("name")

	sub1, err := coherence.CreatSubscriberWithTransformer(ctx, session1, topicName, extractor)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	go func() {
		results, err1 := receiveMessages(sub1, messageCount, timeout, &finished, nil)
		actualCount.Set(len(results))
		log.Printf("Received %d messages: %v", len(results), results)
		g.Expect(err1).ShouldNot(gomega.HaveOccurred())
		g.Expect(results).ShouldNot(gomega.BeEmpty())
		g.Expect(len(results)).Should(gomega.Equal(messageCount))
	}()

	utils.Sleep(5)

	runTestPerson(g, topic1, messageCount)

	utils.Sleep(5)

	g.Eventually(func() int { return actualCount.Get() }, timeout+(time.Second*5)).Should(gomega.Equal(messageCount))
}

func TestSubscriberWithTransformerAndFilter(t *testing.T) {
	var (
		g            = gomega.NewWithT(t)
		err          error
		ctx          = context.Background()
		messageCount = 100
		actualCount  counter
		timeout      = 60 * time.Second
		finished     bool
	)

	const topicName = "my-topic-anon-transformer-filter"

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer func() {
		_ = topic1.Destroy(ctx)
		session1.Close()
	}()

	extractor := extractors.Extract[string]("name")

	sub1, err := coherence.CreatSubscriberWithTransformer(ctx, session1, topicName, extractor,
		subscriber.WithFilter(filters.GreaterEqual(extractors.Extract[int]("age"), 20)))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	log.Println("Subscriber created", sub1)

	go func() {
		results, err1 := receiveMessages(sub1, 10, time.Second*30, &finished, nil)
		actualCount.Set(len(results))
		log.Printf("Received %d messages: %v", len(results), results)
		g.Expect(err1).ShouldNot(gomega.HaveOccurred())
		g.Expect(results).ShouldNot(gomega.BeEmpty())
		g.Expect(len(results)).Should(gomega.Equal(10))
	}()

	utils.Sleep(5)

	runTestPerson(g, topic1, messageCount)

	g.Eventually(func() int { return actualCount.Get() }, timeout+(time.Second*5)).Should(gomega.Equal(10))
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
	defer func() {
		_ = topic1.Destroy(ctx)
		session1.Close()
	}()

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
		topicName = "my-topic-with-sg-2"
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

type counter struct {
	sync.Mutex
	value int
}

func (c *counter) Set(value int) {
	c.Lock()
	defer c.Unlock()
	c.value = value
}

func (c *counter) Get() int {
	c.Lock()
	defer c.Unlock()
	return c.value
}

func (c *counter) Increment() {
	c.Lock()
	defer c.Unlock()
	c.value++
}

func TestMultipleSubscribers(t *testing.T) {
	var (
		allChannels              = []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		g                        = gomega.NewWithT(t)
		emptySubOptions          []func(*subscriber.Options)
		emptyPubOptions          []func(*publisher.Options)
		pub23Channels            = []func(*publisher.Options){publisher.WithChannelCount(23)}
		subGroupOptions          = []func(*subscriber.Options){subscriber.InSubscriberGroup("group1")}
		subGroupOptions2         = []func(*subscriber.Options){subscriber.InSubscriberGroup("group1"), subscriber.WithMaxMessages(10)}
		subGroupOptions2Channels = []func(*subscriber.Options){subscriber.InSubscriberGroup("group1"), subscriber.WithChannels(allChannels)}
	)

	testCases := []struct {
		testName        string
		g               *gomega.WithT
		subscriberCount int
		subOptions      []func(*subscriber.Options)
		pubOptions      []func(*publisher.Options)
		test            func(testName string, g *gomega.WithT, subscriberCount int, subOptions []func(o *subscriber.Options), pubOptions []func(o *publisher.Options))
	}{
		{"SubscribersSubGroup2SubscribersMultiple", g, 2, subGroupOptions2, emptyPubOptions, RunTestMultipleSubscribers},
		{"SubscribersSubGroup2SubscribersMultipleChannels", g, 2, subGroupOptions2Channels, emptyPubOptions, RunTestMultipleSubscribers},

		{"SubscribersSubGroup2Subscribers", g, 2, subGroupOptions, emptyPubOptions, RunTestMultipleSubscribers},
		{"SubscribersSubGroup6Subscribers", g, 6, subGroupOptions, emptyPubOptions, RunTestMultipleSubscribers},
		{"SubscribersSubGroup18Subscribers", g, 18, subGroupOptions, emptyPubOptions, RunTestMultipleSubscribers},
		{"SubscribersSubGroup23Channels12Subscribers", g, 4, subGroupOptions, pub23Channels, RunTestMultipleSubscribers},

		{"SubscribersNoSubGroup2Subscribers", g, 2, emptySubOptions, emptyPubOptions, RunTestMultipleSubscribers},
		{"SubscribersNoSubGroup6Subscribers", g, 6, emptySubOptions, emptyPubOptions, RunTestMultipleSubscribers},
		{"SubscribersNoSubGroup12Subscribers", g, 12, emptySubOptions, emptyPubOptions, RunTestMultipleSubscribers},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(tc.testName, g, tc.subscriberCount, tc.subOptions, tc.pubOptions)
		})
	}
}

func RunTestMultipleSubscribers(testName string, g *gomega.WithT, subscriberCount int,
	subOptions []func(o *subscriber.Options), pubOptions []func(o *publisher.Options)) {
	var (
		ctx                = context.Background()
		receiveCounts      = make([]int, subscriberCount)
		timeout            = time.Second * 600
		maxMessages        = 10_000
		subscriberOptions  = getSubscriberOptions(subOptions...)
		finished           = false
		mutex              sync.Mutex
		wg                 sync.WaitGroup
		topicName          = fmt.Sprintf("my-topic-%s", testName)
		hasSubscriberGroup bool
	)

	session1, topic1 := getSessionAndTopic[utils.Person](g, topicName)
	defer func() {
		_ = topic1.Destroy(ctx)
		session1.Close()
	}()

	wg.Add(subscriberCount)

	for i := 0; i < subscriberCount; i++ {
		go func(subscriberID int) {
			sub1, err1 := topic1.CreateSubscriber(ctx, subOptions...)
			defer func() {
				_ = sub1.Close(ctx)
			}()
			g.Expect(err1).ShouldNot(gomega.HaveOccurred())
			log.Println("Subscriber created", sub1)

			expectedCount := maxMessages
			if subscriberOptions.SubscriberGroup != nil {
				// reset expectedCount to 0 which means it will wait for the timeout rather than the count as
				// with subscriber groups, the messages are only received by one subscriber, not all
				expectedCount = 0
				hasSubscriberGroup = true
			}

			// signal that the subscriber has been created and is ready
			wg.Done()
			results, err1 := receiveMessages(sub1, expectedCount, timeout, &finished, &receiveCounts[subscriberID])
			g.Expect(err1).ShouldNot(gomega.HaveOccurred())
			mutex.Lock()
			receiveCounts[subscriberID] = len(results)
			mutex.Unlock()
			log.Printf("Subscriber %v received %d messages", subscriberID, receiveCounts[subscriberID])
		}(i)
	}

	wg.Wait()
	log.Println("all subscribers ready, sleeping")

	utils.Sleep(5)

	go runTestPerson(g, topic1, maxMessages, pubOptions...)

	valueToMatch := maxMessages
	if !hasSubscriberGroup {
		valueToMatch = maxMessages * subscriberCount
	}

	g.Eventually(func() int {
		mutex.Lock()
		defer mutex.Unlock()

		count := 0
		for _, c := range receiveCounts {
			count += c
		}

		if hasSubscriberGroup {
			// this means the total count should == maxMessages as messages are only consumed
			// by one subscriber in the subscriber group
			if count == maxMessages {
				setFinished(&finished)
			}
		} else if count == maxMessages*subscriberCount {
			// should be subscriber count * maxMessages as each subscriber receives each message
			setFinished(&finished)
		}

		return count
	}, timeout+(time.Second*10)).Should(gomega.Equal(valueToMatch))
}

func getSubscriberOptions(options ...func(*subscriber.Options)) *subscriber.Options {
	var subscriberOptions = &subscriber.Options{}

	// apply any subscriber options
	for _, f := range options {
		f(subscriberOptions)
	}

	return subscriberOptions
}

func isFinished(finished *bool) bool {
	finishedMutex.Lock()
	defer finishedMutex.Unlock()
	return finished != nil && *finished
}

func setFinished(finished *bool) {
	finishedMutex.Lock()
	defer finishedMutex.Unlock()
	*finished = true
}

func receiveMessages[T any](sub coherence.Subscriber[T], expectedCount int, timeout time.Duration, finished *bool, count *int) ([]T, error) {
	var (
		start          = time.Now()
		results        = make([]T, 0)
		messageCounter = 0
	)

	for {
		if time.Since(start) > timeout || *finished {
			if expectedCount > 0 {
				return results, errors.New("timeout")
			}
			return results, nil
		}

		// attempt to receive
		r, err := sub.Receive(context.Background())
		if err != nil {
			return results, err
		}

		if len(r) == 0 {
			// nothing, so sleep a while
			time.Sleep(time.Millisecond * 250)
			continue
		}

		for _, msg := range r {
			results = append(results, *msg.Value)
			messageCounter++
			if count != nil {
				*count = messageCounter
			}
		}

		lastChannel := r[len(r)-1].Channel
		lastPosition := r[len(r)-1].Position

		_, err = sub.Commit(context.Background(), lastChannel, lastPosition)
		if err != nil {
			return nil, err
		}

		if (expectedCount != 0 && messageCounter == expectedCount) || isFinished(finished) {
			return results, nil
		}
	}
}
