/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package queues

import (
	"context"
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	. "github.com/oracle/coherence-go-client/test/utils"
	"log"
	"sync"
	"testing"
	"time"
)

func TestStandardQueue(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
		value   *string
		ctx     = context.Background()
	)

	session, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedQueue, err := coherence.GetNamedQueue[string](ctx, session, "my-queue")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer namedQueue.Close()

	err = namedQueue.Offer("value1")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSize(g, namedQueue, 1)

	// peek the value
	value, err = namedQueue.Peek()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal("value1"))

	// size should be still 1
	validateQueueSize(g, namedQueue, 1)

	// Poll() and remove the value
	value, err = namedQueue.Poll()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal("value1"))
	validateQueueSize(g, namedQueue, 0)

	// issue another Poll() and we should get nil as no more entries
	value, err = namedQueue.Poll()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).To(gomega.BeNil())

	// add 10 values and ensure we dequeue them in the order they were put on
	for i := 1; i <= 10; i++ {
		err = namedQueue.Offer(fmt.Sprintf("value-%d", i))
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}
	validateQueueSize(g, namedQueue, 10)

	start := 1
	for start <= 10 {
		expected := fmt.Sprintf("value-%d", start)

		// Peek() first
		value, err = namedQueue.Peek()
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(*value).To(gomega.Equal(expected))

		// Poll()
		value, err = namedQueue.Poll()
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(*value).To(gomega.Equal(expected))

		start++
	}

	// when we get here there should be nothing on the queue
	validateQueueSize(g, namedQueue, 0)

	value, err = namedQueue.Poll()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).To(gomega.BeNil())
}

type Customer struct {
	ID      int     `json:"id"`
	Name    string  `json:"name"`
	Balance float32 `json:"balance"`
}

type JavaCustomer struct {
	Class        string `json:"@class"`
	ID           int    `json:"id"`
	CustomerName string `json:"customerName"`
	CustomerType string `json:"customerType"`
}

func TestStandardQueueWithStruct(t *testing.T) {
	var (
		g         = gomega.NewWithT(t)
		err       error
		session   *coherence.Session
		value     *Customer
		customer1 = Customer{ID: 1, Name: "Tim", Balance: 100.25}
		ctx       = context.Background()
	)

	session, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedQueue, err := coherence.GetNamedQueue[Customer](ctx, session, "my-queue")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer namedQueue.Close()

	err = namedQueue.Offer(customer1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSizeCustomer(g, namedQueue, 1)

	// peek the value
	value, err = namedQueue.Peek()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal(customer1))

	// size should be still 1
	validateQueueSizeCustomer(g, namedQueue, 1)

	// Poll() and remove the value
	value, err = namedQueue.Poll()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal(customer1))
	validateQueueSizeCustomer(g, namedQueue, 0)

	// issue another Poll() and we should get nil as no more entries
	value, err = namedQueue.Poll()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).To(gomega.BeNil())

	// add 10 values and ensure we dequeue them in the order they were put on
	for i := 1; i <= 10; i++ {
		err = namedQueue.Offer(Customer{ID: i, Name: fmt.Sprintf("Tim-%d", i), Balance: float32(i) * 1000})
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}
	validateQueueSizeCustomer(g, namedQueue, 10)

	start := 1
	for start <= 10 {
		expected := Customer{ID: start, Name: fmt.Sprintf("Tim-%d", start), Balance: float32(start) * 1000}

		// Peek() first
		value, err = namedQueue.Peek()
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(*value).To(gomega.Equal(expected))

		// Poll()
		value, err = namedQueue.Poll()
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(*value).To(gomega.Equal(expected))

		start++
	}

	// when we get here there should be nothing on the queue
	validateQueueSizeCustomer(g, namedQueue, 0)

	value, err = namedQueue.Poll()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).To(gomega.BeNil())
}

func TestStandardBlockingQueue(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		session2 *coherence.Session
		value    *string
		ctx      = context.Background()
	)

	const queueName = "blocking-queue-1"

	// Note: We use two sessions, so we can have a standard and blocking queue with the same name
	session1, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	session2, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session2.Close()

	receivingQueue, err := coherence.GetBlockingNamedQueue[string](ctx, session1, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer receivingQueue.Close()

	publishingQueue, err := coherence.GetNamedQueue[string](ctx, session2, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	validateQueueSize(g, publishingQueue, 0)

	// try Peek(), should timeout
	value, err = receivingQueue.Peek(time.Duration(2) * time.Second)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err).To(gomega.Equal(coherence.ErrQueueTimedOut))
	g.Expect(value).To(gomega.BeNil())

	// then Poll()
	value, err = receivingQueue.Poll(time.Duration(2) * time.Second)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err).To(gomega.Equal(coherence.ErrQueueTimedOut))
	g.Expect(value).To(gomega.BeNil())

	// publish data to the queue
	err = publishingQueue.Offer("value-1")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try Peek(), should not timeout
	value, err = receivingQueue.Peek(time.Duration(2) * time.Second)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal("value-1"))
	validateQueueSize(g, publishingQueue, 1)

	// try Poll(), should not timeout
	value, err = receivingQueue.Poll(time.Duration(2) * time.Second)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal("value-1"))

	t.Log("Done")
	validateQueueSize(g, publishingQueue, 0)
}

func TestStandardBlockingQueueWithGoRoutines(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		session2 *coherence.Session
		ctx      = context.Background()
		wg       sync.WaitGroup
	)

	const queueName = "blocking-queue-2"

	// Note: We use two sessions, so we can have a standard and blocking queue with the same name
	session1, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	session2, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session2.Close()

	receivingQueue, err := coherence.GetBlockingNamedQueue[string](ctx, session1, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer receivingQueue.Close()

	publishingQueue, err := coherence.GetNamedQueue[string](ctx, session2, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	validateQueueSize(g, publishingQueue, 0)

	count := 100
	wg.Add(1)

	// start a go routine to wait for a specific number of entries
	go func(receive coherence.NamedBlockingQueue[string], count int) {
		var (
			err1          error
			value1        *string
			start         = time.Now()
			maxWaitTime   = 30
			receivedCount = 0
		)

		defer wg.Done()
		for {
			value1, err1 = receivingQueue.Poll(time.Duration(2) * time.Second)
			if err1 == coherence.ErrQueueTimedOut {
				log.Println("timeout")
				if time.Since(start) > time.Duration(maxWaitTime)*time.Second {
					g.Fail(fmt.Sprintf("timeed out after %d seconds", maxWaitTime))
				}
				continue
			}
			g.Expect(err1).ShouldNot(gomega.HaveOccurred())
			g.Expect(*value1).To(gomega.Not(gomega.BeNil()))
			receivedCount++
			log.Println("received", receivedCount)

			if receivedCount == count {
				break
			}
		}
	}(receivingQueue, count)

	// sleep for 10 seconds to allow for the Poll() to try a few times
	time.Sleep(time.Duration(10) * time.Second)

	// now offer count entries to the queue
	for i := 1; i <= count; i++ {
		err = publishingQueue.Offer(fmt.Sprintf("value=%d", i))
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	log.Println("submitted", count)

	// we should be  able to wait for the wg successfully
	wg.Wait()
}

func TestStandardBlockingQueueCloseOperation(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		session2 *coherence.Session
		ctx      = context.Background()
		wg       sync.WaitGroup
	)

	const queueName = "blocking-queue-3"

	// Note: We use two sessions, so we can have a standard and blocking queue with the same name
	session1, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	session2, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session2.Close()

	receivingQueue, err := coherence.GetBlockingNamedQueue[string](ctx, session1, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer receivingQueue.Close()

	publishingQueue, err := coherence.GetNamedQueue[string](ctx, session2, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	validateQueueSize(g, publishingQueue, 0)

	count := 100
	wg.Add(1)

	// start a go routine to wait for a specific number of entries
	go func(receive coherence.NamedBlockingQueue[string], count int) {
		var (
			err1          error
			value1        *string
			start         = time.Now()
			maxWaitTime   = 30
			receivedCount = 0
		)

		defer wg.Done()
		for {
			value1, err1 = receivingQueue.Poll(time.Duration(2) * time.Second)
			if err1 == coherence.ErrQueueTimedOut {
				log.Println("timeout")
				if time.Since(start) > time.Duration(maxWaitTime)*time.Second {
					g.Fail(fmt.Sprintf("timeed out after %d seconds", maxWaitTime))
				}
				continue
			}
			g.Expect(err1).ShouldNot(gomega.HaveOccurred())
			g.Expect(*value1).To(gomega.Not(gomega.BeNil()))
			receivedCount++
			log.Println("received", receivedCount)

			if receivedCount == count {
				log.Println("exiting go routine")
				break
			}
		}
	}(receivingQueue, count)

	// sleep for 10 seconds to allow for the Poll() to try a few times
	time.Sleep(time.Duration(5) * time.Second)

	// now offer count entries to the queue via a go routine and when the count reaches 100 the above
	// blocking Poll(), should complete and we should be able to exit without any locking issues
	go func() {
		for i := 1; i <= 1_000; i++ {
			err = publishingQueue.Offer(fmt.Sprintf("value=%d", i))
		}
		log.Println("Finished")
	}()

	// we should be able to wait for the wg successfully
	wg.Wait()
}

func TestStandardBlockingQueueMultipleGoRoutines(t *testing.T) {
	const (
		queueName     = "blocking-queue-4"
		numGoRoutines = 10
	)

	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		session2 *coherence.Session
		ctx      = context.Background()
		wg       sync.WaitGroup
		results  = make(map[int]int, numGoRoutines)
		mapMutex sync.Mutex
	)

	cancelCtx, cancel := context.WithCancel(context.Background())

	// Note: We use two sessions, so we can have a standard and blocking queue with the same name
	session1, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	session2, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session2.Close()

	receivingQueue, err := coherence.GetBlockingNamedQueue[string](ctx, session1, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer receivingQueue.Close()

	publishingQueue, err := coherence.GetNamedQueue[string](ctx, session2, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	validateQueueSize(g, publishingQueue, 0)

	// start numGoRoutines go routines
	for i := 0; i < numGoRoutines; i++ {
		log.Println("Starting go routine", i)
		go runBlockingDequeue(cancelCtx, receivingQueue, results, i, &mapMutex)
	}

	// sleep for 5 seconds to allow for the Poll() to try a few times
	time.Sleep(time.Duration(5) * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 3_000; i++ {
			err = publishingQueue.Offer(fmt.Sprintf("value=%d", i))
			if i%250 == 0 {
				log.Println("publish", i)
			}
		}
	}()

	// we should be able to wait for the wg successfully
	wg.Wait()

	// we should see at least one entry per go routine
	mapMutex.Lock()
	for k, v := range results {
		log.Printf("routine=%d, count=%d\n", k, v)
		g.Expect(v > 0).To(gomega.BeTrue())
	}
	mapMutex.Unlock()

	log.Println("Done, exiting")
	cancel()
	time.Sleep(time.Second)
	// ensure cancel is processed
	// we should be able to exit fine
}

func TestStandardQueueFromJava(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
		ctx     = context.Background()
		result  *JavaCustomer
	)

	const queueEntries = 1000

	session, err = GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedQueue, err := coherence.GetNamedQueue[JavaCustomer](ctx, session, "test-queue")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer namedQueue.Close()

	// add 1000 entries to the "test-queue" in Java
	_, err = IssueGetRequest(GetTestContext().RestURL + "/populateQueues")
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))
	g.Expect(namedQueue.Size()).To(gomega.Equal(queueEntries))

	for i := 1; i <= queueEntries; i++ {
		result, err = namedQueue.Poll()
		g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))
		g.Expect(result.ID).To(gomega.Equal(i))
		g.Expect(result.CustomerName).To(gomega.Equal(fmt.Sprintf("Name-%d", i)))
		g.Expect(result.CustomerType).To(gomega.Equal("GOLD"))
	}
}

func runBlockingDequeue(cancelCtx context.Context, receivingQueue coherence.NamedBlockingQueue[string], results map[int]int, routine int, mtx *sync.Mutex) {
	var (
		err           error
		receivedCount = 0
	)
	for {
		_, err = receivingQueue.Poll(time.Duration(2) * time.Second)
		if err == coherence.ErrQueueTimedOut {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			// exit the go routine as we may be closing the connection
			return
		}
		receivedCount++
		mtx.Lock()
		results[routine] = receivedCount
		mtx.Unlock()
		if receivedCount%10 == 0 {
			log.Printf("routine=%d, dequeue count=%d...", routine, receivedCount)
		}

		// check for cancel
		select {
		case <-cancelCtx.Done():
			return
		case <-time.After(time.Duration(1) * time.Millisecond):
			continue
		}
	}
}

func validateQueueSize(g *gomega.WithT, namedQueue coherence.NamedQueue[string], expectedSize int) {
	size, err := namedQueue.Size()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(expectedSize))
}

func validateQueueSizeCustomer(g *gomega.WithT, namedQueue coherence.NamedQueue[Customer], expectedSize int) {
	size, err := namedQueue.Size()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(expectedSize))
}
