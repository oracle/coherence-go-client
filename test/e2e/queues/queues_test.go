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
	"github.com/oracle/coherence-go-client/test/utils"
	"sync"
	"testing"
)

const (
	value1 = "value-1"
	value2 = "value-2"
)

func TestQueueTypeValidation(t *testing.T) {
	var (
		g          = gomega.NewWithT(t)
		err        error
		session    *coherence.Session
		queue1     coherence.NamedQueue[string]
		queue1Same coherence.NamedQueue[string]
		queue2     coherence.NamedQueue[string]
		queue2Same coherence.NamedQueue[string]
		ctx        = context.Background()
	)

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	// cannot create a Dequeue using GetNamedQueue
	_, err = coherence.GetNamedQueue[string](ctx, session, "badq", coherence.Dequeue)
	g.Expect(err).Should(gomega.HaveOccurred())

	queue1, err = coherence.GetNamedQueue[string](ctx, session, "q1", coherence.Queue)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(queue1).ShouldNot(gomega.BeNil())

	// should get back the same queue
	queue1Same, err = coherence.GetNamedQueue[string](ctx, session, "q1", coherence.Queue)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(queue1Same).ShouldNot(gomega.BeNil())
	g.Expect(queue1Same).To(gomega.Equal(queue1))

	// this should fail as we can't create a new queue with different type params and same name
	_, err = coherence.GetNamedQueue[int](ctx, session, "q1", coherence.Queue)
	g.Expect(err).Should(gomega.HaveOccurred())

	// this should fail as we can't create another queue, that is already defined as a different type
	_, err = coherence.GetNamedQueue[string](ctx, session, "q1", coherence.PagedQueue)
	g.Expect(err).Should(gomega.HaveOccurred())

	queue2, err = coherence.GetNamedQueue[string](ctx, session, "q1paged", coherence.PagedQueue)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(queue2).ShouldNot(gomega.BeNil())

	// should get back the same queue
	queue2Same, err = coherence.GetNamedQueue[string](ctx, session, "q1paged", coherence.PagedQueue)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(queue2Same).ShouldNot(gomega.BeNil())
	g.Expect(queue2Same).To(gomega.Equal(queue2))

	// should not be able to get normal queue back.
	_, err = coherence.GetNamedQueue[string](ctx, session, "q1paged", coherence.Queue)
	g.Expect(err).Should(gomega.HaveOccurred())
}
func TestQueueVDequeue(t *testing.T) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		session1 *coherence.Session
		queue1   coherence.NamedQueue[string]
		queue2   coherence.NamedDequeue[string]
		ctx      = context.Background()
	)

	const queueName = "my-queue"

	session1, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session1.Close()

	// get a NamedQueue with name "my-queue"
	queue1, err = coherence.GetNamedQueue[string](ctx, session1, queueName, coherence.Queue)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// get a Dequeue with the same name, should fail
	_, err = coherence.GetNamedDeQueue[string](ctx, session1, queueName)
	g.Expect(err).Should(gomega.HaveOccurred())

	err = queue1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// now try the other way around
	queue2, err = coherence.GetNamedDeQueue[string](ctx, session1, queueName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	_, err = coherence.GetNamedQueue[string](ctx, session1, queueName, coherence.Queue)
	g.Expect(err).Should(gomega.HaveOccurred())

	err = queue2.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func TestDequeue(t *testing.T) {
	var (
		ctx   = context.Background()
		g     = gomega.NewWithT(t)
		value *string
	)

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedDequeue, err := coherence.GetNamedDeQueue[string](ctx, session, "dequeue")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// offer to the tail
	err = namedDequeue.OfferTail(ctx, value1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSize(g, namedDequeue, 1)

	// peek the head, should be "value1"
	value, err = namedDequeue.PeekHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).ShouldNot(gomega.BeNil())
	g.Expect(*value).Should(gomega.Equal(value1))

	// offer to the head
	err = namedDequeue.OfferHead(ctx, value2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSize(g, namedDequeue, 2)

	// peek the head and it should be "value2"
	value, err = namedDequeue.PeekHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).ShouldNot(gomega.BeNil())
	g.Expect(*value).Should(gomega.Equal(value2))

	// peek the tail, should be "value1"
	value, err = namedDequeue.PeekTail(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).ShouldNot(gomega.BeNil())
	g.Expect(*value).Should(gomega.Equal(value1))

	// poll the tail, should be "value1"
	value, err = namedDequeue.PeekTail(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).ShouldNot(gomega.BeNil())
	g.Expect(*value).Should(gomega.Equal(value1))

	err = namedDequeue.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func TestReleaseQueue(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	runTestReleaseQueue(g, session, "standard-q-release", coherence.Queue)
	runTestReleaseQueue(g, session, "paged-q-release", coherence.PagedQueue)
	runTestReleaseQueue(g, session, "double-q-release", coherence.Dequeue)
}

func runTestReleaseQueue(g *gomega.WithT, session *coherence.Session, queueName string, queueType coherence.NamedQueueType) {
	var (
		err   error
		queue coherence.NamedQueue[string]
		ctx   = context.Background()
		value *string
	)

	queue = getQueue[string](g, session, queueName, queueType)

	// add to the queue
	err = queue.OfferTail(ctx, value1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSize(g, queue, 1)

	// release the queue, it should not destroy, and we can get another
	queue.Release()

	// re-get the queue
	queue = getQueue[string](g, session, queueName, queueType)

	value, err = queue.PollHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).ShouldNot(gomega.BeNil())
	g.Expect(*value).Should(gomega.Equal(value1))
	validateQueueSize(g, queue, 0)

	// destroy the queue
	err = queue.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func TestStandardQueue(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	runTestStandardQueue(g, session, "standard-q", coherence.Queue)
	runTestStandardQueue(g, session, "paged-q", coherence.PagedQueue)
	runTestStandardQueue(g, session, "dequeue-q", coherence.Dequeue)
}

func runTestStandardQueue(g *gomega.WithT, session *coherence.Session, queueName string, queueType coherence.NamedQueueType) {
	var (
		err     error
		queue   coherence.NamedQueue[string]
		ctx     = context.Background()
		isReady bool
		isEmpty bool
		value   *string
	)

	queue = getQueue[string](g, session, queueName, queueType)

	err = queue.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// Get the queue size
	validateQueueSize(g, queue, 0)

	// confirm IsEmpty
	isEmpty, err = queue.IsEmpty(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(isEmpty).To(gomega.BeTrue())

	// confirm IsReady
	isReady, err = queue.IsReady(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(isReady).To(gomega.BeTrue())

	// add to the queue
	err = queue.OfferTail(ctx, value1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSize(g, queue, 1)

	// ensure IsEmpty is false
	isEmpty, err = queue.IsEmpty(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(isEmpty).To(gomega.BeFalse())

	err = queue.OfferTail(ctx, value2)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSize(g, queue, 2)

	// peek and we should see value1
	value, err = queue.PeekHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).ShouldNot(gomega.BeNil())
	g.Expect(*value).Should(gomega.Equal(value1))
	validateQueueSize(g, queue, 2)

	// remove the first value
	value, err = queue.PollHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).ShouldNot(gomega.BeNil())
	g.Expect(*value).Should(gomega.Equal(value1))
	validateQueueSize(g, queue, 1)

	// remove the second
	value, err = queue.PollHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).ShouldNot(gomega.BeNil())
	g.Expect(*value).Should(gomega.Equal(value2))
	validateQueueSize(g, queue, 0)

	// try peek or poll on empty queue
	value, err = queue.PeekHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).Should(gomega.BeNil())

	value, err = queue.PollHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).Should(gomega.BeNil())

	// clear the queue
	err = queue.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSize(g, queue, 0)

	// destroy the queue
	err = queue.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	id := coherence.GetSessionQueueID(session, queueName)
	g.Expect(id).To(gomega.BeNil())
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
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	runTestStandardQueueWithStruct(g, session, "struct-standard-q", coherence.Queue)
	runTestStandardQueueWithStruct(g, session, "struct-paged-q", coherence.PagedQueue)
	runTestStandardQueueWithStruct(g, session, "struct-dq", coherence.Dequeue)
}

func runTestStandardQueueWithStruct(g *gomega.WithT, session *coherence.Session, queueName string, queueType coherence.NamedQueueType) {
	var (
		err       error
		value     *Customer
		queue     coherence.NamedQueue[Customer]
		customer1 = Customer{ID: 1, Name: "Tim", Balance: 100.25}
		ctx       = context.Background()
	)

	queue = getQueue[Customer](g, session, queueName, queueType)

	err = queue.OfferTail(ctx, customer1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	validateQueueSizeCustomer(g, queue, 1)

	// peek the value
	value, err = queue.PeekHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal(customer1))

	// size should be still 1
	validateQueueSizeCustomer(g, queue, 1)

	// Poll() and remove the value
	value, err = queue.PollHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal(customer1))
	validateQueueSizeCustomer(g, queue, 0)

	// issue another Poll() and we should get nil as no more entries
	value, err = queue.PollHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).To(gomega.BeNil())

	// add 10 values and ensure we dequeue them in the order they were put on
	for i := 1; i <= 10; i++ {
		err = queue.OfferTail(ctx, Customer{ID: i, Name: fmt.Sprintf("Tim-%d", i), Balance: float32(i) * 1000})
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}
	validateQueueSizeCustomer(g, queue, 10)

	start := 1
	for start <= 10 {
		expected := Customer{ID: start, Name: fmt.Sprintf("Tim-%d", start), Balance: float32(start) * 1000}

		// Peek() first
		value, err = queue.PeekHead(ctx)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(*value).To(gomega.Equal(expected))

		// Poll()
		value, err = queue.PollHead(ctx)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(*value).To(gomega.Equal(expected))

		start++
	}

	// when we get here there should be nothing on the queue
	validateQueueSizeCustomer(g, queue, 0)

	value, err = queue.PollHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).To(gomega.BeNil())

	err = queue.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func TestStandardQueueWithGoRoutines(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	runTestStandardQueueWithGoRoutines(g, session, "goroutines-q", coherence.Queue)
	runTestStandardQueueWithGoRoutines(g, session, "goroutines-paged-q", coherence.PagedQueue)
	runTestStandardQueueWithGoRoutines(g, session, "goroutines-dq", coherence.Dequeue)
}

func runTestStandardQueueWithGoRoutines(g *gomega.WithT, session *coherence.Session, queueName string, queueType coherence.NamedQueueType) {
	var (
		wg    sync.WaitGroup
		queue coherence.NamedQueue[string]
	)

	const processCount = 5_000

	queue = getQueue[string](g, session, queueName, queueType)

	wg.Add(2)

	go testGoRoutines(g, &wg, queue, processCount, true)  // write
	go testGoRoutines(g, &wg, queue, processCount, false) // read

	wg.Wait()

}

func testGoRoutines(g *gomega.WithT, wg *sync.WaitGroup, queue coherence.NamedQueue[string], count int, write bool) {
	var (
		err   error
		value *string
		ctx   = context.Background()
	)

	defer wg.Done()

	for i := 1; i <= count; i++ {
		if write {
			err = queue.OfferTail(ctx, fmt.Sprintf("value-%d", i))
			g.Expect(err).ShouldNot(gomega.HaveOccurred())
		} else {
			// read
			for {
				value, err = queue.PollHead(ctx)
				g.Expect(err).ShouldNot(gomega.HaveOccurred())
				// we may not actually get a value depending upon the go-routines
				if value != nil {
					break
				}
			}
		}
	}
}

func TestStandardQueueFromJava(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		session *coherence.Session
		result  *JavaCustomer
		ctx     = context.Background()
		i       int32
	)

	const queueEntries int32 = 1000

	session, err = utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	namedQueue, err := coherence.GetNamedQueue[JavaCustomer](ctx, session, "test-queue", coherence.Queue)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// add 1000 entries to the "test-queue" in Java
	_, err = utils.IssueGetRequest(utils.GetTestContext().RestURL + "/populateQueues")
	g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))
	g.Expect(namedQueue.Size(ctx)).To(gomega.Equal(queueEntries))

	for i = 1; i <= queueEntries; i++ {
		result, err = namedQueue.PollHead(ctx)
		g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))
		g.Expect(result.ID).To(gomega.Equal(int(i)))
		g.Expect(result.CustomerName).To(gomega.Equal(fmt.Sprintf("Name-%d", i)))
		g.Expect(result.CustomerType).To(gomega.Equal("GOLD"))
	}

	newCustomer := JavaCustomer{
		ID:           100000,
		CustomerName: "Tim",
		CustomerType: "GOLD",
	}

	err = namedQueue.OfferTail(ctx, newCustomer)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	c1, err := namedQueue.PollHead(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*c1).To(gomega.Equal(newCustomer))

	err = namedQueue.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func validateQueueSize(g *gomega.WithT, namedQueue coherence.NamedQueue[string], expectedSize int32) {
	size, err := namedQueue.Size(context.Background())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(expectedSize))
}

func validateQueueSizeCustomer(g *gomega.WithT, namedQueue coherence.NamedQueue[Customer], expectedSize int32) {
	size, err := namedQueue.Size(context.Background())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(expectedSize))
}

func getQueue[V any](g *gomega.WithT, session *coherence.Session, queueName string, queueType coherence.NamedQueueType) coherence.NamedQueue[V] {
	var (
		err   error
		queue coherence.NamedQueue[V]
		ctx   = context.Background()
	)
	if queueType == coherence.Dequeue {
		queue, err = coherence.GetNamedDeQueue[V](ctx, session, queueName)
	} else {
		queue, err = coherence.GetNamedQueue[V](ctx, session, queueName, queueType)
	}
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	return queue
}
