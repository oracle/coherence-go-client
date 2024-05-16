/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use a blocking queue. This program simulates order processing by issuing
a blocking Poll() on the "orders-queue" and on receive processes the order and places on the "processed-queue".
*/
package main

import (
	"context"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/examples/queues/blocking/common"
	"log"
	"time"
)

func main() {
	var (
		ctx        = context.Background()
		order      *common.Order
		err        error
		processed  int
		processing bool
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	blockingQueue, err := coherence.GetBlockingNamedQueue[common.Order](ctx, session, common.QueueNameOrders)
	if err != nil {
		panic(err)
	}

	processedQueue, err := coherence.GetNamedQueue[common.Order](ctx, session, common.QueueNameProcessed)
	if err != nil {
		panic(err)
	}

	defer blockingQueue.Close()
	defer processedQueue.Close()

	log.Println("Waiting for messages to process")
	for {
		order, err = blockingQueue.Poll(time.Duration(10) * time.Second)
		if err == coherence.ErrQueueTimedOut {
			log.Println("Waiting for messages to process")
			processing = false
			continue
		}
		if !processing {
			log.Println("Processing orders...")
			processing = true
		}

		if err != nil {
			panic(err)
		}

		// simulate processing and set the processing time ...
		start := time.Now()
		time.Sleep(time.Duration(10) * time.Millisecond)
		order.CompleteTime = time.Now()
		order.OrderProcessingTime = time.Since(start)
		order.OrderStatus = "COMPLETE"

		// publish to the processedQueue

		err = processedQueue.Offer(*order)
		if err != nil {
			panic(err)
		}

		processed++
		if processed%1000 == 0 {
			log.Printf("Processed %d orders", processed)
		}
	}
}
