/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use a blocking queue. This program subscribes to the "processed-queue" and
does a Poll() for messages and displays the average and last processing time.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/examples/queues/blocking/common"
	"log"
	"time"
)

func main() {
	var (
		ctx                 = context.Background()
		order               *common.Order
		err                 error
		received            int64
		totalProcessingTime time.Duration
		started             bool
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	blockingQueue, err := coherence.GetBlockingNamedQueue[common.Order](ctx, session, common.QueueNameProcessed)
	if err != nil {
		panic(err)
	}

	defer blockingQueue.Close()

	log.Println("Waiting for completed orders")
	start := time.Now()
	for {
		order, err = blockingQueue.Poll(time.Duration(5) * time.Second)
		if err == coherence.ErrQueueTimedOut {
			continue
		}
		if err != nil {
			panic(err)
		}
		if !started {
			start = time.Now()
			started = true
		}

		totalProcessingTime += order.OrderProcessingTime
		received++
		averageProcessingTime := totalProcessingTime.Nanoseconds() / received
		if received%100 == 0 {
			fmt.Printf("\033G[Receieved: %-10d  Total time: %v Average processing time: %v\r", received,
				time.Since(start), time.Duration(averageProcessingTime)*time.Nanosecond)
		}
	}
}
