/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use a blocking queue.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"log"
	"time"
)

func main() {
	var (
		value *string
		ctx   = context.Background()
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session1, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session1.Close()

	requestQueue, err := coherence.GetBlockingNamedQueue[string](ctx, session1, "my-request-queue")
	if err != nil {
		panic(err)
	}

	responseQueue, err := coherence.GetNamedQueue[string](ctx, session1, "my-response-queue")
	if err != nil {
		panic(err)
	}

	defer requestQueue.Close()
	defer responseQueue.Close()

	log.Println("issuing Poll() with 30 second timeout")
	for {
		value, err = requestQueue.Poll(time.Duration(30) * time.Second)
		if err == coherence.ErrQueueTimedOut {
			log.Println("timed out, try again")
			continue
		}
		if err != nil {
			panic(err)
		}

		log.Printf("next value is %s, posting response\n", *value)

		// post on response queue
		err = responseQueue.Offer(fmt.Sprintf("%s - reponse", *value))
		if err != nil {
			panic(err)
		}
	}

}
