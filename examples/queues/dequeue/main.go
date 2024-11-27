/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use a dequeue or double ended queue.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"log"
)

func main() {
	var (
		value *string
		ctx   = context.Background()
	)

	const iterations = 10

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	namedQueue, err := coherence.GetNamedDeQueue[string](ctx, session, "double-ended-queue")
	if err != nil {
		panic(err)
	}

	// add1 0 entries to the end (tail) of the queue
	for i := 1; i <= iterations; i++ {
		v := fmt.Sprintf("value-%v", i)
		log.Printf("OfferTail() %s to the queue\n", v)
		err = namedQueue.OfferTail(ctx, v)
		if err != nil {
			panic(err)
		}
	}

	// Offer a value to the head
	err = namedQueue.OfferHead(ctx, "value-head")
	if err != nil {
		panic(err)
	}

	// peek the tail of the queue
	value, err = namedQueue.PeekTail(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("PeekTail() returned: %s\n", *value)

	// poll for iterations +1 because we added another entry to the head
	for i := 1; i <= iterations+1; i++ {
		value, err = namedQueue.PollHead(ctx)
		if err != nil {
			panic(err)
		}
		log.Printf("PollHead() returned: %s\n", *value)
	}

	// try to read again should get nil
	value, err = namedQueue.PollHead(ctx)
	if err != nil {
		panic(err)
	}
	log.Println("last value is", value)
}
