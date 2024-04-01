/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use queues.
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

	const iterations = 100_000

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	namedQueue, err := coherence.GetNamedQueue[string](ctx, session, "my-queue")
	if err != nil {
		panic(err)
	}

	for i := 1; i <= iterations; i++ {
		v := fmt.Sprintf("value-%v", i)
		err = namedQueue.Offer(v)
		if err != nil {
			panic(err)
		}
		if i%1000 == 0 {
			log.Println("Offer()", i)
		}
	}

	for i := 1; i <= iterations; i++ {
		_, err = namedQueue.Poll()
		if err != nil {
			panic(err)
		}
		if i%1000 == 0 {
			log.Println("Poll() iteration", i)
		}
	}

	// try to read again should get nil
	value, err = namedQueue.Poll()
	if err != nil {
		panic(err)
	}
	log.Println("last value is", value)
}
