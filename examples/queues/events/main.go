/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for events on queues.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"time"
)

func main() {
	var ctx = context.Background()

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	namedQueue, err := coherence.GetNamedQueue[string](ctx, session, "queue-events", coherence.Queue)
	if err != nil {
		panic(err)
	}

	// Create a listener to monitor for Released events
	listener := coherence.NewQueueLifecycleListener[string]().
		OnReleased(func(e coherence.QueueLifecycleEvent[string]) {
			fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
		})

	err = namedQueue.AddLifecycleListener(listener)
	if err != nil {
		panic(err)
	}

	namedQueue.Release()

	time.Sleep(5 * time.Second)
}
