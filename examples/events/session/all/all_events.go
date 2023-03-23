/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for all session events on a NamedMap or NamedCache.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"time"
)

type Person struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	ctx := context.Background()

	// create a new Session
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	// Create a listener to listen for session events
	listener := NewAllLifecycleEventsListener()

	session.AddSessionLifecycleListener(listener.listener)
	defer session.RemoveSessionLifecycleListener(listener.listener)

	session.Close()

	sleep("sleeping to ensure we see events")
}

func sleep(msg string) {
	fmt.Println(msg)
	time.Sleep(time.Duration(5) * time.Second)
}

type AllSessionLifecycleEventsListener struct {
	listener coherence.SessionLifecycleListener
}

func NewAllLifecycleEventsListener() *AllSessionLifecycleEventsListener {
	exampleListener := AllSessionLifecycleEventsListener{
		listener: coherence.NewSessionLifecycleListener(),
	}

	exampleListener.listener.OnAny(func(e coherence.SessionLifecycleEvent) {
		fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
	})

	return &exampleListener
}
