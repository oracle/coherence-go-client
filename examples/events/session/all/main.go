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

func (p Person) String() string {
	return fmt.Sprintf("Person{id=%d, name=%s, age=%d}", p.ID, p.Name, p.Age)
}

func main() {
	ctx := context.Background()

	// create a new Session
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	// Create a listener to listen for session events
	listener := coherence.NewSessionLifecycleListener().
		OnAny(func(e coherence.SessionLifecycleEvent) {
			fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
		})

	session.AddSessionLifecycleListener(listener)
	defer session.RemoveSessionLifecycleListener(listener)

	session.Close()

	sleep("Sleeping to ensure we see events")
}

func sleep(msg string) {
	fmt.Println(msg)
	time.Sleep(time.Duration(5) * time.Second)
}
