/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for released events on a NamedMap or NamedCache.
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
	var (
		size int
		ctx  = context.Background()
	)

	// create a new Session
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}

	defer session.Close()

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	// Create a listener and add to the cache
	listener := NewReleaseEventsListener[int, Person]()

	namedMap.AddLifecycleListener(listener.listener)
	defer namedMap.RemoveLifecycleListener(listener.listener)

	newPerson := Person{ID: 1, Name: "Tim", Age: 53}
	fmt.Println("Add new Person", newPerson)
	if _, err = namedMap.Put(ctx, newPerson.ID, newPerson); err != nil {
		panic(err)
	}

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size, "releasing cache")

	namedMap.Release()
	sleep("sleep")

	// try an operation and you will get error as the resources are released
	_, err = namedMap.Size(ctx)
	if err != coherence.ErrReleased {
		panic(err)
	}

	fmt.Println("Done")
}

func sleep(msg string) {
	fmt.Println(msg)
	time.Sleep(time.Duration(5) * time.Second)
}

type ReleaseEventsListener[K comparable, V any] struct {
	listener coherence.MapLifecycleListener[K, V]
}

func NewReleaseEventsListener[K comparable, V any]() *ReleaseEventsListener[K, V] {
	exampleListener := ReleaseEventsListener[K, V]{
		listener: coherence.NewMapLifecycleListener[K, V](),
	}

	exampleListener.listener.OnReleased(func(e coherence.MapLifecycleEvent[K, V]) {
		fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
	})

	return &exampleListener
}
