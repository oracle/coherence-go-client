/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for truncated events on a NamedMap or NamedCache.
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
	listener := NewTruncateEventsListener[int, Person]()

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
	fmt.Println("Cache size is", size, "truncating cache")

	if err = namedMap.Truncate(ctx); err != nil {
		panic(err)
	}

	sleep("sleep")

	fmt.Println("Removing lifecycle listener, we should not see truncate event again")
	namedMap.RemoveLifecycleListener(listener.listener)

	fmt.Println("Truncate cache again")
	if err = namedMap.Truncate(ctx); err != nil {
		panic(err)
	}

	sleep("sleep")
	fmt.Println("Done")
}

func sleep(msg string) {
	fmt.Println(msg)
	time.Sleep(time.Duration(5) * time.Second)
}

type TruncateEventsListener[K comparable, V any] struct {
	listener coherence.MapLifecycleListener[K, V]
}

func NewTruncateEventsListener[K comparable, V any]() *TruncateEventsListener[K, V] {
	exampleListener := TruncateEventsListener[K, V]{
		listener: coherence.NewMapLifecycleListener[K, V](),
	}

	exampleListener.listener.OnTruncated(func(e coherence.MapLifecycleEvent[K, V]) {
		fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
	})

	return &exampleListener
}
