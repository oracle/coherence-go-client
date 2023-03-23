/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for insert events on a NamedMap or NamedCache.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/processors"
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
	listener := NewInsertEventsListener[int, Person]()
	if err = namedMap.AddListener(ctx, listener.listener); err != nil {
		panic(err)
	}

	defer func(ctx context.Context, namedMap coherence.NamedMap[int, Person], listener *InsertEventsListener[int, Person]) {
		if err := namedMap.RemoveListener(ctx, listener.listener); err != nil {
			panic(err)
		}
	}(ctx, namedMap, listener)

	newPerson := Person{ID: 1, Name: "Tim", Age: 53}
	fmt.Println("Add new Person", newPerson)
	if _, err = namedMap.Put(ctx, newPerson.ID, newPerson); err != nil {
		panic(err)
	}

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size)

	fmt.Println("Update person age using processor")
	// Update the age
	_, err = coherence.Invoke[int, Person, bool](ctx, namedMap, 1, processors.Update("age", 56))
	if err != nil {
		panic(err)
	}

	_, err = namedMap.Remove(ctx, 1)
	if err != nil {
		panic(err)
	}

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size)

	if err = namedMap.Truncate(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Cache truncated")
}

type InsertEventsListener[K comparable, V any] struct {
	listener coherence.MapListener[K, V]
}

func NewInsertEventsListener[K comparable, V any]() *InsertEventsListener[K, V] {
	exampleListener := InsertEventsListener[K, V]{
		listener: coherence.NewMapListener[K, V](),
	}

	exampleListener.listener.OnInserted(func(e coherence.MapEvent[K, V]) {
		key, err := e.Key()
		if err != nil {
			panic(err)
		}
		newValue, err := e.NewValue()
		if err != nil {
			panic(err)
		}

		fmt.Printf("**EVENT=Inserted: key=%v, value=%v\n", *key, newValue)
	})

	return &exampleListener
}
