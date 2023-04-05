/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for delete events on a NamedMap or NamedCache.
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

func (p Person) String() string {
	return fmt.Sprintf("Person{id=%d, name=%s, age=%d}", p.ID, p.Name, p.Age)
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
	namedCache, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	// clear the Map
	if err = namedCache.Clear(ctx); err != nil {
		panic(err)
	}

	// Create a listener and add to the cache
	listener := NewInsertEventsListener[int, Person]()
	if err = namedCache.AddListener(ctx, listener.listener); err != nil {
		panic(err)
	}

	newPerson := Person{ID: 1, Name: "Tim", Age: 53}
	fmt.Println("Add new Person", newPerson)
	if _, err = namedCache.Put(ctx, newPerson.ID, newPerson); err != nil {
		panic(err)
	}

	if size, err = namedCache.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size)

	fmt.Println("Update person age using processor")
	// Update the age
	_, err = coherence.Invoke[int, Person, bool](ctx, namedCache, 1, processors.Update("age", 56))
	if err != nil {
		panic(err)
	}

	_, err = namedCache.Remove(ctx, 1)
	if err != nil {
		panic(err)
	}

	if size, err = namedCache.Size(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Cache size is", size)
}

type InsertEventsListener[K comparable, V any] struct {
	listener coherence.MapListener[K, V]
}

func NewInsertEventsListener[K comparable, V any]() *InsertEventsListener[K, V] {
	exampleListener := InsertEventsListener[K, V]{
		listener: coherence.NewMapListener[K, V](),
	}

	exampleListener.listener.OnDeleted(func(e coherence.MapEvent[K, V]) {
		key, err := e.Key()
		if err != nil {
			panic("unable to deserialize key")
		}
		oldValue, err := e.OldValue()
		if err != nil {
			panic("unable to deserialize old value")
		}

		fmt.Printf("**EVENT=Deleted: key=%v, value=%v\n", *key, oldValue)
	})

	return &exampleListener
}
