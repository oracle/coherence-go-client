/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for all lifecycle events on a NamedMap or NamedCache.
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
	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	// Create a listener and add to the cache
	listener := coherence.NewMapLifecycleListener[int, Person]().OnAny(func(e coherence.MapLifecycleEvent[int, Person]) {
		fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
	})

	namedMap.AddLifecycleListener(listener)

	defer namedMap.RemoveLifecycleListener(listener)

	newPerson := Person{ID: 1, Name: "Tim", Age: 21}
	fmt.Println("Add new Person", newPerson)
	if _, err = namedMap.Put(ctx, newPerson.ID, newPerson); err != nil {
		panic(err)
	}

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Cache size is", size, ", truncate cache")
	if err = namedMap.Truncate(ctx); err != nil {
		panic(err)
	}

	sleep("Sleeping to ensure we see event")

	fmt.Println("Cache size is", size, "destroy cache")
	if err = namedMap.Destroy(ctx); err != nil {
		panic(err)
	}

	sleep("Sleeping to ensure we see events")
}

func sleep(msg string) {
	fmt.Println(msg)
	time.Sleep(time.Duration(5) * time.Second)
}
