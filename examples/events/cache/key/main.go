/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for events on a NamedMap or NamedCache for a specific key.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
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

	// Create a listener and add to the cache for key 1
	listener := coherence.NewMapListener[int, Person]().OnUpdated(func(e coherence.MapEvent[int, Person]) {
		key, err1 := e.Key()
		if err1 != nil {
			panic("unable to deserialize key")
		}

		newValue, err1 := e.NewValue()
		if err1 != nil {
			panic("unable to deserialize new value")
		}

		oldValue, err1 := e.OldValue()
		if err1 != nil {
			panic("unable to deserialize old value")
		}

		fmt.Printf("**EVENT=Updated: key=%v, oldValue=%v, newValue=%v\n", *key, *oldValue, *newValue)
	})
	if err = namedMap.AddKeyListener(ctx, listener, 1); err != nil {
		panic(err)
	}

	defer func(ctx context.Context, namedMap coherence.NamedMap[int, Person], listener coherence.MapListener[int, Person]) {
		if err := namedMap.RemoveListener(ctx, listener); err != nil {
			panic(fmt.Sprintf("cannot remove listener %v", listener))
		}
	}(ctx, namedMap, listener)

	newPerson := Person{ID: 1, Name: "Tim", Age: 21}
	fmt.Println("Add new Person", newPerson)
	if _, err = namedMap.Put(ctx, newPerson.ID, newPerson); err != nil {
		panic(err)
	}

	fmt.Println("Update person age using processor")
	// Update the age
	_, err = coherence.Invoke[int, Person, bool](ctx, namedMap, 1, processors.Update("age", 22))
	if err != nil {
		panic(err)
	}
}
