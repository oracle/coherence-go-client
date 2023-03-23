/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to run processors against a NamedMap or NamedCache.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
)

func main() {
	type Person struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	var (
		person1 = Person{ID: 1, Name: "Tim", Age: 53}
		person2 = Person{ID: 2, Name: "Helen", Age: 44}
		person  *Person
		ctx     = context.Background()
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	defer session.Close()

	if err != nil {
		panic(err)
	}

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.NewNamedMap[int, Person](session, "processor-test")
	if err != nil {
		panic(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Adding person", person1)
	// put a new entry
	if _, err = namedMap.Put(ctx, person1.ID, person1); err != nil {
		panic(err)
	}

	// get the Person
	if person, err = namedMap.Get(ctx, 1); err != nil {
		panic(err)
	}
	fmt.Println("Person is", *person)

	// update the age to 54
	if _, err = coherence.Invoke[int, Person, bool](ctx, namedMap, 1, processors.Update("age", 54)); err != nil {
		panic(err)
	}

	// get the Person
	person, err = namedMap.Get(ctx, 1)
	if err != nil {
		panic(err)
	}
	fmt.Println("Person age is now", person.Age)

	fmt.Println("Adding person", person2)
	// put a new entry
	if _, err = namedMap.Put(ctx, person2.ID, person2); err != nil {
		panic(err)
	}

	// invoke an entry processor over all people, this returns the keys that have been updated
	ch := coherence.InvokeAll[int, Person, int](ctx, namedMap, processors.Increment("age", 1))
	for se := range ch {
		if se.Err != nil {
			panic(se.Err)
		}
		fmt.Println("Updated key", se.Value)
	}

	fmt.Println("Displaying all people")
	for se := range namedMap.ValuesFilter(ctx, filters.Always()) {
		if se.Err != nil {
			panic(err)
		}
		if se.Err == nil {
			fmt.Println("Person is", se.Value)
		}
	}
}
