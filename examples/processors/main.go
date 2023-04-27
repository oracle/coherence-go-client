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
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
)

type Person struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Retired bool   `json:"retired"`
}

func (p Person) String() string {
	return fmt.Sprintf("Person{id=%d, name=%s, age=%d, retired=%v}", p.ID, p.Name, p.Age, p.Retired)
}

func main() {
	var (
		person1 = Person{ID: 1, Name: "Tim", Age: 54}
		person2 = Person{ID: 2, Name: "Helen", Age: 44}
		person  *Person
		ctx     = context.Background()
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

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
	if _, err = coherence.Invoke[int, Person, bool](ctx, namedMap, 1, processors.Update("age", 68)); err != nil {
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

	fmt.Println("Retiring all people over 67, just an example ;)")
	// invoke an entry processor over all people, this returns the keys that have been updated
	ch := coherence.InvokeAll[int, Person, int](ctx, namedMap, processors.Increment("age", 1))
	for se := range ch {
		if se.Err != nil {
			panic(se.Err)
		}
		fmt.Println("Updated person with key", se.Value)
	}

	// invoke an entry process over all people older than 67 and set then as retired
	age := extractors.Extract[int]("age")
	ch2 := coherence.InvokeAllFilter[int, Person, int](ctx, namedMap, filters.Greater(age, 67),
		processors.Update("retired", true))
	for se := range ch2 {
		if se.Err != nil {
			panic(se.Err)
		}
		fmt.Println("Retired person with key", se.Value)
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
