/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to run queries against a NamedMap or NamedCache using filters.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
)

type Person struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
	City string `json:"city"`
}

func main() {

	var (
		ctx    = context.Background()
		cities = []string{"Perth", "Adelaide", "Sydney", "Melbourne"}
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	if err = namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Adding 20 random people")
	for i := 1; i <= 20; i++ {
		p := Person{ID: i, Name: fmt.Sprintf("Person-%d", i), Age: 15 + i, City: cities[i%4]}
		_, err = namedMap.Put(ctx, p.ID, p)
		if err != nil {
			panic(err)
		}
	}

	size, _ := namedMap.Size(ctx)
	fmt.Println("Cache size is", size)

	age := extractors.Extract[int]("age")
	city := extractors.Extract[string]("city")

	fmt.Println("Retrieve the people between the age of 17 and 21")
	ch := namedMap.EntrySetFilter(ctx, filters.Between(age, 17, 21))
	for result := range ch {
		if result.Err != nil {
			panic(err)
		}

		fmt.Println("Key:", result.Key, "Value:", result.Value)
	}

	fmt.Println("Retrieve the people between the age of 17 and 30 and who live in Perth")
	ch = namedMap.EntrySetFilter(ctx, filters.Between(age, 17, 30).And(filters.Equal(city, "Perth")))
	for result := range ch {
		if result.Err != nil {
			panic(err)
		}

		fmt.Println("Key:", result.Key, "Value:", result.Value)
	}

	fmt.Println("Retrieve the people between the age of 17 and 25 and who live in Perth or Melbourne")
	ch = namedMap.EntrySetFilter(ctx, filters.Between(age, 17, 25).And(filters.In(city, []string{"Perth", "Melbourne"})))
	for result := range ch {
		if result.Err != nil {
			panic(err)
		}

		fmt.Println("Key:", result.Key, "Value:", result.Value)
	}
}
