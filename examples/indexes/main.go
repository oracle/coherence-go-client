/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to add indexes against a NamedMap or NamedCache to support queries and aggregations.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/aggregators"
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"time"
)

type Person struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
	City string `json:"city"`
}

func (p Person) String() string {
	return fmt.Sprintf("Person{id=%d, name=%s, age=%d, city=%s}", p.ID, p.Name, p.Age, p.City)
}

func main() {
	var (
		ctx         = context.Background()
		cities      = []string{"Perth", "Adelaide", "Sydney", "Melbourne"}
		insertCount = 50_000
		count       *int64
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

	fmt.Printf("Adding %d random people using PutAll()\n", insertCount)

	batchSize := 5000
	buffer := make(map[int]Person)

	for i := 1; i <= insertCount; i++ {
		p := Person{ID: i, Name: fmt.Sprintf("Person-%d", i), Age: 15 + i%40, City: cities[i%4]}
		buffer[p.ID] = p
		if i%batchSize == 0 {
			if err = namedMap.PutAll(ctx, buffer); err != nil {
				panic(err)
			}
			buffer = make(map[int]Person)
		}
	}
	// write any left in the buffer
	if len(buffer) > 0 {
		if err = namedMap.PutAll(ctx, buffer); err != nil {
			panic(err)
		}
	}

	size, _ := namedMap.Size(ctx)
	fmt.Println("Cache size is", size)

	age := extractors.Extract[int]("age")

	fmt.Println("Retrieve the count of people between the age of 20 and 40 without indexes")
	start := time.Now()
	count, err = coherence.AggregateFilter(ctx, namedMap, filters.Between(age, 20, 40), aggregators.Count())
	duration := time.Since(start)

	if err != nil {
		panic(err)
	}
	fmt.Printf("Count = %v, duration without index: %v. Adding index and waiting 10 seconds...\n", *count, duration)

	err = coherence.AddIndex(ctx, namedMap, age, true)
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Duration(10) * time.Second)
	start = time.Now()
	count, err = coherence.AggregateFilter(ctx, namedMap, filters.Between(age, 20, 40), aggregators.Count())
	duration = time.Since(start)

	if err != nil {
		panic(err)
	}
	fmt.Printf("Count = %v, duration WITH index:    %v\n", *count, duration)

	err = coherence.RemoveIndex(ctx, namedMap, age)
	if err != nil {
		panic(err)
	}
}
