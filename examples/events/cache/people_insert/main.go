/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main inserts entries into a people NamedMap insert PutAll().
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"log"
	"os"
	"strconv"
	"time"
)

type Person struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Age        int    `json:"age"`
	InsertTime int64  `json:"insertTime"`
}

func (p Person) String() string {
	return fmt.Sprintf("Person{id=%d, name=%s, age=%d, insertTime=%v}", p.ID, p.Name, p.Age, p.InsertTime)
}

func main() {
	var (
		ctx        = context.Background()
		err        error
		startID    int
		iterations int
		size       int
	)

	// check arguments
	if len(os.Args) != 3 {
		log.Println("please provide a starting ID and number to insert")
		return
	}

	if startID, err = strconv.Atoi(os.Args[1]); err != nil {
		panic("invalid value for starting ID")
	}
	if iterations, err = strconv.Atoi(os.Args[2]); err != nil {
		panic("invalid value for iterations")
	}

	fmt.Println("Start ID=", startID, ",Iterations=", iterations)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	log.Printf("%v\n", session)

	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	if iterations == 0 {
		fmt.Println("Clearing cache")
		if err = namedMap.Clear(ctx); err != nil {
			panic(err)
		}
		fmt.Println("cache cleared")
		return
	}

	if iterations < 0 {
		panic("iterations cannot be less that zero")
	}

	log.Println("Adding", iterations, "people starting at", startID)
	batchSize := 1000
	buffer := make(map[int]Person)

	for i := startID; i < startID+iterations; i++ {
		p := Person{ID: i, Name: fmt.Sprintf("Name-%d", i), Age: 15 + i, InsertTime: time.Now().UnixNano()}
		buffer[p.ID] = p
		if i%batchSize == 0 {
			log.Println("Writing batch of", batchSize)
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

	if size, err = namedMap.Size(ctx); err != nil {
		panic(err)
	}
	log.Println("Size is", size)
}
