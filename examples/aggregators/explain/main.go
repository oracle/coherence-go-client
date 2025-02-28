/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to run an explain plan using a NamedMap with key of int and value of Person struct.
*/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/aggregators"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
)

type Person struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Age        int    `json:"age"`
	Department string `json:"department"`
}

func main() {
	var (
		personData = map[int]Person{
			1: {ID: 1, Name: "Tim", Age: 19, Department: "IT"},
			2: {ID: 2, Name: "Andrew", Age: 20, Department: "IT"},
			3: {ID: 3, Name: "John", Age: 30, Department: "HR"},
			4: {ID: 4, Name: "Steve", Age: 40, Department: "Finance"},
		}
		ctx = context.Background()
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())

	if err != nil {
		panic(err)
	}

	defer session.Close()

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.GetNamedMap[int, Person](session, "explain-test")
	if err != nil {
		panic(err)
	}

	// Add an index on department
	department := extractors.Extract[string]("department")

	err = coherence.AddIndex(ctx, namedMap, department, true)
	if err != nil {
		panic(err)
	}

	// clear and populate the Map
	_ = namedMap.Clear(ctx)

	if err = namedMap.PutAll(ctx, personData); err != nil {
		panic(err)
	}

	jsonResult, err := coherence.AggregateFilter[int, Person, map[string]interface{}](ctx, namedMap,
		filters.Equal(department, "IT"),
		aggregators.QueryRecorder(aggregators.Explain))
	if err != nil {
		panic(err)
	}

	var results string

	if results, err = getResults(jsonResult); err != nil {
		panic(err)
	}

	fmt.Println(aggregators.Explain)
	fmt.Println(results)

	jsonResult, err = coherence.AggregateFilter[int, Person, map[string]interface{}](ctx, namedMap,
		filters.Equal(department, "IT"),
		aggregators.QueryRecorder(aggregators.Trace))
	if err != nil {
		panic(err)
	}

	if results, err = getResults(jsonResult); err != nil {
		panic(err)
	}
	fmt.Println(aggregators.Trace)
	fmt.Println(results)
}

func getResults(results *map[string]interface{}) (string, error) {
	prettyJSON, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", fmt.Errorf("error marshalling: %v", err)
	}

	return string(prettyJSON), nil
}
