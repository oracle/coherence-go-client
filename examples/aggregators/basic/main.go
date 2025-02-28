/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows aggregation examples using a NamedMap with key of int and value of Person struct.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/aggregators"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"math/big"
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
		count  *int64
		bigRat *big.Rat
		ctx    = context.Background()
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())

	if err != nil {
		panic(err)
	}

	defer session.Close()

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.GetNamedMap[int, Person](session, "aggregation-test")
	if err != nil {
		panic(err)
	}

	// clear the Map
	_ = namedMap.Clear(ctx)

	if err = namedMap.PutAll(ctx, personData); err != nil {
		panic(err)
	}
	values, err := coherence.Aggregate(ctx, namedMap, aggregators.Distinct(extractors.Extract[string]("department")))
	if err != nil {
		panic(err)
	}
	fmt.Println("Distinct departments", *values)

	if count, err = coherence.Aggregate(ctx, namedMap, aggregators.Count()); err != nil {
		panic(err)
	}
	fmt.Println("Number of people", *count)

	age := extractors.Extract[int]("age")

	// Count with Filter age > 19
	count, err = coherence.AggregateFilter(ctx, namedMap, filters.Greater(age, 19), aggregators.Count())
	if err != nil {
		panic(err)
	}
	fmt.Println("Number of people aged greater than 19 is", *count)

	// Average age of people < 30
	bigRat, err = coherence.AggregateFilter(ctx, namedMap, filters.Less(age, 30), aggregators.Average(age))
	if err != nil {
		panic(err)
	}
	value, _ := bigRat.Float32()
	fmt.Printf("Average age of people less than 30 is %.2f\n", value)

	// return the oldest person in each department
	departmentResult, err := coherence.Aggregate(ctx, namedMap,
		aggregators.GroupBy(extractors.Extract[string]("department"), aggregators.Max(age)))
	if err != nil {
		panic(err)
	}
	for _, v := range departmentResult.Entries {
		fmt.Println("Department", v.Key, "Max age", v.Value)
	}
}
