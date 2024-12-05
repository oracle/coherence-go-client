/*
 * Copyright (c) 2023, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use ContainsKey, ContainsValue and ContainsEntry against a NamedCache.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
)

func main() {
	var (
		ctx      = context.Background()
		contains bool
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// create a new NamedMap with key of int and value of string
	namedMap, err := coherence.GetNamedMap[int, string](session, "my-contains-map")
	if err != nil {
		panic(err)
	}

	fmt.Println("Put key 1, value one")
	// put a new key / value
	if _, err = namedMap.Put(ctx, 1, "one"); err != nil {
		panic(err)
	}

	if contains, err = namedMap.ContainsKey(ctx, 1); err != nil {
		panic(err)
	}
	fmt.Printf("NamedMap ContainsKey 1 should be true and is %v\n", contains)

	if contains, err = namedMap.ContainsValue(ctx, "one"); err != nil {
		panic(err)
	}
	fmt.Printf("NamedMap ContainsValue \"one\" should be true and is %v\n", contains)

	if contains, err = namedMap.ContainsValue(ctx, "two"); err != nil {
		panic(err)
	}
	fmt.Printf("NamedMap ContainsValue \"two\" should be false and is %v\n", contains)

	if contains, err = namedMap.ContainsEntry(ctx, 1, "two"); err != nil {
		panic(err)
	}
	fmt.Printf("NamedMap ContainsEntry key=1, value=\"two\" should be false and is %v\n", contains)
}
