/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package main

import (
	"context"
	"fmt"
	"log"

	daprd "github.com/dapr/go-sdk/client"
)

func main() {
	client, err := daprd.NewClient()
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	emptyMetadata := map[string]string{}

	key := "foo"
	val := []byte("bar")

	if err = client.SaveState(ctx, "statestore", key, val, emptyMetadata); err != nil {
		log.Fatalf("save error: %v", err)
	}

	item, err := client.GetState(ctx, "statestore", key, emptyMetadata)
	if err != nil {
		log.Fatalf("get error: %v", err)
	}

	fmt.Printf("Got state: %s = %s\n", key, item.Value)
}
