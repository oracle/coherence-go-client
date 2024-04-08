/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to use a blocking queue. This program simulates submitting orders to the "orders-queue".
*/
package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/oracle/coherence-go-client/coherence"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const queueNameOrders = "orders-queue"

// Order represents a fictitious order.
type Order struct {
	OrderID             string        `json:"orderID"`
	Customer            string        `json:"customer"`
	OrderStatus         string        `json:"orderStatus"`
	OrderTotal          float32       `json:"orderTotal"`
	CreateTime          time.Time     `json:"createTime"`
	CompleteTime        time.Time     `json:"completeTime"`
	OrderProcessingTime time.Duration `json:"orderProcessingTime"`
}

func main() {
	var (
		ctx       = context.Background()
		numOrders int
		err       error
	)

	// check arguments
	if len(os.Args) != 2 {
		log.Println("Please provide the number of orders to create")
		return
	}

	if numOrders, err = strconv.Atoi(os.Args[1]); err != nil {
		log.Println("Invalid value for number of orders")
		return
	}

	if numOrders <= 0 {
		log.Println("Enter a positive number")
		return
	}

	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	orderQueue, err := coherence.GetNamedQueue[Order](ctx, session, queueNameOrders)
	if err != nil {
		panic(err)
	}

	defer orderQueue.Close()

	for i := 1; i <= numOrders; i++ {
		newOrder := Order{
			OrderID:     uuid.New().String(),
			Customer:    fmt.Sprintf("Customer %d", i),
			OrderStatus: "NEW",
			OrderTotal:  rand.Float32() * 1000, //nolint
			CreateTime:  time.Now(),
		}
		err = orderQueue.Offer(newOrder)

		if i%1000 == 0 {
			log.Printf("submitted %d orders so far", i)
		}

		if err != nil {
			panic(err)
		}
	}

	log.Printf("Submitted %d orders", numOrders)
}
