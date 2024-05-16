/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package common

import "time"

const (
	QueueNameOrders    = "orders-queue"
	QueueNameProcessed = "processed-queue"
)

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
