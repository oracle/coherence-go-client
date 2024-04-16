/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package com.oracle.coherence.go.queues;

import com.oracle.coherence.go.testing.Customer;
import com.tangosol.net.Coherence;
import com.tangosol.net.NamedQueue;

/**
 * Populate queues.
 */
public class PopulateQueue {
    public PopulateQueue() {}

    public void offerData() {
        Coherence coherence = Coherence.client().start().join();
        NamedQueue<Customer> myQueue = coherence.getSession().getQueue("test-queue");
        for (int i = 1; i <= 1000; i++) {
            Customer customer = new Customer();
            customer.setId(i);
            customer.setCustomerName("Name-" + i);
            customer.setCustomerType(Customer.GOLD);
            myQueue.offer(customer);
        }
    }
}
