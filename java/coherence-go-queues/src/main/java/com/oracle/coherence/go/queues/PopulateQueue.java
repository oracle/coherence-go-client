/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package com.oracle.coherence.go.queues;

import com.oracle.coherence.go.testing.Customer;
import com.tangosol.net.Coherence;
import com.tangosol.net.NamedQueue;
import com.tangosol.net.Session;
import com.tangosol.coherence.config.scheme.SimpleDequeScheme;

/**
 * Populate queues.
 */
public class PopulateQueue {
    public PopulateQueue() {}

    public void offerData() {
        Coherence            coherence = Coherence.client().start().join();
        Session              session   = coherence.getSession();

        NamedQueue<Customer> myQueue   =SimpleDequeScheme.INSTANCE.realize("test-queue", session);
        for (int i = 1; i <= 1000; i++) {
            Customer customer = new Customer();
            customer.setId(i);
            customer.setCustomerName("Name-" + i);
            customer.setCustomerType(Customer.GOLD);
            myQueue.offer(customer);
        }
    }
}
