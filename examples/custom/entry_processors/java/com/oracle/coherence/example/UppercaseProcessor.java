/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package com.oracle.coherence.example;

import com.tangosol.util.InvocableMap;

/**
 * Sample entry processor that ensures updates a {@link Customer}s name to uppercase.
 */
public class UppercaseProcessor implements InvocableMap.EntryProcessor<String, Customer, Void> {
    @Override
    public Void process(InvocableMap.Entry<String, Customer> entry) {
        Customer customer = entry.getValue();
        customer.setName(customer.getName().toUpperCase());
        entry.setValue(customer);
        return null;
    }
}