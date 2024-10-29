/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package com.oracle.coherence.go.testing;

import com.tangosol.util.Base;
import com.tangosol.util.InvocableMap;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * An entry process that takes a long time to run to test timeouts.
 */
public class LongEntryProcessor extends AbstractProcessor<String, String, Void> {
    public LongEntryProcessor() {
    }
    @Override
    public Void process(InvocableMap.Entry<String, String> entry) {
        System.out.println("LongEntryProcessor: Sleeping for 30 seconds");
        Base.sleep(30_000L);
        return null;
    }
}
