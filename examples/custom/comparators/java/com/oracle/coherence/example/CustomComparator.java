/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package com.oracle.coherence.example;


import com.oracle.coherence.common.base.Logger;
import com.oracle.coherence.io.json.JsonObject;

import com.tangosol.io.ExternalizableLite;

import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.ValueExtractor;

import com.tangosol.util.function.Remote;

import jakarta.json.bind.annotation.JsonbProperty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Comparator;


/**
 * An example {@link Comparator} that does custom sorting.
 */
public class CustomComparator implements
        Remote.Comparator<JsonObject>, Comparator<JsonObject>, ExternalizableLite {

    /**
     * Default constructor (for PortableObject).
     */
    public CustomComparator() {
        Logger.warn("CustomDateComparator constructor");
    }

    public CustomComparator(ValueExtractor<JsonObject, String> extractor) {
        m_extractor = extractor;
    }

    @Override
    public int compare(JsonObject json1, JsonObject json2) {
        String s1 = m_extractor.extract(json1);
        String s2 = m_extractor.extract(json2);
        // implement your own comparison operation here, this example assumes the values are Strings
        return s1.compareTo(s2);
    }

    // ---- ExternalizableLite implementation -------------------------------

    @Override
    public void readExternal(DataInput in) throws IOException {
        m_extractor = ExternalizableHelper.readObject(in);
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
        ExternalizableHelper.writeObject(out, m_extractor);
    }

    /**
     * <tt>ValueExtractor</tt> to extract value(s) to be used in comparison
     */
    @JsonbProperty("extractor")
    private ValueExtractor<JsonObject, String> m_extractor;
}
