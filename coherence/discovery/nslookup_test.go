/*
 * Copyright (c) 2021, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package discovery

import (
	"testing"
)

var defaultTimeout int32 = 30

func TestInvalidHostIp(t *testing.T) {
	_, err := Open("host:123:123", defaultTimeout)
	ensureFails(t, err)

	_, err = Open("host:1233f", defaultTimeout)
	ensureFails(t, err)

	_, err = Open("host:-1", defaultTimeout)
	ensureFails(t, err)

	_, err = Open("host:1023", defaultTimeout)
	ensureFails(t, err)

	_, err = Open("host:65536", defaultTimeout)
	ensureFails(t, err)
}

func ensureFails(t *testing.T, err error) {
	if err == nil {
		t.Errorf("err was nil but should have failed")
	}
}

func TestParseResults(t *testing.T) {
	assertEquals(t, len(parseResults("")), 0)
	assertEquals(t, len(parseResults("[123]")), 1)
	assertEquals(t, len(parseResults("[123, 123]")), 2)
	assertEquals(t, len(parseResults("[123, 123, 456]")), 3)

	result := parseResults("[A, BB, CCC]")
	assertEquals(t, len(result), 3)
	for _, v := range result {
		valid := v == "A" || v == "BB" || v == "CCC"
		if !valid {
			t.Errorf("invalid result: %v", v)
		}
	}
}

func assertEquals(t *testing.T, actual, expected int) {
	if actual != expected {
		t.Errorf("actual %d != expected %d", actual, expected)
	}
}
