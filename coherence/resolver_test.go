/*
 * Copyright (c) 2024, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"testing"
)

func TestResolverParseNsLookupString(t *testing.T) {
	checkError := func(err error, shouldFail bool, label string) {
		if shouldFail && err == nil {
			t.Fatalf("%s: expected error but got nil", label)
		}
		if !shouldFail && err != nil {
			t.Fatalf("%s: unexpected error: %v", label, err)
		}
	}

	checkResults := func(results []string, expected []string, label string) {
		if len(results) != len(expected) {
			t.Fatalf("%s: expected %d results, got %d", label, len(expected), len(results))
		}
		for i := range expected {
			if results[i] != expected[i] {
				t.Fatalf("%s: expected results[%d] = %s, got %s", label, i, expected[i], results[i])
			}
		}
	}

	_, err := parseNsLookupString("")
	checkError(err, true, "empty input")

	_, err = parseNsLookupString("[123123123")
	checkError(err, true, "missing closing bracket")

	_, err = parseNsLookupString("123123123]")
	checkError(err, true, "missing opening bracket")

	results, err := parseNsLookupString("[]")
	checkError(err, false, "empty array")
	checkResults(results, []string{}, "empty array result")

	results, err = parseNsLookupString("[127.0.0.1, 58193, 127.0.0.1, 58192, 127.0.0.1, 58194]")
	checkError(err, false, "multiple pairs")
	checkResults(results, []string{
		"127.0.0.1:58193",
		"127.0.0.1:58192",
		"127.0.0.1:58194",
	}, "multiple pairs result")

	results, err = parseNsLookupString("[127.0.0.1, 58193]")
	checkError(err, false, "single pair")
	checkResults(results, []string{"127.0.0.1:58193"}, "single pair result")

	results, err = parseNsLookupString("[127.0.0.1, 58193, 127.0.0.1, 58192]")
	checkError(err, false, "two pairs")
	checkResults(results, []string{
		"127.0.0.1:58193",
		"127.0.0.1:58192",
	}, "two pairs result")
}
