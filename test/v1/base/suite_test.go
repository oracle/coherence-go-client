/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package base

import (
	"github.com/oracle/coherence-go-client/test/utils"
	"testing"
)

// The entry point for the test suite
func TestMain(m *testing.M) {
	utils.RunTest(m, 1408, 30000, 8080, false)
}
