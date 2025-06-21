/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestErrorLogLevel(t *testing.T) {
	runLogLevelTest(t, ERROR, ERROR, true)
	runLogLevelTest(t, WARNING, ERROR, false)
	runLogLevelTest(t, INFO, ERROR, false)
	runLogLevelTest(t, DEBUG, ERROR, false)
	runLogLevelTest(t, ALL, ERROR, false)

	runLogLevelTest(t, ERROR, WARNING, true)
	runLogLevelTest(t, WARNING, WARNING, true)
	runLogLevelTest(t, INFO, WARNING, false)
	runLogLevelTest(t, DEBUG, WARNING, false)
	runLogLevelTest(t, ALL, WARNING, false)

	runLogLevelTest(t, ERROR, INFO, true)
	runLogLevelTest(t, WARNING, INFO, true)
	runLogLevelTest(t, INFO, INFO, true)
	runLogLevelTest(t, DEBUG, INFO, false)
	runLogLevelTest(t, ALL, INFO, false)

	runLogLevelTest(t, ERROR, DEBUG, true)
	runLogLevelTest(t, WARNING, DEBUG, true)
	runLogLevelTest(t, INFO, DEBUG, true)
	runLogLevelTest(t, DEBUG, DEBUG, true)
	runLogLevelTest(t, ALL, DEBUG, false)

	runLogLevelTest(t, ERROR, ALL, true)
	runLogLevelTest(t, WARNING, ALL, true)
	runLogLevelTest(t, INFO, ALL, true)
	runLogLevelTest(t, DEBUG, ALL, true)
	runLogLevelTest(t, ALL, ALL, true)
}

func runLogLevelTest(t *testing.T, messageLevel, testLogLevel logLevel, expectOutput bool) {
	const message = "MESSAGE"
	var buf bytes.Buffer

	origOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(origOutput)

	setLogLevel(testLogLevel.String())
	logMessage(messageLevel, message)
	output := buf.String()

	if expectOutput && !strings.Contains(output, message) {
		t.Fatalf("expected output to contain %q but it didn't", message)
	}
	if !expectOutput && strings.Contains(output, message) {
		t.Fatalf("expected output to NOT contain %q but it did", message)
	}
}
