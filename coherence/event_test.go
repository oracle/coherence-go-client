/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"testing"
)

func TestEventEmitter(t *testing.T) {
	var value string

	callback := func(a string) {
		value = a
	}

	emitter := newEventEmitter[string, string]()
	emitter.on("a", callback)

	emitter.emit("a", "event")
	if value != "event" {
		t.Fatalf("expected value to be 'event', got '%s'", value)
	}

	emitter.emit("a", "event2")
	if value != "event2" {
		t.Fatalf("expected value to be 'event2', got '%s'", value)
	}
}
