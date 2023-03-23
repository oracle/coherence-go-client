/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"github.com/onsi/gomega"
	"testing"
)

func TestEventEmitter(t *testing.T) {
	g := gomega.NewWithT(t)
	var value = ""

	callback := func(a string) {
		value = a
	}

	emitter := newEventEmitter[string, string]() // string label and string event
	emitter.on("a", callback)
	emitter.emit("a", "event")
	g.Expect(value).Should(gomega.Equal("event"))
	emitter.emit("a", "event2")
	g.Expect(value).Should(gomega.Equal("event2"))
}
