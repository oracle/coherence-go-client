/*
 * Copyright (c) 2021, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package discovery

import (
	. "github.com/onsi/gomega"
	"testing"
)

var defaultTimeout int32 = 30

func TestInvalidHostIp(t *testing.T) {
	g := NewGomegaWithT(t)

	_, err := Open("host:123:123", defaultTimeout)
	g.Expect(err).To(Not(BeNil()))

	_, err = Open("host:1233f", defaultTimeout)
	g.Expect(err).To(Not(BeNil()))

	_, err = Open("host:-1", defaultTimeout)
	g.Expect(err).To(Not(BeNil()))

	_, err = Open("host:1023", defaultTimeout)
	g.Expect(err).To(Not(BeNil()))

	_, err = Open("host:65536", defaultTimeout)
	g.Expect(err).To(Not(BeNil()))
}

func TestParseResults(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(len(parseResults(""))).To(Equal(0))
	g.Expect(len(parseResults("[123]"))).To(Equal(1))
	g.Expect(len(parseResults("[123, 123]"))).To(Equal(2))
	g.Expect(len(parseResults("[123, 123, 456]"))).To(Equal(3))
	result := parseResults("[A, BB, CCC]")
	g.Expect(len(result)).To(Equal(3))
	for _, v := range result {
		valid := v == "A" || v == "BB" || v == "CCC"
		g.Expect(valid).To(BeTrue())
	}
}
