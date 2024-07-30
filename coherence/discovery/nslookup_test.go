/*
 * Copyright (c) 2021, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package discovery

import (
	"github.com/onsi/gomega"
	"testing"
)

var defaultTimeout int32 = 30

func TestInvalidHostIp(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	_, err := Open("host:123:123", defaultTimeout)
	g.Expect(err).To(gomega.Not(gomega.BeNil()))

	_, err = Open("host:1233f", defaultTimeout)
	g.Expect(err).To(gomega.Not(gomega.BeNil()))

	_, err = Open("host:-1", defaultTimeout)
	g.Expect(err).To(gomega.Not(gomega.BeNil()))

	_, err = Open("host:1023", defaultTimeout)
	g.Expect(err).To(gomega.Not(gomega.BeNil()))

	_, err = Open("host:65536", defaultTimeout)
	g.Expect(err).To(gomega.Not(gomega.BeNil()))
}

func TestParseResults(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	g.Expect(len(parseResults(""))).To(gomega.Equal(0))
	g.Expect(len(parseResults("[123]"))).To(gomega.Equal(1))
	g.Expect(len(parseResults("[123, 123]"))).To(gomega.Equal(2))
	g.Expect(len(parseResults("[123, 123, 456]"))).To(gomega.Equal(3))
	result := parseResults("[A, BB, CCC]")
	g.Expect(len(result)).To(gomega.Equal(3))
	for _, v := range result {
		valid := v == "A" || v == "BB" || v == "CCC"
		g.Expect(valid).To(gomega.BeTrue())
	}
}
