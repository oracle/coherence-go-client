/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"github.com/onsi/gomega"
	"testing"
)

func TestResolverParseNsLookupString(t *testing.T) {
	var (
		g       = gomega.NewWithT(t)
		err     error
		results []string
	)

	_, err = parseNsLookupString("")
	g.Expect(err).To(gomega.HaveOccurred())

	_, err = parseNsLookupString("[123123123")
	g.Expect(err).To(gomega.HaveOccurred())

	_, err = parseNsLookupString("123123123]")
	g.Expect(err).To(gomega.HaveOccurred())

	results, err = parseNsLookupString("[]")
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(len(results)).To(gomega.Equal(0))

	results, err = parseNsLookupString("[127.0.0.1, 58193, 127.0.0.1, 58192, 127.0.0.1, 58194]")
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(len(results)).To(gomega.Equal(3))
	g.Expect(results[0]).To(gomega.Equal("127.0.0.1:58193"))
	g.Expect(results[1]).To(gomega.Equal("127.0.0.1:58192"))
	g.Expect(results[2]).To(gomega.Equal("127.0.0.1:58194"))

	results, err = parseNsLookupString("[127.0.0.1, 58193]")
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(len(results)).To(gomega.Equal(1))
	g.Expect(results[0]).To(gomega.Equal("127.0.0.1:58193"))

	results, err = parseNsLookupString("[127.0.0.1, 58193, 127.0.0.1, 58192]")
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(len(results)).To(gomega.Equal(2))
	g.Expect(results[0]).To(gomega.Equal("127.0.0.1:58193"))
	g.Expect(results[1]).To(gomega.Equal("127.0.0.1:58192"))
}
