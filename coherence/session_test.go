/*
* Copyright (c) 2022, 2023 Oracle and/or its affiliates.
* Licensed under the Universal Permissive License v 1.0 as shown at
* https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"github.com/onsi/gomega"
	"strconv"
	"testing"
	"time"
)

func TestSessionValidation(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		ctx = context.Background()
	)

	_, err = NewSession(ctx, WithFormat("not-json"))
	g.Expect(err).To(gomega.Equal(ErrInvalidFormat))

	// test default timeout
	timeout, _ := strconv.ParseInt(defaultSessionTimeout, 10, 64)
	s, err := NewSession(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.Timeout).To(gomega.Equal(time.Duration(timeout) * time.Millisecond))

	// test setting a timeout
	s, err = NewSession(ctx, WithSessionTimeout(time.Duration(33)*time.Millisecond))
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.Timeout).To(gomega.Equal(time.Duration(33) * time.Millisecond))
}

func TestSessionEnvDebug(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		ctx = context.Background()
	)
	t.Setenv(envSessionDebug, "true")
	_, err := NewSession(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
}

func TestConnectionOverride(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		ctx = context.Background()
	)
	t.Setenv(envHostName, "localhost:12345")
	s, err := NewSession(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.Address).To(gomega.Equal("localhost:12345"))
}

func TestInvalidFormat(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		ctx = context.Background()
	)
	_, err := NewSession(ctx, WithFormat("abc"))
	g.Expect(err).To(gomega.HaveOccurred())
}

func TestSessionTimeout(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		ctx = context.Background()
	)

	t.Setenv(envSessionTimeout, "-1")
	_, err := NewSession(ctx)
	g.Expect(err).To(gomega.HaveOccurred())
}
