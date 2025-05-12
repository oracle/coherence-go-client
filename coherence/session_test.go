/*
* Copyright (c) 2022, 2025 Oracle and/or its affiliates.
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
	timeout, _ := strconv.ParseInt(defaultRequestTimeout, 10, 64)
	s, err := NewSession(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.RequestTimeout).To(gomega.Equal(time.Duration(timeout) * time.Millisecond))

	// test setting a request timeout
	s, err = NewSession(ctx, WithRequestTimeout(time.Duration(33)*time.Millisecond))
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.RequestTimeout).To(gomega.Equal(time.Duration(33) * time.Millisecond))

	// test setting a disconnected timeout
	s, err = NewSession(ctx, WithDisconnectTimeout(time.Duration(34)*time.Millisecond))
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.DisconnectTimeout).To(gomega.Equal(time.Duration(34) * time.Millisecond))

	// test setting a ready timeout
	s, err = NewSession(ctx, WithReadyTimeout(time.Duration(35)*time.Millisecond))
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.ReadyTimeout).To(gomega.Equal(time.Duration(35) * time.Millisecond))
}

func TestSessionEnvValidation(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		ctx = context.Background()
	)

	// test default timeout
	t.Setenv("COHERENCE_CLIENT_REQUEST_TIMEOUT", "5000")
	s, err := NewSession(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.RequestTimeout).To(gomega.Equal(time.Duration(5000) * time.Millisecond))

	// test setting a disconnected timeout
	t.Setenv("COHERENCE_SESSION_DISCONNECT_TIMEOUT", "6000")
	s, err = NewSession(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.DisconnectTimeout).To(gomega.Equal(time.Duration(6000) * time.Millisecond))

	// test setting a ready timeout
	t.Setenv("COHERENCE_READY_TIMEOUT", "7000")
	s, err = NewSession(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(s.sessOpts.ReadyTimeout).To(gomega.Equal(time.Duration(7000) * time.Millisecond))
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

	t.Setenv(envRequestTimeout, "-1")
	_, err := NewSession(ctx)
	g.Expect(err).To(gomega.HaveOccurred())
}
