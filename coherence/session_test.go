/*
* Copyright (c) 2022, 2025 Oracle and/or its affiliates.
* Licensed under the Universal Permissive License v 1.0 as shown at
* https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestSessionValidation(t *testing.T) {
	ctx := context.Background()

	_, err := NewSession(ctx, WithFormat("not-json"))
	if err != ErrInvalidFormat {
		t.Fatalf("expected ErrInvalidFormat, got %v", err)
	}

	// test default timeout
	timeout, err := strconv.ParseInt(defaultRequestTimeout, 10, 64)
	if err != nil {
		t.Fatalf("failed to parse default request timeout: %v", err)
	}

	s, err := NewSession(ctx)
	if err != nil {
		t.Fatalf("unexpected error creating default session: %v", err)
	}
	if got := s.sessOpts.RequestTimeout; got != time.Duration(timeout)*time.Millisecond {
		t.Fatalf("expected default timeout %v, got %v", time.Duration(timeout)*time.Millisecond, got)
	}

	// test setting a request timeout
	s, err = NewSession(ctx, WithRequestTimeout(33*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error setting request timeout: %v", err)
	}
	if got := s.sessOpts.RequestTimeout; got != 33*time.Millisecond {
		t.Fatalf("expected request timeout 33ms, got %v", got)
	}

	// test setting a disconnected timeout
	s, err = NewSession(ctx, WithDisconnectTimeout(34*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error setting disconnect timeout: %v", err)
	}
	if got := s.sessOpts.DisconnectTimeout; got != 34*time.Millisecond {
		t.Fatalf("expected disconnect timeout 34ms, got %v", got)
	}

	// test setting a ready timeout
	s, err = NewSession(ctx, WithReadyTimeout(35*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error setting ready timeout: %v", err)
	}
	if got := s.sessOpts.ReadyTimeout; got != 35*time.Millisecond {
		t.Fatalf("expected ready timeout 35ms, got %v", got)
	}
}

func TestSessionEnvValidation(t *testing.T) {
	ctx := context.Background()

	t.Setenv("COHERENCE_CLIENT_REQUEST_TIMEOUT", "5000")
	s, err := NewSession(ctx)
	if err != nil {
		t.Fatalf("unexpected error creating session: %v", err)
	}
	if got := s.sessOpts.RequestTimeout; got != 5000*time.Millisecond {
		t.Fatalf("expected request timeout 5000ms, got %v", got)
	}

	t.Setenv("COHERENCE_SESSION_DISCONNECT_TIMEOUT", "6000")
	s, err = NewSession(ctx)
	if err != nil {
		t.Fatalf("unexpected error creating session with disconnect timeout: %v", err)
	}
	if got := s.sessOpts.DisconnectTimeout; got != 6000*time.Millisecond {
		t.Fatalf("expected disconnect timeout 6000ms, got %v", got)
	}

	t.Setenv("COHERENCE_READY_TIMEOUT", "7000")
	s, err = NewSession(ctx)
	if err != nil {
		t.Fatalf("unexpected error creating session with ready timeout: %v", err)
	}
	if got := s.sessOpts.ReadyTimeout; got != 7000*time.Millisecond {
		t.Fatalf("expected ready timeout 7000ms, got %v", got)
	}
}

func TestSessionEnvDebug(t *testing.T) {
	ctx := context.Background()
	t.Setenv(envSessionDebug, "true")

	_, err := NewSession(ctx)
	if err != nil {
		t.Fatalf("expected no error creating session with debug env, got: %v", err)
	}
}

func TestConnectionOverride(t *testing.T) {
	ctx := context.Background()
	t.Setenv(envHostName, "localhost:12345")

	s, err := NewSession(ctx)
	if err != nil {
		t.Fatalf("expected no error creating session, got: %v", err)
	}

	if s.sessOpts.Address != "localhost:12345" {
		t.Fatalf("expected address to be 'localhost:12345', got: %s", s.sessOpts.Address)
	}
}

func TestInvalidFormat(t *testing.T) {
	ctx := context.Background()

	_, err := NewSession(ctx, WithFormat("abc"))
	if err == nil {
		t.Fatal("expected error when using invalid format, got nil")
	}
}

func TestSessionTimeout(t *testing.T) {
	ctx := context.Background()
	t.Setenv(envRequestTimeout, "-1")

	_, err := NewSession(ctx)
	if err == nil {
		t.Fatal("expected error due to invalid request timeout, got nil")
	}
}
