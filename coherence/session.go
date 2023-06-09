/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrInvalidFormat indicates that the serialization format can only be JSON.
var ErrInvalidFormat = errors.New("format can only be 'json'")

const (
	defaultFormat            = "json"
	mapOrCacheExists         = "the %s %s already exists with different type parameters"
	defaultRequestTimeout    = "30000" // millis
	defaultDisconnectTimeout = "30000" // millis
	defaultReadyTimeout      = "0"     // millis
)

// Session provides APIs to create NamedCaches. The [NewSession] method creates a
// new instance of a [Session]. This method also takes a variable number of arguments, called options,
// that can be passed to configure the Session.
type Session struct {
	sessionID             uuid.UUID
	sessOpts              *SessionOptions
	conn                  *grpc.ClientConn
	dialOptions           []grpc.DialOption
	closed                bool
	caches                map[string]interface{}
	maps                  map[string]interface{}
	lifecycleListeners    []*SessionLifecycleListener
	sessionConnectCtx     context.Context
	mutex                 sync.RWMutex
	firstConnectAttempted bool           // indicates if the first connection has been attempted
	hasConnected          bool           // indicates if the session has ever connected
	debug                 func(v ...any) // a function to output debug messages
}

// SessionOptions holds the session attributes like host, port, tls attributes etc.
type SessionOptions struct {
	Address           string
	TLSEnabled        bool
	Scope             string
	Format            string
	ClientCertPath    string
	ClientKeyPath     string
	CaCertPath        string
	PlainText         bool
	RequestTimeout    time.Duration
	DisconnectTimeout time.Duration
	ReadyTimeout      time.Duration
}

// NewSession creates a new Session with the specified sessionOptions.
//
// Example 1: Create a Session that will eventually connect to host "localhost" and gRPC port: 1408 using an insecure connection.
//
//	ctx := context.Background()
//	session, err = coherence.NewSession(ctx, coherence.WithPlainText())
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Example 2: Create a Session that will eventually connect to host "acme.com" and gRPC port: 1408
//
//	session, err := coherence.NewSession(ctx, coherence.WithAddress("acme.com:1408"), coherence.WithPlainText())
//
// You can also set the environment variable COHERENCE_SERVER_ADDRESS to specify the address.
//
// Example 3: Create a Session that will eventually connect to default "localhost:1408" using a secured connection
//
//	session, err := coherence.NewSession(ctx)
//
// A Session can also be configured using environment variable COHERENCE_SERVER_ADDRESS. See [Grpc Naming]
// for information on values for this.
//
// To Configure SSL, you must first enable SSL on the gRPC Proxy, see [gRPC Proxy Server] for details.
//
// The following environment variables need to be set for the client:
//
//	export COHERENCE_TLS_CLIENT_CERT=/path/to/client/certificate
//	export COHERENCE_TLS_CLIENT_KEY=/path/path/to/client/key
//	export COHERENCE_TLS_CERTS_PATH=/path/to/cert/to/be/added/for/trust
//	export COHERENCE_IGNORE_INVALID_CERTS=true    // option to ignore self-signed certificates - for testing only. Not to be used in production
//
// Finally, the Close() method can be used to close the [Session]. Once a [Session] is closed, no APIs
// on the [NamedMap] instances should be invoked. If invoked they will return an error.
// [gRPC Naming]: https://github.com/grpc/grpc/blob/master/doc/naming.md
// [gRPC Proxy Server]: https://docs.oracle.com/en/middleware/standalone/coherence/14.1.1.2206/develop-remote-clients/using-coherence-grpc-server.html
func NewSession(ctx context.Context, options ...func(session *SessionOptions)) (*Session, error) {
	session := &Session{
		sessionID:             uuid.New(),
		sessionConnectCtx:     ctx,
		closed:                false,
		firstConnectAttempted: false,
		hasConnected:          false,
		debug:                 func(v ...any) {},
		maps:                  make(map[string]interface{}, 0),
		caches:                make(map[string]interface{}, 0),
		lifecycleListeners:    []*SessionLifecycleListener{},
		sessOpts: &SessionOptions{
			PlainText:         false,
			Format:            defaultFormat,
			RequestTimeout:    time.Duration(0) * time.Second,
			ReadyTimeout:      time.Duration(0) * time.Second,
			DisconnectTimeout: time.Duration(0) * time.Second},
	}

	if getBoolValueFromEnvVarOrDefault(envSessionDebug, false) {
		// enable session debugging
		session.debug = func(v ...any) {
			log.Println("DEBUG:", v)
		}
	}

	// apply any options
	for _, f := range options {
		f(session.sessOpts)
	}

	if session.sessOpts.Format != defaultFormat {
		return nil, ErrInvalidFormat
	}

	// if no address option sent in then use the env or defaults
	if session.sessOpts.Address == "" {
		session.sessOpts.Address = getStringValueFromEnvVarOrDefault(envHostName, "localhost:1408")
	}

	// if no request timeout then use the env or default
	if session.sessOpts.RequestTimeout == time.Duration(0) {
		timeout, err := getTimeoutValue(envRequestTimeout, defaultRequestTimeout, "request timeout")
		if err != nil {
			return nil, err
		}
		session.sessOpts.RequestTimeout = timeout
	}

	// if no disconnect timeout then use the env or default
	if session.sessOpts.DisconnectTimeout == time.Duration(0) {
		timeout, err := getTimeoutValue(envDisconnectTimeout, defaultDisconnectTimeout, "disconnect timeout")
		if err != nil {
			return nil, err
		}
		session.sessOpts.DisconnectTimeout = timeout
	}

	// if no ready timeout then use the env or default
	if session.sessOpts.ReadyTimeout == time.Duration(0) {
		timeout, err := getTimeoutValue(envReadyTimeout, defaultReadyTimeout, "ready timeout")
		if err != nil {
			return nil, err
		}
		session.sessOpts.ReadyTimeout = timeout
	}

	// ensure initial connection
	return session, session.ensureConnection()
}

func getTimeoutValue(envVar, defaultValue, description string) (time.Duration, error) {
	timeoutString := getStringValueFromEnvVarOrDefault(envVar, defaultValue)
	timeout, err := strconv.ParseInt(timeoutString, 10, 64)
	if err != nil || timeout < 0 {
		return 0, fmt.Errorf("invalid value of %s for %s", timeoutString, description)
	}
	return time.Duration(timeout) * time.Millisecond, nil
}

// WithAddress returns a function to set the address for session.
func WithAddress(host string) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.Address = host
	}
}

// WithFormat returns a function to set the format for a session. Currently, only "json" is supported.
func WithFormat(format string) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.Format = format
	}
}

// WithScope returns a function to set the scope for a session. This will prefix all
// caches with the provided scope to make them unique within a scope.
func WithScope(scope string) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.Scope = scope
	}
}

// WithPlainText returns a function to set the connection to plan text (insecure) for a session.
func WithPlainText() func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.PlainText = true
	}
}

// WithRequestTimeout returns a function to set the request timeout in millis.
func WithRequestTimeout(timeout time.Duration) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.RequestTimeout = timeout
	}
}

// WithDisconnectTimeout returns a function to set the maximum amount of time, in millis, a [Session]
// may remain in a disconnected state without successfully reconnecting.
func WithDisconnectTimeout(timeout time.Duration) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.DisconnectTimeout = timeout
	}
}

// WithReadyTimeout returns a function to set the maximum amount of time an [NamedMap] or [NamedCache]
// operations may wait for the underlying gRPC channel to be ready.  This is independent of the request
// timeout which sets a deadline on how long the call may take after being dispatched.
func WithReadyTimeout(timeout time.Duration) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.ReadyTimeout = timeout
	}
}

// ID returns the identifier of a session.
func (s *Session) ID() string {
	return s.sessionID.String()
}

// Close closes a connection.
func (s *Session) Close() {
	if !s.closed {
		s.maps = make(map[string]interface{}, 0)
		s.caches = make(map[string]interface{}, 0)
		err := s.conn.Close()
		s.closed = true
		s.dispatch(Closed, func() SessionLifecycleEvent {
			return newSessionLifecycleEvent(s, Closed)
		})
		if err != nil {
			log.Printf("Unable to close session %s %v", s.sessionID, err)
		}
	}
}

func (s *Session) String() string {
	return fmt.Sprintf("Session{id=%s, closed=%v, caches=%d, maps=%d, options=%v}", s.sessionID.String(), s.closed,
		len(s.caches), len(s.maps), s.sessOpts)
}

// GetRequestTimeout returns the session timeout in millis.
func (s *Session) GetRequestTimeout() time.Duration {
	return s.sessOpts.RequestTimeout
}

// GetDisconnectTimeout returns the session disconnect timeout in millis.
func (s *Session) GetDisconnectTimeout() time.Duration {
	return s.sessOpts.DisconnectTimeout
}

// GetReadyTimeout returns the session disconnect timeout in millis.
func (s *Session) GetReadyTimeout() time.Duration {
	return s.sessOpts.ReadyTimeout
}

// ensureConnection ensures a session has a valid connection
func (s *Session) ensureConnection() error {
	if s.firstConnectAttempted {
		// We have previously tried to connect so check that the connect state is connected
		if s.conn.GetState() != connectivity.Ready {
			// if the readyTime is set, and we are not connected then block and wait for connection
			if s.GetReadyTimeout() != 0 {
				return waitForReady(s)
			}
			s.debug(fmt.Sprintf("session: %s attempting connection to address %s", s.sessionID, s.sessOpts.Address))
			s.conn.Connect()
			return nil
		}
		return nil
	}

	var locked = false

	defer func() {
		if locked {
			s.mutex.Unlock()
		}
	}()

	s.dialOptions = []grpc.DialOption{}

	tlsOpt, err := s.sessOpts.createTLSOption()
	if err != nil {
		errString := fmt.Sprintf("error while setting up channel credentials: %v", err)
		return fmt.Errorf(errString)
	}
	s.dialOptions = append(s.dialOptions, tlsOpt)

	connOpt := grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  1.0 * time.Second,
			Multiplier: 1.1,
			Jitter:     0.0,
			MaxDelay:   3.0 * time.Second,
		},
		MinConnectTimeout: 10 * time.Second,
	})
	s.dialOptions = append(s.dialOptions, connOpt)

	s.mutex.Lock()
	locked = true

	newCtx, cancel := s.ensureContext(s.sessionConnectCtx)
	if cancel != nil {
		defer cancel()
	}

	conn, err := grpc.DialContext(newCtx, s.sessOpts.Address, s.dialOptions...)

	if err != nil {
		log.Printf("could not connect. Reason: %v", err)
		return err
	}

	s.conn = conn
	s.firstConnectAttempted = true

	// register for state change events - This uses and experimental gRPC API
	// so may not be reliable or may change in the future.
	// refer: https://grpc.github.io/grpc/core/md_doc_connectivity-semantics-and-api.html
	go func(session *Session) {
		var (
			firstConnect   = true
			connected      = false
			ctx            = context.Background()
			lastState      = session.conn.GetState()
			disconnectTime int64
		)

		for {
			// listen for change from the current lastState
			change := session.conn.WaitForStateChange(ctx, lastState)
			if !change {
				return
			}

			newState := session.conn.GetState()
			session.debug("connection:", lastState, "=>", newState)

			if newState == connectivity.Shutdown {
				log.Printf("closed session %v", session.sessionID)
				session.Close()
				return
			}

			if newState == connectivity.Ready {
				if !firstConnect && !connected {
					disconnectTime = 0
					log.Printf("session: %s re-connected to address %s", session.sessionID, session.sessOpts.Address)
					session.dispatch(Reconnected, func() SessionLifecycleEvent {
						return newSessionLifecycleEvent(session, Reconnected)
					})
					session.closed = false
					connected = true
				} else if firstConnect && !connected {
					firstConnect = false
					connected = true
					session.hasConnected = true
					log.Printf("session: %s connected to address %s", session.sessionID, session.sessOpts.Address)
					session.dispatch(Connected, func() SessionLifecycleEvent {
						return newSessionLifecycleEvent(session, Connected)
					})
				}
			} else {
				if connected {
					disconnectTime = -1
					log.Printf("session: %s disconnected from address %s", session.sessionID, session.sessOpts.Address)
					session.dispatch(Disconnected, func() SessionLifecycleEvent {
						return newSessionLifecycleEvent(session, Disconnected)
					})
					connected = false
				}

				if disconnectTime != 0 {
					if disconnectTime == -1 {
						disconnectTime = time.Now().UnixMilli()
					} else {
						waited := time.Now().UnixMilli() - disconnectTime
						if waited >= session.GetDisconnectTimeout().Milliseconds() {
							log.Printf("session: %s unable to reconnect within disconnect timeout of [%s]. Closing session.",
								session.sessionID, session.GetDisconnectTimeout().String())
							session.Close()
							return
						}
					}
				}

				// trigger a reconnection on state change
				if newState != connectivity.Connecting {
					conn.Connect()
				}
			}
			lastState = session.conn.GetState()
		}
	}(s)

	return nil
}

// waitForReady waits until the connection is ready up to the ready session timeout and will
// return nil if the session was connected, otherwise an error is returned.
// We intentionally do no use the gRPC WaitForReady as this can cause a race condition in the session
// events code.
func waitForReady(s *Session) error {
	var (
		readyTimeout  = s.GetReadyTimeout()
		messageLogged = false
	)

	// try to connect up until timeout, then throw err if not available
	timeout := time.Now().Add(readyTimeout)
	for {
		if time.Now().After(timeout) {
			return fmt.Errorf("unable to connect to %s after ready timeout of %v", s.sessOpts.Address, readyTimeout)
		}

		s.conn.Connect()

		time.Sleep(time.Duration(250) * time.Millisecond)
		state := s.conn.GetState()

		if state == connectivity.Ready {
			return nil
		}
		if !messageLogged {
			log.Printf("State is %v, waiting until ready timeout of %v for valid connection", state, readyTimeout)
			messageLogged = true
		}
	}
}

// GetOptions returns the options that were passed during this session creation.
func (s *Session) GetOptions() *SessionOptions {
	return s.sessOpts
}

// AddSessionLifecycleListener adds a SessionLifecycleListener that will receive events (connected, closed,
// disconnected or reconnected) that occur against the session.
func (s *Session) AddSessionLifecycleListener(listener SessionLifecycleListener) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, e := range s.lifecycleListeners {
		if *e == listener {
			return
		}
	}
	s.lifecycleListeners = append(s.lifecycleListeners, &listener)
}

// RemoveSessionLifecycleListener removes SessionLifecycleListener for a session.
func (s *Session) RemoveSessionLifecycleListener(listener SessionLifecycleListener) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	idx := -1
	listeners := s.lifecycleListeners
	for i, c := range listeners {
		if *c == listener {
			idx = i
			break
		}
	}
	if idx != -1 {
		result := append(listeners[:idx], listeners[idx+1:]...)
		s.lifecycleListeners = result
	}
}

// GetNamedMap returns a [NamedMap] from a session. If there is an existing [NamedMap] defined with the same
// type parameters and name it will be returned, otherwise a new instance will be returned.
// An error will be returned if there already exists a [NamedMap] with the same name and different type parameters.
//
//	// connect to the default address localhost:1408
//	session, err = coherence.NewSession(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
func GetNamedMap[K comparable, V any](session *Session, cacheName string, options ...func(session *CacheOptions)) (NamedMap[K, V], error) {
	return newNamedMap[K, V](session, cacheName, session.sessOpts, options...)
}

// GetNamedCache returns a [NamedCache] from a session.  [NamedCache] is syntactically identical in behaviour to a [NamedMap],
// but additionally implements the PutWithExpiry function. If there is an existing [NamedCache] defined with the same
// type parameters and name it will be returned, otherwise a new instance will be returned.
// An error will be returned if there already exists a [NamedCache] with the same name and different type parameters.
//
//	// connect to the default address localhost:1408
//	session, err = coherence.NewSession(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	namedMap, err := coherence.GetNamedCache[int, Person](session, "people")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// If you wish to create a [NamedCache] that has the same expiry for each entry you can use the [coherence.WithExpiry] cache option.
// Each call to Put will use the default expiry you have specified. If you use PutWithExpiry, this will override the default
// expiry for that key.
//
//	namedCache, err := coherence.GetNamedCache[int, Person](session, "cache-expiry", coherence.WithExpiry(time.Duration(5)*time.Second))
func GetNamedCache[K comparable, V any](session *Session, cacheName string, options ...func(session *CacheOptions)) (NamedCache[K, V], error) {
	return newNamedCache[K, V](session, cacheName, session.sessOpts, options...)
}

// IsClosed returns true if the Session is closed. Returns false otherwise.
func (s *Session) IsClosed() bool {
	return s.closed
}

// IsPlainText returns true if plain text, e.g. Non TLS. Returns false otherwise.
func (s *SessionOptions) IsPlainText() bool {
	return s.PlainText
}

func (s *SessionOptions) createTLSOption() (grpc.DialOption, error) {
	if s.PlainText {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}

	var (
		err          error
		cp           *x509.CertPool
		certData     []byte
		certificates = make([]tls.Certificate, 0)
	)

	// check whether to ignore invalid certs
	ignoreInvalidCertsEnv := getStringValueFromEnvVarOrDefault(envIgnoreInvalidCerts, "false")
	ignoreInvalidCerts := ignoreInvalidCertsEnv == "true"
	if ignoreInvalidCerts {
		log.Println("WARNING: you have turned off SSL certificate validation. This is insecure and not recommended.")
	}

	// retrieve the certificate path
	certPath := getStringValueFromEnvVarOrDefault(envTLSCertPath, "")
	clientCertEnv := getStringValueFromEnvVarOrDefault(envTLSClientCert, "")
	clientCertKeyEnv := getStringValueFromEnvVarOrDefault(envTLSClientKey, "")

	if certPath != "" {
		cp = x509.NewCertPool()

		log.Println("loading client certificates")
		if err = validateFilePath(certPath); err != nil {
			return nil, err
		}

		certData, err = os.ReadFile(certPath)
		if err != nil {
			return nil, err
		}

		if !cp.AppendCertsFromPEM(certData) {
			return nil, errors.New("credentials: failed to append certificates")
		}
	}

	if clientCertEnv != "" && clientCertKeyEnv != "" {
		if err = validateFilePath(clientCertEnv); err != nil {
			return nil, err
		}
		if err = validateFilePath(clientCertKeyEnv); err != nil {
			return nil, err
		}
		var clientCert tls.Certificate
		clientCert, err = tls.LoadX509KeyPair(clientCertEnv, clientCertKeyEnv)
		if err != nil {
			return nil, err
		}
		certificates = []tls.Certificate{clientCert}
	}

	config := &tls.Config{
		InsecureSkipVerify: ignoreInvalidCerts, //nolint
		RootCAs:            cp,
		Certificates:       certificates,
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(config)), nil
}

// validateFilePath checks to see if a file path is valid.
func validateFilePath(file string) error {
	if _, err := os.Stat(file); err == nil {
		return nil
	}

	return fmt.Errorf("%s is not a valid file", file)
}

// String returns a string representation of SessionOptions.
func (s *SessionOptions) String() string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("SessionOptions{address=%v, tLSEnabled=%v, scope=%v, format=%v, request timeout=%v, disconnect timeout=%v, ready timeout=%v",
		s.Address, s.TLSEnabled, s.Scope, s.Format, s.RequestTimeout, s.DisconnectTimeout, s.ReadyTimeout))

	if s.TLSEnabled {
		sb.WriteString(fmt.Sprintf(" clientCertPath=%v, clientKeyPath=%v, caCertPath=%v,",
			s.ClientCertPath, s.ClientKeyPath, s.CaCertPath))
	}

	return sb.String()
}

func (s *Session) dispatch(eventType SessionLifecycleEventType,
	creator func() SessionLifecycleEvent) {
	if len(s.lifecycleListeners) > 0 {
		event := creator()
		for _, l := range s.lifecycleListeners {
			e := *l
			e.getEmitter().emit(eventType, event)
		}
	}
}

// ensureContext will ensure that the context has deadline and if not will apply the timeout from the
// [SessionOptions].
func (s *Session) ensureContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); !ok {
		// no deadline set, so wrap the context in a RequestTimeout
		return context.WithTimeout(ctx, s.sessOpts.RequestTimeout)
	}
	return ctx, nil
}
