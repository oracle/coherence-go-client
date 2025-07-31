/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
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
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrInvalidFormat indicates that the serialization format can only be JSON.
var (
	ErrInvalidFormat             = errors.New("format can only be 'json'")
	ErrInvalidNearCache          = errors.New("you must specify at least one near cache option")
	ErrInvalidNearCacheWithTTL   = errors.New("when using TTL for near cache you can only specify highUnits or highUnitsMemory")
	ErrInvalidNearCacheTTL       = errors.New("minimum near cache TTL is 1/4 of a second")
	ErrInvalidNearCacheWithNoTTL = errors.New("you can only specify highUnits or highUnitsMemory, not both")
	ErrNegativeNearCacheOptions  = errors.New("you cannot specify negative values for near cache options")
	ErrInvalidPruneFactor        = errors.New("prune factor must be between 0.1 and 1.0")
)

const (
	defaultFormat             = "json"
	mapOrCacheExists          = "the %s %s already exists with different type parameters"
	mapOrCacheExistsNearCache = "the %s %s already exists with different near cache options"
	defaultRequestTimeout     = "30000" // millis
	defaultDisconnectTimeout  = "30000" // millis
	defaultReadyTimeout       = "0"     // millis
	insecureWarning           = "WARNING: you have turned off SSL certificate validation. This is insecure and not recommended."
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
	genericSerializer     Serializer[any]
	mapMutex              sync.RWMutex
	caches                map[string]interface{}
	maps                  map[string]interface{}
	queues                map[string]interface{}
	topics                map[string]interface{}
	publishers            map[int64]interface{}
	subscribers           map[int64]interface{}
	cacheIDMap            safeMap[string, int32]
	queueIDMap            safeMap[string, int32]
	topicIDMap            safeMap[string, int32]
	subscriberIDMap       safeMap[int64, int32]
	publisherIDMap        safeMap[int64, int32]
	lifecycleMutex        sync.RWMutex
	lifecycleListeners    []*SessionLifecycleListener
	sessionConnectCtx     context.Context
	connectMutex          sync.RWMutex         // mutes for connection attempts
	firstConnectAttempted bool                 // indicates if the first connection has been attempted
	hasConnected          bool                 // indicates if the session has ever connected
	debug                 func(string, ...any) // a function to output DEBUG messages
	debugConnection       func(string, ...any) // a function to output ALL messages for gRPCV1 connections
	requestID             int64                // request id for gRPC v1
	filterID              int64                // filter id for gRPC v1
	v1StreamManagerCache  *streamManagerV1
	v1StreamManagerQueue  *streamManagerV1
	v1StreamManagerTopics *streamManagerV1
}

// SessionOptions holds the session attributes like host, port, tls attributes etc.
type SessionOptions struct {
	Address            string
	Scope              string
	Format             string
	ClientCertPath     string
	ClientKeyPath      string
	CaCertPath         string
	PlainText          bool
	IgnoreInvalidCerts bool
	RequestTimeout     time.Duration
	DisconnectTimeout  time.Duration
	ReadyTimeout       time.Duration
	TlSConfig          *tls.Config
}

// NewSession creates a new [Session] with the specified sessionOptions.
//
// Example 1: Create a [Session] that will eventually connect to host "localhost" and gRPC port: 1408 using an insecure connection.
//
//	ctx := context.Background()
//	session, err = coherence.NewSession(ctx, coherence.WithPlainText())
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Example 2: Create a [Session] that will eventually connect to host "acme.com" and gRPC port: 1408 using an insecure connection.
//
//	session, err := coherence.NewSession(ctx, coherence.WithAddress("acme.com:1408"), coherence.WithPlainText())
//
// You can also set the environment variable COHERENCE_SERVER_ADDRESS to specify the address.
//
// Example 3: Create a [Session] that will eventually connect to default "localhost:1408" using a secured connection.
//
//	session, err := coherence.NewSession(ctx)
//
// Example 4: Create a [Session] that will use the Coherence Name Service to discover the gRPC endpoints using a secured connection.
//
//	session, err := coherence.NewSession(ctx, coherence.WithAddress("coherence:///localhost:7574"))
//
// Example 5: Create a [Session] that will use the Coherence Name Service to discover the gRPC endpoints when multiple clusters are listening on the same cluster port using a secured connection.
//
//	session, err := coherence.NewSession(ctx, coherence.WithAddress("coherence:///localhost:7574/cluster2"))
//
// A [Session] can also be configured using environment variable COHERENCE_SERVER_ADDRESS.
// See [gRPC Naming] for information on values for this.
//
// To Configure SSL, you must first enable SSL on the gRPC Proxy, see [gRPC Proxy Server] for details.
//
// There are a number of ways to set the TLS options when creating a session.
// You can use [WithTLSConfig] to specify a custom tls.Config or specify the client certificate, key and trust
// certificate using additional session options or using environment variables. See below for more details.
//
//	myTlSConfig = &tls.Config{....}
//	session, err := coherence.NewSession(ctx, coherence.WithTLSConfig(myTLSConfig))
//
// You can also use the following to set the required TLS options when creating a session:
//
//	session, err := coherence.NewSession(ctx, coherence.WithTLSClientCert("/path/to/client/certificate"),
//	                                          coherence.WithTLSClientKey("/path/path/to/client/key"),
//	                                          coherence.WithTLSCertsPath("/path/to/cert/to/be/added/for/trust"))
//
// You can also use  coherence.[WithIgnoreInvalidCerts]() to ignore self-signed certificates for testing only, not to be used in production.
//
// The following environment variables can also be set for the client, and will override any options.
//
//	export COHERENCE_TLS_CLIENT_CERT=/path/to/client/certificate
//	export COHERENCE_TLS_CLIENT_KEY=/path/path/to/client/key
//	export COHERENCE_TLS_CERTS_PATH=/path/to/cert/to/be/added/for/trust
//	export COHERENCE_IGNORE_INVALID_CERTS=true    // option to ignore self-signed certificates for testing only, not to be used in production
//
// Finally, the Close() method can be used to close the [Session]. Once a [Session] is closed, no APIs
// on the [NamedMap] instances should be invoked. If invoked they will return an error.
//
// [gRPC Proxy Server]: https://docs.oracle.com/en/middleware/standalone/coherence/14.1.1.2206/develop-remote-clients/using-coherence-grpc-server.html
// [gRPC Naming]: https://github.com/grpc/grpc/blob/master/doc/naming.md
func NewSession(ctx context.Context, options ...func(session *SessionOptions)) (*Session, error) {
	session := &Session{
		sessionID:             uuid.New(),
		sessionConnectCtx:     ctx,
		closed:                false,
		firstConnectAttempted: false,
		hasConnected:          false,
		debug: func(_ string, _ ...any) {
			// empty by default
		},
		debugConnection: func(_ string, _ ...any) {
			// empty by default
		},
		maps:               make(map[string]interface{}, 0),
		caches:             make(map[string]interface{}, 0),
		queues:             make(map[string]interface{}, 0),
		topics:             make(map[string]interface{}, 0),
		publishers:         make(map[int64]interface{}, 0),
		subscribers:        make(map[int64]interface{}, 0),
		cacheIDMap:         newSafeIDMap(),
		queueIDMap:         newSafeIDMap(),
		topicIDMap:         newSafeIDMap(),
		publisherIDMap:     newSafeIDMapInt64(),
		subscriberIDMap:    newSafeIDMapInt64(),
		lifecycleListeners: []*SessionLifecycleListener{},
		sessOpts: &SessionOptions{
			PlainText:          false,
			IgnoreInvalidCerts: false,
			Format:             defaultFormat,
			RequestTimeout:     time.Duration(0) * time.Second,
			ReadyTimeout:       time.Duration(0) * time.Second,
			DisconnectTimeout:  time.Duration(0) * time.Second},
	}

	if getBoolValueFromEnvVarOrDefault(envResolverRandomize, true) {
		randomizeAddresses = true
	}

	// ensure name resolver has been registered
	resolver.Register(&nsLookupResolverBuilder{})

	// set the coherenceLogLevel
	setLogLevel(getStringValueFromEnvVarOrDefault(envLogLevel, "3"))

	if getBoolValueFromEnvVarOrDefault(envSessionDebug, false) || currentLogLevel >= int(DEBUG) {
		// enable session debugging
		session.debug = func(format string, v ...any) {
			logMessage(DEBUG, format, v...)
		}
		if currentLogLevel <= int(DEBUG) {
			currentLogLevel = int(DEBUG)
		}
	}

	messageDebug := getStringValueFromEnvVarOrDefault(envMessageDebug, "")
	if messageDebug != "" || currentLogLevel == int(ALL) {
		// enable session debugging
		session.debugConnection = func(s string, v ...any) {
			logMessage(DEBUG, s, v...)
		}
		currentLogLevel = int(ALL)
	}

	// apply any options
	for _, f := range options {
		f(session.sessOpts)
	}

	if session.sessOpts.Format != defaultFormat {
		return nil, ErrInvalidFormat
	}

	session.genericSerializer = NewSerializer[any](session.sessOpts.Format)

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

// setLogLevel sets the log level from the COHERENCE_LOG_LEVEL environment variable.
func setLogLevel(envLevel string) {
	var level int
	logLevelMutex.Lock()
	defer logLevelMutex.Unlock()

	// try to convert from integer first
	if lvl, err := strconv.Atoi(envLevel); err == nil {
		if lvl >= 1 && lvl <= 5 {
			currentLogLevel = lvl
			return
		}
	}

	// fall-through, check for string values
	switch envLevel {
	case "ERROR":
		level = 1
	case "WARNING":
		level = 2
	case "INFO":
		level = 3
	case "DEBUG":
		level = 4
	case "ALL":
		level = 5
	default:
		level = 3 // INFO
	}

	currentLogLevel = level
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

// WithIgnoreInvalidCerts returns a function to set the connection to ignore invalid certificates for a session.
func WithIgnoreInvalidCerts() func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.IgnoreInvalidCerts = true
	}
}

// WithPlainText returns a function to set the connection to plan text (insecure) for a session.
func WithPlainText() func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.PlainText = true
	}
}

// WithTLSCertsPath returns a function to set the (CA) certificates to be added for a session.
func WithTLSCertsPath(path string) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.CaCertPath = path
	}
}

// WithTLSClientCert returns a function to set the client certificate to be added for a session.
func WithTLSClientCert(path string) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.ClientCertPath = path
	}
}

// WithTLSClientKey returns a function to set the client key to be added for a session.
func WithTLSClientKey(path string) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.ClientKeyPath = path
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

// WithTLSConfig returns a function to set the tls.Config directly. This is typically used
// when you require fine-grained control over these options.
func WithTLSConfig(tlsConfig *tls.Config) func(sessionOptions *SessionOptions) {
	return func(s *SessionOptions) {
		s.TlSConfig = tlsConfig
	}
}

func (s *Session) NextRequestID() int64 {
	return atomic.AddInt64(&s.requestID, 1)
}

func (s *Session) NextFilterID() int64 {
	return atomic.AddInt64(&s.filterID, 1)
}

func (s *Session) getCacheID(cache string) *int32 {
	return s.cacheIDMap.Get(cache)
}

func (s *Session) getQueueID(queue string) *int32 {
	return s.queueIDMap.Get(queue)
}

func (s *Session) getTopicID(topic string) *int32 {
	return s.topicIDMap.Get(topic)
}

func (s *Session) getCacheNameFromCacheID(cacheID int32) *string {
	return s.cacheIDMap.KeyFromValue(cacheID)
}

func (s *Session) getNamedMapClient(name string) interface{} {
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	if existingCache, ok := s.maps[name]; ok {
		return existingCache
	}
	return nil
}

func (s *Session) getNamedCacheClient(name string) interface{} {
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	if existingCache, ok := s.caches[name]; ok {
		return existingCache
	}
	return nil
}

// ID returns the identifier of a session.
func (s *Session) ID() string {
	return s.sessionID.String()
}

// Close closes a connection.
func (s *Session) Close() {
	s.mapMutex.Lock()
	if !s.closed {
		s.maps = make(map[string]interface{}, 0)
		s.caches = make(map[string]interface{}, 0)
		err := s.conn.Close()
		s.closed = true

		s.mapMutex.Unlock()

		if s.GetProtocolVersion() > 0 {
			_ = s.v1StreamManagerCache.eventStream.grpcStream.CloseSend()
			if s.v1StreamManagerQueue != nil {
				_ = s.v1StreamManagerQueue.eventStream.grpcStream.CloseSend()
			}
		}
		s.dispatch(Closed, func() SessionLifecycleEvent {
			return newSessionLifecycleEvent(s, Closed)
		})
		if err != nil {
			logMessage(WARNING, "unable to close session %s %v", s.sessionID, err)
		}
	} else {
		defer s.mapMutex.Unlock()
	}
}

func (s *Session) String() string {
	var serverProtocolVersion int32
	if s.v1StreamManagerCache != nil {
		serverProtocolVersion = s.v1StreamManagerCache.serverProtocolVersion
	}
	return fmt.Sprintf("Session{id=%s, closed=%v, caches=%d, maps=%d, serverProtocolVersion=%v, options=%v}", s.sessionID.String(), s.closed,
		len(s.caches), len(s.maps), serverProtocolVersion, s.sessOpts)
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
	s.connectMutex.Lock()
	defer s.connectMutex.Unlock()

	if s.firstConnectAttempted {
		// We have previously tried to connect so check that the connect state is connected
		if s.conn.GetState() != connectivity.Ready {
			// if the readyTime is set, and we are not connected then block and wait for connection
			if s.GetReadyTimeout() != 0 {
				return waitForReady(s)
			}
			logMessage(INFO, "Session: [%s] attempting connection to address %s", s.sessionID, s.sessOpts.Address)
			s.conn.Connect()
			return nil
		}
		return nil
	}

	s.dialOptions = []grpc.DialOption{}

	tlsOpt, err := s.sessOpts.createTLSOption()
	if err != nil {
		errString := fmt.Sprintf("error while setting up channel credentials: %v", err)
		return errors.New(errString)
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

	newCtx, cancel := s.ensureContext(s.sessionConnectCtx)
	if cancel != nil {
		defer cancel()
	}

	//nolint:staticcheck // SA1019 - ignore for the moment
	conn, err := grpc.DialContext(newCtx, s.sessOpts.Address, s.dialOptions...)

	if err != nil {
		logMessage(WARNING, "Session: [%s] could not connect. Reason: %v", s.sessionID, err)
		return err
	}

	s.conn = conn
	s.firstConnectAttempted = true

	var apiMessage = " serverProtocolVersion: 0"

	// attempt to connect to V1 gRPC endpoint first and fallback if not available
	manager, err1 := newStreamManagerV1(s, cacheServiceProtocol)
	if err1 == nil {
		// save the stream manager for a successful V1 client connection
		s.v1StreamManagerCache = manager
		apiMessage = fmt.Sprintf(" %v", manager)
	} else {
		// check if this is a gRPC status error
		if sts, ok := status.FromError(err1); ok {
			if sts.Message() == "Method not found: coherence.proxy.v1.ProxyService/subChannel" {
				s.debug("error connecting to session via v1, falling back to v0: %v", err1)
			} else {
				s.debug("received a different gRPC error: %v", err1)
			}
		}
	}

	logMessage(INFO, "Session [%s] connected to [%s]%s", s.sessionID, s.sessOpts.Address, apiMessage)

	// register for state change events - This uses an experimental gRPC API
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

			if session.GetProtocolVersion() > 0 {
				firstConnect = !s.hasConnected
			}

			newState := session.conn.GetState()
			session.debug("connection: %v => %v", lastState, newState)

			if newState == connectivity.Shutdown {
				logMessage(INFO, "Session [%s] closed", session.sessionID)
				session.Close()
				return
			}

			if newState == connectivity.Ready {
				if !firstConnect && !connected {
					// Reconnected
					disconnectTime = 0
					session.closed = false
					connected = true

					logMessage(INFO, "Session [%s] re-connected to address %s (%v)", session.sessionID, session.sessOpts.Address, newState)
					session.dispatch(Reconnected, func() SessionLifecycleEvent {
						return newSessionLifecycleEvent(session, Reconnected)
					})
				} else if firstConnect && !connected {
					// Initial Connect
					firstConnect = false
					connected = true
					session.hasConnected = true
					logMessage(INFO, "Session [%s] connected to address %s", session.sessionID, session.sessOpts.Address)
					session.dispatch(Connected, func() SessionLifecycleEvent {
						return newSessionLifecycleEvent(session, Connected)
					})
				}
			} else {
				if connected {
					disconnectTime = -1
					logMessage(WARNING, "Session [%s] disconnected from address %s", session.sessionID, session.sessOpts.Address)
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
							logMessage(ERROR, "Session [%s] unable to reconnect within disconnect timeout of [%s], closing session.",
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

// GetProtocolVersion returns the protocol version used by the server.
func (s *Session) GetProtocolVersion() int32 {
	if s.v1StreamManagerCache == nil {
		return 0
	}
	return s.v1StreamManagerCache.serverProtocolVersion
}

// waitForReady waits until the connection is ready up to the ready session timeout and will
// return nil if the session was connected, otherwise an error is returned.
// We intentionally do not use the gRPC WaitForReady as this can cause a race condition in the session
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
			logMessage(INFO, "Session [%s] State is %v, waiting until ready timeout of %v for valid connection",
				s.sessionID, state, readyTimeout)
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
	s.lifecycleMutex.Lock()
	defer s.lifecycleMutex.Unlock()

	for _, e := range s.lifecycleListeners {
		if *e == listener {
			return
		}
	}
	s.lifecycleListeners = append(s.lifecycleListeners, &listener)
}

// RemoveSessionLifecycleListener removes SessionLifecycleListener for a session.
func (s *Session) RemoveSessionLifecycleListener(listener SessionLifecycleListener) {
	s.lifecycleMutex.Lock()
	defer s.lifecycleMutex.Unlock()

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
	return getNamedMap[K, V](session, cacheName, session.sessOpts, options...)
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
	return getNamedCache[K, V](session, cacheName, session.sessOpts, options...)
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

	// check if a tls.Config has been set and use this, otherwise continue to check for env and other options
	if s.TlSConfig != nil {
		if s.TlSConfig.InsecureSkipVerify {
			logMessage(WARNING, insecureWarning)
		}
		return grpc.WithTransportCredentials(credentials.NewTLS(s.TlSConfig)), nil
	}

	var (
		err          error
		cp           *x509.CertPool
		certData     []byte
		certificates = make([]tls.Certificate, 0)
	)

	// check whether to ignore invalid certs, check env then option
	ignoreInvalidCertsEnv := getStringValueFromEnvVarOrDefault(envIgnoreInvalidCerts, "")
	if ignoreInvalidCertsEnv == "" {
		// get value from options
		ignoreInvalidCertsEnv = fmt.Sprintf("%v", s.IgnoreInvalidCerts)
	}

	ignoreInvalidCerts := ignoreInvalidCertsEnv == "true"
	if ignoreInvalidCerts {
		logMessage(WARNING, insecureWarning)
	}
	s.IgnoreInvalidCerts = ignoreInvalidCerts

	// search TLS options in ENV first
	certPathEnv := getStringValueFromEnvVarOrDefault(envTLSCertPath, "")
	clientCertEnv := getStringValueFromEnvVarOrDefault(envTLSClientCert, "")
	clientCertKeyEnv := getStringValueFromEnvVarOrDefault(envTLSClientKey, "")

	// If the env options are empty then populate them from the options
	if certPathEnv == "" {
		certPathEnv = s.CaCertPath
	}
	if clientCertEnv == "" {
		clientCertEnv = s.ClientCertPath
	}
	if clientCertKeyEnv == "" {
		clientCertKeyEnv = s.ClientKeyPath
	}

	s.CaCertPath = certPathEnv
	s.ClientKeyPath = clientCertKeyEnv
	s.ClientCertPath = clientCertEnv

	if s.CaCertPath != "" {
		cp = x509.NewCertPool()

		logMessage(DEBUG, "loading CA certificate")
		if err = validateFilePath(s.CaCertPath); err != nil {
			return nil, err
		}

		certData, err = os.ReadFile(s.CaCertPath)
		if err != nil {
			return nil, err
		}

		if !cp.AppendCertsFromPEM(certData) {
			return nil, errors.New("credentials: failed to append certificates")
		}
	}

	if s.ClientCertPath != "" && s.ClientKeyPath != "" {
		logMessage(DEBUG, "loading client certificate and key paths, cert=%s, key=%s", s.ClientCertPath, s.ClientKeyPath)
		if err = validateFilePath(s.ClientCertPath); err != nil {
			return nil, err
		}
		if err = validateFilePath(s.ClientKeyPath); err != nil {
			return nil, err
		}
		var clientCert tls.Certificate
		clientCert, err = tls.LoadX509KeyPair(s.ClientCertPath, s.ClientKeyPath)
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
	sb.WriteString(fmt.Sprintf("SessionOptions{address=%v, plainText=%v, scope=%v, format=%v, requestTimeout=%v, disconnectTimeout=%v, readyTimeout=%v",
		s.Address, s.PlainText, s.Scope, s.Format, s.RequestTimeout, s.DisconnectTimeout, s.ReadyTimeout))

	if !s.PlainText {
		if s.TlSConfig == nil {
			sb.WriteString(fmt.Sprintf(" clientCertPath=%v, clientKeyPath=%v, caCertPath=%v, igoreInvalidCerts=%v",
				s.ClientCertPath, s.ClientKeyPath, s.CaCertPath, s.IgnoreInvalidCerts))
		} else {
			sb.WriteString("tls.Config specified")
		}
	}

	return sb.String()
}

func (s *Session) dispatch(eventType SessionLifecycleEventType, creator func() SessionLifecycleEvent) {
	s.lifecycleMutex.Lock()
	defer s.lifecycleMutex.Unlock()
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

// a thread safe map for storage of keys and values. Used for cacheId and queueId mappings.
type safeMap[K comparable, V any] interface {
	Add(key K, value V)
	Clear()
	Get(key K) *V
	Remove(key K)
	Keys() []K
	KeyFromValue(V) *K
}

type safeMapImpl[K comparable, V any] struct {
	mutex       sync.RWMutex
	internalMap map[K]V
}

func (m *safeMapImpl[K, V]) Add(key K, value V) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.internalMap[key] = value
}

func (m *safeMapImpl[K, V]) Keys() []K {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	keys := make([]K, 0)
	for k := range m.internalMap {
		keys = append(keys, k)
	}

	return keys
}

func (m *safeMapImpl[K, V]) KeyFromValue(value V) *K {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for k, v := range m.internalMap {
		if reflect.DeepEqual(v, value) {
			return &k
		}
	}

	return nil
}

func (m *safeMapImpl[K, V]) Get(key K) *V {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if v, ok := m.internalMap[key]; ok {
		return &v
	}
	return nil
}

func (m *safeMapImpl[K, V]) Remove(key K) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.internalMap, key)
}

func (m *safeMapImpl[K, V]) Clear() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.internalMap = make(map[K]V, 0)
}

func newSafeIDMap() safeMap[string, int32] {
	return &safeMapImpl[string, int32]{internalMap: make(map[string]int32, 0)}
}

func newSafeIDMapInt64() safeMap[int64, int32] {
	return &safeMapImpl[int64, int32]{internalMap: make(map[int64]int32, 0)}
}
