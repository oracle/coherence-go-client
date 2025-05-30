/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"errors"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence/discovery"
	"google.golang.org/grpc/resolver"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	nsLookupScheme       = "coherence"
	defaultRetries       = 20
	defaultResolverDelay = 1000 // ms
)

var (
	emptyAddresses = make([]string, 0)
	resolverDebug  = func(_ string, _ ...any) {
		// noop as default debug mode
	}
	randomizeAddresses bool
)

type nsLookupResolverBuilder struct {
}

func (b *nsLookupResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	r := &nsLookupResolver{
		target: target,
		cc:     cc,
		addrStore: map[string][]string{
			nsLookupScheme: generateNSAddresses(target.Endpoint()),
		},
	}
	checkResolverDebug()

	// set the number of resolver retried
	retries := getStringValueFromEnvVarOrDefault(envResolverRetries, "20")
	retriesValue, err := strconv.Atoi(retries)
	if err != nil {
		retriesValue = defaultRetries
	}

	resolverDebug("resolver retries=%v", retriesValue)
	r.resolverRetries = retriesValue

	r.start()
	return r, nil
}
func (*nsLookupResolverBuilder) Scheme() string { return nsLookupScheme }

type nsLookupResolver struct {
	target          resolver.Target
	cc              resolver.ClientConn
	mutex           sync.Mutex
	addrStore       map[string][]string
	resolverRetries int
}

func (r *nsLookupResolver) resolve() {
	r.mutex.Lock()
	grpcEndpoints := generateNSAddresses(r.target.Endpoint())
	defer r.mutex.Unlock()

	if len(grpcEndpoints) == 0 {
		// try r.resolverRetries; times over 2 seconds to get gRPC addresses as we may be in the middle of fail-over
		for i := 1; i <= r.resolverRetries; i++ {
			resolverDebug("retrying NSLookup attempt: %v", i)
			time.Sleep(time.Duration(defaultResolverDelay) * time.Millisecond)
			grpcEndpoints = generateNSAddresses(r.target.Endpoint())
			if len(grpcEndpoints) != 0 {
				break
			}
		}

		if len(grpcEndpoints) == 0 {
			msg := "resolver produced zero addresses"
			resolverDebug(msg)
			r.cc.ReportError(errors.New(msg))
			return
		}
	}

	addresses := make([]resolver.Address, len(grpcEndpoints))
	for i, s := range grpcEndpoints {
		addresses[i] = resolver.Address{Addr: s}
	}

	if randomizeAddresses {
		// randomize the address list
		rand.NewSource(time.Now().UnixNano())
		rand.Shuffle(len(addresses), func(i, j int) {
			addresses[i], addresses[j] = addresses[j], addresses[i]
		})
	}

	resolverDebug("resolver produced the following addresses: %v, randomize=%v", addresses, randomizeAddresses)
	_ = r.cc.UpdateState(resolver.State{Addresses: addresses})
	if len(addresses) > 0 {
		resolverDebug("resolver chose address: %s", addresses[0])
	}
}

func (r *nsLookupResolver) start() {
	r.resolve()
}
func (r *nsLookupResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	r.resolve()
}

func (*nsLookupResolver) Close() {
}

func generateNSAddresses(endpoint string) []string {
	addresses, err := NsLookupGrpcAddresses(endpoint)
	if err != nil {
		resolverDebug("NSlookup returned error: %v", err)
		return emptyAddresses
	}
	return addresses
}

// NsLookupGrpcAddresses looks up grpc proxy server addresses based upon the provided
// name service address provided as host:port, e.g. localhost:7574[/cluster].
func NsLookupGrpcAddresses(address string) ([]string, error) {
	var (
		addrString     string
		_              []string
		foreignCluster string
	)

	// check to see a cluster is provided for lookup
	if strings.Contains(address, "/") {
		s := strings.Split(address, "/")
		address = s[0]
		foreignCluster = s[1]

		// retrieve the foreign clusters actual name service address
		nsF, err := discovery.Open(address, discovery.DefaultTimeout)
		if err != nil {
			return emptyAddresses, err
		}
		defer nsF.Close()

		query := discovery.NSPrefix + discovery.ClusterForeignLookup + "/" + foreignCluster + discovery.NSLocalPort
		resolverDebug("lookup for foreign cluster NS port using %s", query)
		port, err := nsF.Lookup(query)
		if err != nil {
			return emptyAddresses, fmt.Errorf("unable to lookup foreign clsuter NS port: %v", err)
		}

		// get the IP address portion
		s1 := strings.Split(address, ":")
		address = fmt.Sprintf("%s:%v", s1[0], port)

		resolverDebug("NS port for %s is %s", foreignCluster, address)
		// fall through and do the actual lookup using new address
	}

	ns, err := discovery.Open(address, discovery.DefaultTimeout)
	if err != nil {
		return emptyAddresses, err
	}

	defer ns.Close()

	addrString, err = ns.Lookup(discovery.NSPrefix + discovery.GrpcProxyLookup)
	if err != nil {
		return emptyAddresses, err
	}

	// parse the addresses which should be in the format of
	// [127.0.0.1, 58193, 127.0.0.1, 58192, 127.0.0.1, 58194]
	// and will be returned in array of 127.0.0.1:58193, 127.0.0.1:58192, 127.0.0.1:58194
	return parseNsLookupString(addrString)
}

func parseNsLookupString(addresses string) ([]string, error) {
	errInvalid := fmt.Errorf("invalid nslookup response of [%s]", addresses)
	if addresses == "" {
		return emptyAddresses, errInvalid
	}
	if addresses == "[]" {
		return emptyAddresses, nil
	}

	if !strings.HasPrefix(addresses, "[") || !strings.HasSuffix(addresses, "]") {
		return emptyAddresses, errInvalid
	}

	// split the addresses
	addresses = strings.ReplaceAll(addresses, "[", "")
	addresses = strings.ReplaceAll(addresses, "]", "")
	s := strings.Split(addresses, ",")
	l := len(s)

	// should not be zero or an odd number of values, it should be even with IP/ host
	if l == 0 || l%2 != 0 {
		return emptyAddresses, errInvalid
	}

	results := make([]string, l/2)
	for i := 0; i < l/2; i++ {
		results[i] = fmt.Sprintf("%s:%s", strings.TrimSpace(s[i*2]), strings.TrimSpace(s[i*2+1]))
	}

	return results, nil
}

func checkResolverDebug() {
	if getBoolValueFromEnvVarOrDefault(envResolverDebug, false) || currentLogLevel >= int(DEBUG) {
		// enable session debugging
		resolverDebug = func(s string, v ...any) {
			logMessage(DEBUG, s, v...)
		}
		if currentLogLevel <= int(DEBUG) {
			currentLogLevel = int(DEBUG)
		}
	}
}
