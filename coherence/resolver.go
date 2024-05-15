/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

/*

using the nslookup resolver:
	coherence:///host:port


*/
import (
	"errors"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence/discovery"
	"google.golang.org/grpc/resolver"
	"log"
	"strings"
)

const (
	nsLookupScheme = "coherence"
)

var (
	emptyAddresses = make([]string, 0)
	resolverDebug  = func(v ...any) {}
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

	r.start()
	return r, nil
}
func (*nsLookupResolverBuilder) Scheme() string { return nsLookupScheme }

type nsLookupResolver struct {
	target    resolver.Target
	cc        resolver.ClientConn
	addrStore map[string][]string
}

func (r *nsLookupResolver) resolve() {
	grpcEndpoints := generateNSAddresses(r.target.Endpoint())
	if len(grpcEndpoints) == 0 {
		msg := "resolver produced zero addresses"
		resolverDebug(msg)
		r.cc.ReportError(errors.New(msg))
		return
	}

	addresses := make([]resolver.Address, len(grpcEndpoints))
	for i, s := range grpcEndpoints {
		addresses[i] = resolver.Address{Addr: s}
	}
	resolverDebug(fmt.Sprintf("resolver produced the following addresses: %v", addresses))
	_ = r.cc.UpdateState(resolver.State{Addresses: addresses})
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
		resolverDebug(fmt.Sprintf("NSlookup returned error: %v", err))
		return emptyAddresses
	}
	return addresses
}

// NsLookupGrpcAddresses looks up grpc proxy server addresses based upon the provided
// name service address provided as host:port, e.g. localhost:7574.
func NsLookupGrpcAddresses(address string) ([]string, error) {
	var (
		addrString string
		_          []string
	)
	ns, err := discovery.Open(address, 10)
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
	if getBoolValueFromEnvVarOrDefault(envResolverDebug, false) {
		// enable session debugging
		resolverDebug = func(v ...any) {
			log.Println("RESOLVER DEBUG:", v)
		}
	}
}