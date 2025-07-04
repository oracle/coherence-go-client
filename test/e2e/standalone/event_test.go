/*
 * Copyright (c) 2022, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"context"
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const timeout = 20

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
}

var (
	alvin = utils.Person{ID: 1, Name: "Alvin", Age: 40, Salary: 1000,
		Languages:   []string{"English", "French"},
		HomeAddress: utils.Address{Address1: "address1", Address2: "address2", City: "Perth", State: "WA", PostCode: 6000}}

	bill = utils.Person{ID: 1, Name: "Bill", Age: 45, Salary: 1000,
		Languages:   []string{"English", "French"},
		HomeAddress: utils.Address{Address1: "address1", Address2: "address2", City: "Perth", State: "WA", PostCode: 6000}}
	charlie = utils.Person{ID: 1, Name: "Charlie", Age: 29, Salary: 1000,
		Languages:   []string{"English", "French"},
		HomeAddress: utils.Address{Address1: "address1", Address2: "address2", City: "Perth", State: "WA", PostCode: 6000}}
)

func TestMapAndLifecycleEventsAll(t *testing.T) {
	expectedA := "A"
	expectedB := "B"

	expected := ExpectedEvents[string, string]{
		inserts: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryInserted,
				key:       &expectedA,
				old:       nil,
				new:       &expectedA,
			},
		},
		updates: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryUpdated,
				key:       &expectedA,
				old:       &expectedA,
				new:       &expectedB,
			},
		},
		deletes: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryDeleted,
				key:       &expectedA,
				old:       &expectedB,
				new:       nil,
			},
		},
	}

	g, session := initTest(t)
	defer session.Close()

	namedCache := GetNamedCache[string, string](g, session, "test-events-all-cache")
	namedMap := GetNamedMap[string, string](g, session, "test-events-all-map")

	runBasicTests(g, namedCache, namedCache.Name(), &expected, -1)
	runBasicTests(g, namedMap, namedMap.Name(), &expected, -1)
}

func TestMapAndLifecycleEventsAll1(t *testing.T) {
	g, session := initTest(t)
	defer session.Close()

	namedCache := GetNamedCache[string, string](g, session, "test-lifecycle-release-cache")
	namedMap := GetNamedMap[string, string](g, session, "test-lifecycle-release-map")

	runReleasedLifecycleTests(g, namedMap)
	runReleasedLifecycleTests(g, namedCache)
}

func TestMapAndLifecycleEventsAll2(t *testing.T) {
	g, session := initTest(t)
	defer session.Close()

	namedCache := GetNamedCache[string, string](g, session, "test-lifecycle-all-cache-multi")
	namedMap := GetNamedMap[string, string](g, session, "test-lifecycle-all-map-multi")

	runMultipleLifecycleTests(g, namedMap)
	runMultipleLifecycleTests(g, namedCache)
}

func TestMapAndLifecycleEventsAll3(t *testing.T) {
	g, session := initTest(t)
	defer session.Close()

	namedCache := GetNamedCache[string, string](g, session, "test-lifecycle-all-cache")
	namedMap := GetNamedMap[string, string](g, session, "test-lifecycle-all-map")

	runBasicLifecycleTests(g, namedMap, namedMap.Name())
	runBasicLifecycleTests(g, namedCache, namedCache.Name())
}

func TestMapAndLifecycleEventsAll4(t *testing.T) {

	g, session := initTest(t)
	defer session.Close()

	if session.GetProtocolVersion() == 0 {
		t.Skip("Skip for gRPC v0")
	}

	namedCache := GetNamedCache[string, string](g, session, "test-lifecycle-all-cache")
	namedMap := GetNamedMap[string, string](g, session, "test-lifecycle-all-map")

	runMultipleLifecycleTests(g, namedMap)
	runMultipleLifecycleTests(g, namedCache)
}

func TestExpiringEvents(t *testing.T) {
	g, session := initTest(t)
	defer session.Close()

	runTestExpiringEvents(g, GetNamedCache[string, string](g, session, "test-expiring-events-cache"))
}

// TestEventDisconnect tests to ensure that if we get a disconnect, then we can rec-connect and
// re-register event listeners.
func TestEventDisconnect(t *testing.T) {
	g, session := initTest(t,
		coherence.WithDisconnectTimeout(time.Duration(130)*time.Second),
		coherence.WithReadyTimeout(time.Duration(130)*time.Second))
	defer session.Close()

	if session.GetProtocolVersion() == 0 {
		t.Skip("Skip for gRPC v0")
	}

	namedCache := GetNamedCache[string, string](g, session, "test-reconnect-cache")
	g.Expect(namedCache.Clear(ctx)).To(gomega.BeNil())

	RunTestReconnect(g, namedCache, true)

	namedMap := GetNamedMap[string, string](g, session, "test-reconnect-map")
	g.Expect(namedMap.Clear(ctx)).To(gomega.BeNil())
	RunTestReconnect(g, namedMap, true)
}

// TestEventDisconnectWithReadyTimeoutDelay tests that the ready timeout is honoured,
// as we have stopped the gRPC proxy before the test runs.
func TestEventDisconnectWithReadyTimeoutDelay(t *testing.T) {
	fmt.Println("Issue stop of $GRPC:GrpcProxy")
	_, err := utils.IssuePostRequest("http://127.0.0.1:30000/management/coherence/cluster/services/$GRPC:GrpcProxy/members/1/stop")
	if err != nil {
		t.Error("Unable to issue post request to stop gRPC proxy", err)
	}

	utils.Sleep(5)

	g, session := initTest(t, coherence.WithReadyTimeout(time.Duration(130)*time.Second))
	defer session.Close()

	if session.GetProtocolVersion() == 0 {
		t.Skip("Skip for gRPC v0")
	}

	namedCache := GetNamedCache[string, string](g, session, "test-reconnect-cache")
	RunTestReconnect(g, namedCache, false)

	namedMap := GetNamedMap[string, string](g, session, "test-reconnect-map")
	RunTestReconnect(g, namedMap, false)
}

func TestMapEventInsertsOnly(t *testing.T) {
	expectedA := "A"

	expected := ExpectedEvents[string, string]{
		inserts: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryInserted,
				key:       &expectedA,
				old:       nil,
				new:       &expectedA,
			},
		},
		updates: []*ValidateEvent[string, string]{},
		deletes: []*ValidateEvent[string, string]{},
	}

	g, session := initTest(t)
	defer session.Close()

	namedCache := GetNamedCache[string, string](g, session, "test-events-inserts-only-cache")
	namedMap := GetNamedMap[string, string](g, session, "test-events-inserts-only-map")

	runBasicTests(g, namedCache, namedCache.Name(), &expected, filters.MaskInserted)
	runBasicTests(g, namedMap, namedMap.Name(), &expected, filters.MaskInserted)
}

func TestMapEventUpdatesOnly(t *testing.T) {
	expectedA := "A"
	expectedB := "B"

	expected := ExpectedEvents[string, string]{
		inserts: []*ValidateEvent[string, string]{},
		updates: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryUpdated,
				key:       &expectedA,
				old:       &expectedA,
				new:       &expectedB,
			},
		},
		deletes: []*ValidateEvent[string, string]{},
	}

	g, session := initTest(t)
	defer session.Close()

	namedCache := GetNamedCache[string, string](g, session, "test-events-updates-only-cache")
	namedMap := GetNamedMap[string, string](g, session, "test-events-updates-only-map")

	runBasicTests(g, namedCache, namedCache.Name(), &expected, filters.MaskUpdated)
	runBasicTests(g, namedMap, namedMap.Name(), &expected, filters.MaskUpdated)
}

func TestMapEventDeletesOnly(t *testing.T) {
	expectedA := "A"
	expectedB := "B"

	expected := ExpectedEvents[string, string]{
		inserts: []*ValidateEvent[string, string]{},
		updates: []*ValidateEvent[string, string]{},
		deletes: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryDeleted,
				key:       &expectedA,
				old:       &expectedB,
				new:       nil,
			},
		},
	}

	g, session := initTest(t)
	defer session.Close()

	namedCache := GetNamedCache[string, string](g, session, "test-events-deletes-only-cache")
	namedMap := GetNamedMap[string, string](g, session, "test-events-deletes-only-map")

	runBasicTests(g, namedCache, namedCache.Name(), &expected, filters.MaskDeleted)
	runBasicTests(g, namedMap, namedMap.Name(), &expected, filters.MaskDeleted)
}

func TestMapEventMultipleListeners(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testMaps := []coherence.NamedMap[string, string]{
		GetNamedCache[string, string](g, session, "event-multiple-listeners-cache"),
		GetNamedMap[string, string](g, session, "event-multiple-listeners-map"),
	}

	// run tests against NamedMap and NamedCache
	for _, v := range testMaps {
		RunTestMultipleListeners(g, v)
		err := v.Destroy(ctx)
		if err != nil && err != coherence.ErrDestroyed {
			log.Printf("Error destroying map %s: %s", v.Name(), err)
		}
	}
}

// RunTestReconnect tests that a gRPC connection will reset it's self and the map listeners
// will re-register correctly.
func RunTestReconnect(g *gomega.WithT, namedMap coherence.NamedMap[string, string], doStop bool) {
	listener := NewReconnectMapListener[string, string]("test")

	err := namedMap.AddListener(ctx, listener.listener)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	defer func() {
		_ = namedMap.RemoveListener(ctx, listener.listener)
	}()

	var (
		iterations int32 = 100
		additional int32 = 10
	)

	createMutations(g, namedMap, iterations)

	if doStop {
		// issue a stop, which better simulates a sudden disconnect
		// vs shutdown (which is graceful), for the "$GRPC:GrpcProxy" on node 1.
		// the client should eventually connect
		log.Println("Issue stop of $GRPC:GrpcProxy")
		_, err = utils.IssuePostRequest("http://127.0.0.1:30000/management/coherence/cluster/services/$GRPC:GrpcProxy/members/1/stop")
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		log.Println("Sleeping to allow proxy to stop and restart")
		utils.Sleep(5)
	}

	log.Println("Sleeping to test re-connect")
	utils.Sleep(5)

	_, err = namedMap.Size(ctx)
	if err != nil {
		// error could have occurred, we don't care as long as next reconnects
		log.Println("error from Size", err)
	}

	utils.Sleep(5)

	// add another 'additional' mutations
	createMutations(g, namedMap, additional)

	utils.Sleep(5)
	g.Eventually(func() int32 { return listener.insertedCount() }).Should(gomega.Equal(int32(iterations + additional)))
	g.Eventually(func() int32 { return listener.updatedCount() }).Should(gomega.Equal(int32(iterations + additional)))
	g.Eventually(func() int32 { return listener.deletedCount() }).Should(gomega.Equal(int32(iterations + additional)))
}

// createMutations creates a specified number of data mutations.
func createMutations(g *gomega.WithT, namedMap coherence.NamedMap[string, string], iters int32) {
	var err error
	log.Println("createMutations, iters=", iters)
	var i int32
	for i = 0; i < iters; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		_, err = namedMap.Put(ctx, key, value)
		g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

		newValue := fmt.Sprintf("new-value-%d", i)
		_, err = namedMap.Put(ctx, key, newValue)
		g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

		_, err = namedMap.Remove(ctx, key)
		g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	}
}

func RunTestMultipleListeners(g *gomega.WithT, namedMap coherence.NamedMap[string, string]) {
	expectedA := "A"
	expectedB := "B"

	expected := ExpectedEvents[string, string]{
		inserts: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryInserted,
				key:       &expectedA,
				old:       nil,
				new:       &expectedA,
			},
		},
		updates: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryUpdated,
				key:       &expectedA,
				old:       &expectedA,
				new:       &expectedB,
			},
		},
		deletes: []*ValidateEvent[string, string]{
			{
				eventType: coherence.EntryDeleted,
				key:       &expectedA,
				old:       &expectedB,
				new:       nil,
			},
		},
	}

	listener := NewCountingMapListener[string, string]("multiple-1")
	listener2 := NewCountingMapListener[string, string]("multiple-2")

	err1 := namedMap.AddListener(ctx, listener.listener)
	g.Expect(err1).ShouldNot(gomega.HaveOccurred())

	err2 := namedMap.AddListener(ctx, listener2.listener)
	g.Expect(err2).ShouldNot(gomega.HaveOccurred())

	_, err3 := namedMap.Put(ctx, "A", "A")
	g.Expect(err3).ShouldNot(gomega.HaveOccurred())

	_, err4 := namedMap.Put(ctx, "A", "B")
	g.Expect(err4).ShouldNot(gomega.HaveOccurred())

	_, err5 := namedMap.Remove(ctx, "A")
	g.Expect(err5).ShouldNot(gomega.HaveOccurred())

	listener.waitFor(expected.total(), 3*time.Second)
	listener2.waitFor(expected.total(), 3*time.Second)

	expected.validate(g, namedMap.Name(), listener)
	expected.validate(g, namedMap.Name(), listener2)

	// remove the listener and trigger some events.  Ensure no events captured for listener but
	// events captured by the listener2
	listener.reset()
	listener2.reset()
	err6 := namedMap.RemoveListener(ctx, listener.listener)
	g.Expect(err6).ShouldNot(gomega.HaveOccurred())

	_, err7 := namedMap.Put(ctx, "A", "A")
	g.Expect(err7).ShouldNot(gomega.HaveOccurred())

	_, err8 := namedMap.Put(ctx, "A", "B")
	g.Expect(err8).ShouldNot(gomega.HaveOccurred())

	_, err9 := namedMap.Remove(ctx, "A")
	g.Expect(err9).ShouldNot(gomega.HaveOccurred())

	// give some time for any events
	utils.Sleep(1)
	listener.waitFor(0, 3*time.Second)
	listener2.waitFor(expected.total(), 3*time.Second)

	noEvents := ExpectedEvents[string, string]{
		inserts: []*ValidateEvent[string, string]{},
		updates: []*ValidateEvent[string, string]{},
		deletes: []*ValidateEvent[string, string]{},
	}

	noEvents.validate(g, namedMap.Name(), listener)
	expected.validate(g, namedMap.Name(), listener2)

	// remove the remaining listener and trigger some events.  Ensure no events captured.
	listener.reset()
	listener2.reset()
	err10 := namedMap.RemoveListener(ctx, listener2.listener)
	g.Expect(err10).ShouldNot(gomega.HaveOccurred())

	_, err11 := namedMap.Put(ctx, "A", "A")
	g.Expect(err11).ShouldNot(gomega.HaveOccurred())

	_, err12 := namedMap.Put(ctx, "A", "B")
	g.Expect(err12).ShouldNot(gomega.HaveOccurred())

	_, err13 := namedMap.Remove(ctx, "A")
	g.Expect(err13).ShouldNot(gomega.HaveOccurred())

	// give some time for any events
	utils.Sleep(1)
	listener2.waitFor(0, 3*time.Second)
	listener2.waitFor(0, 3*time.Second)

	noEvents.validate(g, namedMap.Name(), listener)
	noEvents.validate(g, namedMap.Name(), listener2)
}

func TestCustomFilterListener(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx = context.Background()
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	cache := GetNamedCache[string, utils.Person](g, session, "event-filter-listener")
	defer func(cache coherence.NamedCache[string, utils.Person], ctx context.Context) {
		err := cache.Destroy(ctx)
		if err != nil {
			log.Printf("Error destroying map %s: %s", cache.Name(), err)
		}
	}(cache, ctx)

	noEvents := ExpectedEvents[string, utils.Person]{
		inserts: []*ValidateEvent[string, utils.Person]{},
		updates: []*ValidateEvent[string, utils.Person]{},
		deletes: []*ValidateEvent[string, utils.Person]{},
	}

	keyC := "C"

	expected := ExpectedEvents[string, utils.Person]{
		inserts: []*ValidateEvent[string, utils.Person]{
			{
				eventType: coherence.EntryInserted,
				key:       &keyC,
				old:       nil,
				new:       &charlie,
			},
		},
		updates: []*ValidateEvent[string, utils.Person]{},
		deletes: []*ValidateEvent[string, utils.Person]{},
	}

	f := filters.Less(extractors.Extract[int]("age"), 30)
	listener := NewCountingMapListener[string, utils.Person]("custom-filter")
	err = cache.AddFilterListener(ctx, listener.listener, f)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	_, err = cache.Put(ctx, "A", alvin)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)
	noEvents.validate(g, cache.Name(), listener)

	_, err = cache.Put(ctx, "B", bill)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)
	noEvents.validate(g, cache.Name(), listener)

	_, err = cache.Put(ctx, "C", charlie)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)
	expected.validate(g, cache.Name(), listener)
}

func TestKeyListener(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx = context.Background()
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	cache := GetNamedCache[string, utils.Person](g, session, "event-key-listener")
	defer func(cache coherence.NamedCache[string, utils.Person], ctx context.Context) {
		err := cache.Destroy(ctx)
		if err != nil {
			log.Printf("Error destroying map %s: %s", cache.Name(), err)
		}
	}(cache, ctx)

	noEvents := ExpectedEvents[string, utils.Person]{
		inserts: []*ValidateEvent[string, utils.Person]{},
		updates: []*ValidateEvent[string, utils.Person]{},
		deletes: []*ValidateEvent[string, utils.Person]{},
	}

	keyC := "C"

	expected := ExpectedEvents[string, utils.Person]{
		inserts: []*ValidateEvent[string, utils.Person]{
			{
				eventType: coherence.EntryInserted,
				key:       &keyC,
				old:       nil,
				new:       &charlie,
			},
		},
		updates: []*ValidateEvent[string, utils.Person]{},
		deletes: []*ValidateEvent[string, utils.Person]{},
	}

	listener := NewCountingMapListener[string, utils.Person]("key-listener")
	err = cache.AddKeyListener(ctx, listener.listener, keyC)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	_, err = cache.Put(ctx, "A", alvin)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)
	noEvents.validate(g, cache.Name(), listener)

	_, err = cache.Put(ctx, "B", bill)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)
	noEvents.validate(g, cache.Name(), listener)

	_, err = cache.Put(ctx, "C", charlie)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)
	expected.validate(g, cache.Name(), listener)
}

func TestLiteListeners(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx = context.Background()
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	cache := GetNamedCache[string, utils.Person](g, session, "event-lite-listener")
	defer func(cache coherence.NamedCache[string, utils.Person], ctx context.Context) {
		err := cache.Destroy(ctx)
		if err != nil {
			log.Printf("Error destroying map %s: %s", cache.Name(), err)
		}
	}(cache, ctx)

	keyC := "C"
	always := filters.Always()

	expectedLite := ExpectedEvents[string, utils.Person]{
		inserts: []*ValidateEvent[string, utils.Person]{
			{
				eventType: coherence.EntryInserted,
				key:       &keyC,
				old:       nil,
				new:       nil,
			},
		},
		updates: []*ValidateEvent[string, utils.Person]{},
		deletes: []*ValidateEvent[string, utils.Person]{},
		lite:    true,
	}

	expectedNonLite := ExpectedEvents[string, utils.Person]{
		inserts: []*ValidateEvent[string, utils.Person]{
			{
				eventType: coherence.EntryInserted,
				key:       &keyC,
				old:       nil,
				new:       &charlie,
			},
		},
		updates: []*ValidateEvent[string, utils.Person]{},
		deletes: []*ValidateEvent[string, utils.Person]{},
	}

	keyListener := NewCountingMapListener[string, utils.Person]("key")
	filterListener := NewCountingMapListener[string, utils.Person]("filter")
	keyListenerLite := NewCountingMapListener[string, utils.Person]("lite-key")
	filterListenerLite := NewCountingMapListener[string, utils.Person]("lite-filter")

	err1 := cache.AddKeyListenerLite(ctx, keyListenerLite.listener, keyC)
	g.Expect(err1).ShouldNot(gomega.HaveOccurred())

	err2 := cache.AddFilterListenerLite(ctx, filterListenerLite.listener, always)
	g.Expect(err2).ShouldNot(gomega.HaveOccurred())

	_, err3 := cache.Put(ctx, keyC, charlie)
	g.Expect(err3).ShouldNot(gomega.HaveOccurred())

	keyListenerLite.waitFor(expectedLite.total(), 3*time.Second)
	expectedLite.validate(g, cache.Name(), keyListenerLite)

	filterListenerLite.waitFor(expectedLite.total(), 3*time.Second)
	expectedLite.validate(g, cache.Name(), filterListenerLite)

	err4 := cache.Truncate(ctx)
	g.Expect(err4).ShouldNot(gomega.HaveOccurred())
	utils.Sleep(1)
	keyListenerLite.reset()
	filterListenerLite.reset()

	// adding non-lite listeners for same key and filter values
	// should result in non-lite events being returned.
	// From the Coherence docs:
	// Note:
	// Obviously, a lite event's old value and new value may be null.
	// However, even if you request lite events, the old and the new value
	// may be included if there is no additional cost to generate and deliver
	// the event. In other words, requesting that a MapListener receive lite
	// events is simply a hint to the system that the MapListener does
	// not have to know the old and new values for the event.
	err5 := cache.AddKeyListener(ctx, keyListener.listener, keyC)
	g.Expect(err5).ShouldNot(gomega.HaveOccurred())

	err6 := cache.AddFilterListener(ctx, filterListener.listener, always)
	g.Expect(err6).ShouldNot(gomega.HaveOccurred())

	_, err7 := cache.Put(ctx, keyC, charlie)
	g.Expect(err7).ShouldNot(gomega.HaveOccurred())

	keyListenerLite.waitFor(expectedNonLite.total(), 3*time.Second)
	expectedNonLite.validate(g, cache.Name(), keyListenerLite)

	filterListenerLite.waitFor(expectedNonLite.total(), 3*time.Second)
	expectedNonLite.validate(g, cache.Name(), filterListenerLite)

	keyListenerLite.waitFor(expectedNonLite.total(), 3*time.Second)
	expectedNonLite.validate(g, cache.Name(), keyListener)

	filterListenerLite.waitFor(expectedNonLite.total(), 3*time.Second)
	expectedNonLite.validate(g, cache.Name(), filterListener)

	err8 := cache.Truncate(ctx)
	g.Expect(err8).ShouldNot(gomega.HaveOccurred())

	keyListener.reset()
	keyListenerLite.reset()
	filterListener.reset()
	filterListenerLite.reset()

	err9 := cache.RemoveKeyListener(ctx, keyListener.listener, keyC)
	g.Expect(err9).ShouldNot(gomega.HaveOccurred())

	err10 := cache.RemoveFilterListener(ctx, filterListener.listener, always)
	g.Expect(err10).ShouldNot(gomega.HaveOccurred())

	_, err11 := cache.Put(ctx, keyC, charlie)
	g.Expect(err11).ShouldNot(gomega.HaveOccurred())

	keyListenerLite.waitFor(expectedLite.total(), 3*time.Second)
	expectedLite.validate(g, cache.Name(), keyListenerLite)

	filterListenerLite.waitFor(expectedLite.total(), 3*time.Second)
	expectedLite.validate(g, cache.Name(), filterListenerLite)

	// wait for a few seconds to ensure events didn't come in on the other listeners
	utils.Sleep(3)

	g.Expect(keyListener.counter).Should(gomega.Equal(int32(0)))
	g.Expect(filterListener.counter).Should(gomega.Equal(int32(0)))

	err12 := cache.Truncate(ctx)
	g.Expect(err12).ShouldNot(gomega.HaveOccurred())
	utils.Sleep(1)
	keyListener.reset()
	keyListenerLite.reset()
	filterListener.reset()
	filterListenerLite.reset()

	err13 := cache.RemoveKeyListener(ctx, keyListenerLite.listener, keyC)
	g.Expect(err13).ShouldNot(gomega.HaveOccurred())

	err14 := cache.RemoveFilterListener(ctx, filterListenerLite.listener, always)
	g.Expect(err14).ShouldNot(gomega.HaveOccurred())

	_, err15 := cache.Put(ctx, keyC, charlie)
	g.Expect(err15).ShouldNot(gomega.HaveOccurred())

	// wait for a few seconds to ensure events didn't come in on the other listeners
	utils.Sleep(3)
	g.Expect(keyListenerLite.counter).Should(gomega.Equal(int32(0)))
	g.Expect(filterListenerLite.counter).Should(gomega.Equal(int32(0)))
}

// initTest initializes a test and returns a gomega.WithT nad coherence.Session
func initTest(t *testing.T, options ...func(session *coherence.SessionOptions)) (*gomega.WithT, *coherence.Session) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession(options...)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	return g, session
}

func checkEvents[K comparable, V any](
	g *gomega.WithT,
	expectedCacheName string,
	expected []*ValidateEvent[K, V],
	actual []coherence.MapEvent[K, V],
	isLite bool) {

	var last coherence.NamedMap[K, V]
	// first, check we have the same NamedCache reference
	// for all received events
	for _, e := range actual {
		if last == nil {
			last = e.Source()
		} else {
			g.Expect(last).To(gomega.Equal(e.Source()))
			last = e.Source()
		}
	}

	// now validate the content of the events
	for idx, exp := range expected {
		actualEvent := actual[idx]
		key, err1 := actualEvent.Key()
		g.Expect(err1).ShouldNot(gomega.HaveOccurred())

		old, err2 := actualEvent.OldValue()
		g.Expect(err2).ShouldNot(gomega.HaveOccurred())

		newValue, err3 := actualEvent.NewValue()
		g.Expect(err3).ShouldNot(gomega.HaveOccurred())

		g.Expect(key).To(gomega.Equal(exp.key))

		// lite may or may not return the values
		if !isLite {
			g.Expect(old).To(gomega.Equal(exp.old))
			g.Expect(newValue).To(gomega.Equal(exp.new))
		}

		g.Expect(actualEvent.Type()).To(gomega.Equal(exp.eventType))
		g.Expect(actualEvent.Source().Name()).To(gomega.Equal(expectedCacheName))
	}
}

func runBasicTests(
	g *gomega.WithT,
	cache coherence.NamedMap[string, string],
	cacheName string,
	expected *ExpectedEvents[string, string],
	filterMask filters.MapEventMask) {

	defer func(cache coherence.NamedMap[string, string], ctx context.Context) {
		log.Println("Truncate cache", cache.Name())
		err := cache.Truncate(ctx)
		if err != nil {
			log.Printf("Error destroying map %s: %s", cache.Name(), err)
		}
	}(cache, ctx)

	listener := NewCountingMapListener[string, string]("basic")

	if filterMask == -1 {
		err := cache.AddListener(ctx, listener.listener)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	} else {
		var thisFilter filters.Filter = filters.NewEventFilterFromMask(filterMask)
		err := cache.AddFilterListener(ctx, listener.listener, thisFilter)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	defer func(cache coherence.NamedMap[string, string], ctx context.Context) {
		_ = cache.RemoveListener(ctx, listener.listener)
	}(cache, ctx)

	log.Println("Waiting for event registrations")
	time.Sleep(time.Duration(5) * time.Second)

	_, err2 := cache.Put(ctx, "A", "A")
	g.Expect(err2).ShouldNot(gomega.HaveOccurred())

	_, err3 := cache.Put(ctx, "A", "B")
	g.Expect(err3).ShouldNot(gomega.HaveOccurred())

	_, err4 := cache.Remove(ctx, "A")
	g.Expect(err4).ShouldNot(gomega.HaveOccurred())

	completed := listener.waitFor(expected.total(), 5*time.Second)
	g.Expect(completed).Should(gomega.BeTrue())
	expected.validate(g, cacheName, listener)
}

func runBasicLifecycleTests(g *gomega.WithT, cache coherence.NamedMap[string, string], cacheName string) {
	defer func(cache coherence.NamedMap[string, string], ctx context.Context) {
		err := cache.Destroy(ctx)
		if err != nil && err != coherence.ErrDestroyed {
			log.Printf("Error destroying map %s: %s", cache.Name(), err)
		}
	}(cache, ctx)

	listener := NewCountingLifecycleListener[string, string](cacheName)

	cache.AddLifecycleListener(listener.listener)

	defer cache.RemoveLifecycleListener(listener.listener)

	time.Sleep(time.Duration(5) * time.Second)

	_, err := cache.Put(ctx, "A", "A")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err := cache.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(1))

	// issue truncate
	log.Println("Issue first truncate")
	err = cache.Truncate(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	time.Sleep(time.Duration(5) * time.Second)

	// issue truncate again
	log.Println("Issue second truncate")
	err = cache.Truncate(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// function to return the truncate count from the listener
	f := func() int32 { return listener.truncateCount() }

	g.Expect(expect[int32](f, 2, timeout)).To(gomega.BeNil())

	// destroy the cache
	err = cache.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func runMultipleLifecycleTests(g *gomega.WithT, cache coherence.NamedMap[string, string]) {
	listener1 := NewCountingLifecycleListener[string, string]("listener1")
	listener2 := NewCountingLifecycleListener[string, string]("listener2")

	cache.AddLifecycleListener(listener1.listener)
	cache.AddLifecycleListener(listener2.listener)

	defer cache.RemoveLifecycleListener(listener1.listener)
	defer cache.RemoveLifecycleListener(listener2.listener)

	utils.Sleep(10)

	_, err := cache.Put(ctx, "A", "A")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	log.Println("Truncate - 1", cache.Name())
	err = cache.Truncate(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	time.Sleep(time.Duration(5) * time.Second)

	log.Println("Truncate - 2", cache.Name())
	err = cache.Truncate(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	//time.Sleep(time.Duration(5) * time.Second)

	// each of the listeners should receive 2 events
	g.Eventually(func() int32 {
		return listener1.truncateCount()
	}).Should(gomega.Equal(int32(2)))

	g.Eventually(func() int32 {
		return listener2.truncateCount()
	}).Should(gomega.Equal(int32(2)))

	// unregister the second listener
	cache.RemoveLifecycleListener(listener2.listener)

	time.Sleep(time.Duration(5) * time.Second)

	log.Println("Truncate - 3", cache.Name())
	// issue another truncate, listener2 should not receive the event
	err = cache.Truncate(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	//time.Sleep(time.Duration(5) * time.Second)

	// listener1 should receive the event, but listener2 should not
	g.Eventually(func() int32 {
		return listener1.truncateCount()
	}).Should(gomega.Equal(int32(3)), "truncateCount to equal 3")

	g.Eventually(func() int32 {
		return listener2.truncateCount()
	}).Should(gomega.Equal(int32(2)), "truncateCount to equal 2")

	log.Println("Destroy", cache.Name())
	// destroy the cache
	err = cache.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	//time.Sleep(time.Duration(5) * time.Second)

	g.Eventually(func() int32 {
		return listener1.releaseCount()
	}).Should(gomega.Equal(int32(1)), "truncateCount to equal 1")
}

func runTestExpiringEvents(g *gomega.WithT, cache coherence.NamedCache[string, string]) {
	listener1 := NewExpiringMapListener[string, string]("listener1")

	err := cache.AddListener(ctx, listener1.listener)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	defer func() {
		_ = cache.RemoveListener(ctx, listener1.listener)
	}()

	// put some values that will expire in 5 seconds and we shoudl receive the events
	_, err = cache.PutWithExpiry(ctx, "A", "A", time.Duration(5)*time.Second)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	_, err = cache.PutWithExpiry(ctx, "B", "B", time.Duration(5)*time.Second)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	utils.Sleep(10)

	expectedEvents := 0 // default for gRPC v0
	if cache.GetSession().GetProtocolVersion() > 0 {
		expectedEvents = 2
	}

	//nolint:gosec
	g.Eventually(func() int32 {
		return listener1.getExpireCount()
	}).Should(gomega.Equal(int32(expectedEvents)))

	//nolint:gosec
	g.Eventually(func() int32 {
		return listener1.getSyntheticCount()
	}).Should(gomega.Equal(int32(expectedEvents)))
}

func runReleasedLifecycleTests(g *gomega.WithT, cache coherence.NamedMap[string, string]) {
	listener1 := NewCountingLifecycleListener[string, string]("listener1")
	listener2 := NewCountingLifecycleListener[string, string]("listener2")

	cache.AddLifecycleListener(listener1.listener)
	cache.AddLifecycleListener(listener2.listener)

	defer cache.RemoveLifecycleListener(listener1.listener)
	defer cache.RemoveLifecycleListener(listener2.listener)

	_, err := cache.Put(ctx, "A", "A")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// issue release
	cache.Release()

	g.Eventually(func() int32 {
		return listener1.releaseCount()
	}).Should(gomega.Equal(int32(1)), "releaseCount to equal 1")

	g.Eventually(func() int32 {
		return listener2.releaseCount()
	}).Should(gomega.Equal(int32(1)), "releaseCount to equal 1")
}

type ValidateEvent[K comparable, V any] struct {
	eventType coherence.MapEventType
	key       *K
	old       *V
	new       *V
}

type ExpectedEvents[K comparable, V any] struct {
	inserts []*ValidateEvent[K, V]
	updates []*ValidateEvent[K, V]
	deletes []*ValidateEvent[K, V]
	lite    bool
}

func (ee *ExpectedEvents[K, V]) total() int32 {
	// nolint:gosec
	return int32(len(ee.inserts) + len(ee.updates) + len(ee.deletes))
}

func (ee *ExpectedEvents[K, V]) insertCount() int {
	return len(ee.inserts)
}

func (ee *ExpectedEvents[K, V]) updateCount() int {
	return len(ee.updates)
}

func (ee *ExpectedEvents[K, V]) deleteCount() int {
	return len(ee.deletes)
}

func (ee *ExpectedEvents[K, V]) validate(g *gomega.WithT, cacheName string, listener *CountingMapListener[K, V]) {
	log.Printf("[%s] Beginning validation of events captured by listener", listener.name)
	g.Expect(listener.counter).To(gomega.Equal(ee.total()))
	g.Expect(listener.insertCount()).To(gomega.Equal(ee.insertCount()))
	g.Expect(listener.updateCount()).To(gomega.Equal(ee.updateCount()))
	g.Expect(listener.deleteCount()).To(gomega.Equal(ee.deleteCount()))

	checkEvents(g, cacheName, ee.inserts, listener.inserted, ee.lite)
	checkEvents(g, cacheName, ee.updates, listener.updated, ee.lite)
	checkEvents(g, cacheName, ee.deletes, listener.deleted, ee.lite)

	orderExpected := append(ee.inserts, append(ee.updates, ee.deletes...)...)
	checkEvents(g, cacheName, orderExpected, listener.order, ee.lite)
}

type CountingLifecycleListener[K comparable, V any] struct {
	listener   coherence.MapLifecycleListener[K, V]
	name       string
	truncCount int32
	destCount  int32
	relCount   int32
}

func (cll *CountingLifecycleListener[K, V]) truncateCount() int32 {
	return cll.truncCount
}

func (cll *CountingLifecycleListener[K, V]) releaseCount() int32 {
	return cll.relCount
}

func NewCountingLifecycleListener[K comparable, V any](name string) *CountingLifecycleListener[K, V] {
	countingListener := CountingLifecycleListener[K, V]{
		name:     name,
		listener: coherence.NewMapLifecycleListener[K, V](),
	}

	countingListener.listener.OnTruncated(func(_ coherence.MapLifecycleEvent[K, V]) {
		atomic.AddInt32(&countingListener.truncCount, 1)
		log.Printf("%s: Truncated, count=%d", name, countingListener.truncCount)
	}).OnDestroyed(func(_ coherence.MapLifecycleEvent[K, V]) {
		atomic.AddInt32(&countingListener.destCount, 1)
	}).OnReleased(func(_ coherence.MapLifecycleEvent[K, V]) {
		atomic.AddInt32(&countingListener.relCount, 1)
	}).OnAny(func(e coherence.MapLifecycleEvent[K, V]) {
		log.Printf("[%s] Received event -> %+v", name, e)
	})

	return &countingListener
}

type CountingMapListener[K comparable, V any] struct {
	listener coherence.MapListener[K, V]
	name     string
	counter  int32
	inserted []coherence.MapEvent[K, V]
	updated  []coherence.MapEvent[K, V]
	deleted  []coherence.MapEvent[K, V]
	order    []coherence.MapEvent[K, V]
	waiter   sync.WaitGroup
	mutex    sync.Mutex
}

func (cl *CountingMapListener[K, V]) waitFor(eventCount int32, timeout time.Duration) bool {
	log.Printf("[%s] Waiting for %d event(s) to be received within %s", cl.name, eventCount, timeout)
	tout := time.After(timeout)
	tick := time.Tick(50 * time.Millisecond)
	cl.waiter.Add(1)
	go func() {
		cl.waiter.Wait()
	}()
	for {
		select {
		case <-tout:
			log.Printf("[%s] Timed out waiting for expected number of events; Received %d events", cl.name, cl.counter)
			return false
		case <-tick:
			if cl.counter >= eventCount {
				log.Printf("[%s] Received expected number of events; returning", cl.name)
				cl.waiter.Done()
				return true
			}
			log.Printf("[%s] tick: recevied %d events", cl.name, cl.counter)
		}
	}
}

func (cl *CountingMapListener[K, V]) reset() {
	cl.inserted = []coherence.MapEvent[K, V]{}
	cl.deleted = []coherence.MapEvent[K, V]{}
	cl.updated = []coherence.MapEvent[K, V]{}
	cl.order = []coherence.MapEvent[K, V]{}
	cl.counter = 0
	cl.waiter = sync.WaitGroup{}

	cl.waiter.Add(1)
	log.Printf("[%s] Listener reset", cl.name)
}

func (cl *CountingMapListener[K, V]) insertCount() int {
	return len(cl.inserted)
}

func (cl *CountingMapListener[K, V]) updateCount() int {
	return len(cl.updated)
}

func (cl *CountingMapListener[K, V]) deleteCount() int {
	return len(cl.deleted)
}

func NewCountingMapListener[K comparable, V any](name string) *CountingMapListener[K, V] {
	countingListener := CountingMapListener[K, V]{
		name:     name,
		counter:  0,
		inserted: []coherence.MapEvent[K, V]{},
		updated:  []coherence.MapEvent[K, V]{},
		deleted:  []coherence.MapEvent[K, V]{},
		order:    []coherence.MapEvent[K, V]{},
		waiter:   sync.WaitGroup{},
		mutex:    sync.Mutex{},
		listener: coherence.NewMapListener[K, V](),
	}
	countingListener.waiter.Add(1)

	countingListener.listener.OnInserted(func(e coherence.MapEvent[K, V]) {
		atomic.AddInt32(&countingListener.counter, 1)
		countingListener.mutex.Lock()
		defer countingListener.mutex.Unlock()
		countingListener.order = append(countingListener.order, e)
		countingListener.inserted = append(countingListener.inserted, e)
	}).OnDeleted(func(e coherence.MapEvent[K, V]) {
		atomic.AddInt32(&countingListener.counter, 1)
		countingListener.mutex.Lock()
		defer countingListener.mutex.Unlock()
		countingListener.order = append(countingListener.order, e)
		countingListener.deleted = append(countingListener.deleted, e)
	}).OnUpdated(func(e coherence.MapEvent[K, V]) {
		atomic.AddInt32(&countingListener.counter, 1)
		countingListener.mutex.Lock()
		defer countingListener.mutex.Unlock()
		countingListener.order = append(countingListener.order, e)
		countingListener.updated = append(countingListener.updated, e)
	}).OnAny(func(e coherence.MapEvent[K, V]) {
		log.Printf("[%s] Received event -> %+v", name, e)
	})

	return &countingListener
}

type ReconnectMapListener[K comparable, V any] struct {
	name        string
	insertCount int32
	deleteCount int32
	updateCount int32
	listener    coherence.MapListener[K, V]
}

func (rcl *ReconnectMapListener[K, V]) insertedCount() int32 {
	return rcl.insertCount
}

func (rcl *ReconnectMapListener[K, V]) updatedCount() int32 {
	return rcl.updateCount
}

func (rcl *ReconnectMapListener[K, V]) deletedCount() int32 {
	return rcl.deleteCount
}

func NewReconnectMapListener[K comparable, V any](name string) *ReconnectMapListener[K, V] {
	reconnectingListener := ReconnectMapListener[K, V]{
		name:     name,
		listener: coherence.NewMapListener[K, V](),
	}

	reconnectingListener.listener.OnInserted(func(_ coherence.MapEvent[K, V]) {
		atomic.AddInt32(&reconnectingListener.insertCount, 1)
	}).OnDeleted(func(_ coherence.MapEvent[K, V]) {
		atomic.AddInt32(&reconnectingListener.deleteCount, 1)
	}).OnUpdated(func(_ coherence.MapEvent[K, V]) {
		atomic.AddInt32(&reconnectingListener.updateCount, 1)
	})

	return &reconnectingListener
}

// expect checks the result of a function call is the value within a time duration
func expect[T comparable](f func() T, expectedValue T, timeout int) error {
	var (
		duration  = 0
		sleepTime = 1
		lastValue T
	)
	for duration < timeout {
		lastValue = f()
		log.Printf("Last value: %v, expected value: %v", lastValue, expectedValue)
		if lastValue == expectedValue {
			return nil
		}
		utils.Sleep(sleepTime)
		duration += sleepTime
		if duration > 10 {
			// back off
			sleepTime = 2
		}
	}
	return fmt.Errorf("expected value of %v was not reached after %d seconds. Last value was %v", expectedValue, timeout, lastValue)
}

type ExpiringMapListener[K comparable, V any] struct {
	listener       coherence.MapListener[K, V]
	name           string
	expireCount    int32
	syntheticCount int32
	deleteCount    int32
}

func (el *ExpiringMapListener[K, V]) getExpireCount() int32 {
	return el.expireCount
}

func (el *ExpiringMapListener[K, V]) getSyntheticCount() int32 {
	return el.syntheticCount
}

func NewExpiringMapListener[K comparable, V any](name string) *ExpiringMapListener[K, V] {
	expiringListener := ExpiringMapListener[K, V]{
		name:     name,
		listener: coherence.NewMapListener[K, V](),
	}

	expiringListener.listener.OnDeleted(func(e coherence.MapEvent[K, V]) {
		atomic.AddInt32(&expiringListener.deleteCount, 1)
		expiring, err := e.IsExpired()
		if err == nil && expiring {
			// only record if the error is nil, which means its gRPC v1 and its expiry
			atomic.AddInt32(&expiringListener.expireCount, 1)
		}
		synthetic, err := e.IsSynthetic()
		if err == nil && synthetic {
			// only record if the error is nil, which means its gRPC v1 and its synthetic
			atomic.AddInt32(&expiringListener.syntheticCount, 1)
		}
	})
	return &expiringListener
}
