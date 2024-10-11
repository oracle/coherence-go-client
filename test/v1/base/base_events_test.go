/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package base

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/filters"
	pb1 "github.com/oracle/coherence-go-client/proto/v1"
	"github.com/oracle/coherence-go-client/test/utils"
	"sync/atomic"
	"testing"
)

// TestMapEvents tests basic map events functions for key listeners.
func TestMapEventsKeyListenerBase(t *testing.T) {
	var (
		g     = gomega.NewWithT(t)
		ctx   = context.Background()
		cache = "test-events-1"
		err   error
	)

	session := getTestSession(t, g)
	defer session.Close()

	_ = ensureCache(g, session, cache)

	// create key and value
	key := ensureSerializedInt32(g, 32)

	// clear the cache
	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	keyOrFilterKey := &pb1.KeyOrFilter_Key{Key: key}
	keyOrFilter := &pb1.KeyOrFilter{KeyOrFilter: keyOrFilterKey}

	// create a Key listener
	err = coherence.TestMapListenerRequest(ctx, session, cache, true /* subscribe */, keyOrFilter,
		false, /* lite */
		false, /* sync */
		false /* priming */, 0)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	// we should have one key listener

	err = coherence.TestMapListenerRequest(ctx, session, cache, false /* subscribe */, keyOrFilter,
		false, /* lite */
		false, /* sync */
		false /* priming */, 0)

	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	// should have unsubscribed
}

func DoTestMapEventsKeyListener(t *testing.T, g *gomega.WithT, cache string, lite bool) {
	ctx := context.Background()

	session := getTestSession(t, g)
	defer session.Close()

	namedMap, err := coherence.GetNamedMap[int, string](session, cache)
	g.Expect(err).Should(gomega.BeNil())

	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	listener := NewTestMapListener[int, string]("test1")
	err = namedMap.AddKeyListener(ctx, listener.listener, 10)
	g.Expect(err).Should(gomega.BeNil())

	// should have 1 entry for key 10
	validateKeyListenerMapSize[int, string](g, namedMap, 1)

	// should have 1 map listener
	validateKeyMapListenerSize[int, string](g, namedMap, 10, 1)

	// put a value, should receive an update event
	_, err = namedMap.Put(ctx, 10, "value-10")
	g.Expect(err).Should(gomega.BeNil())
	g.Eventually(func() int32 {
		return listener.insertCount
	}).Should(gomega.Equal(int32(1)), "insertCount to equal 1")

	// add a new map listener on the same key
	listener2 := NewTestMapListener[int, string]("test2")
	if lite {
		err = namedMap.AddKeyListenerLite(ctx, listener2.listener, 10)
	} else {
		err = namedMap.AddKeyListener(ctx, listener2.listener, 10)
	}
	g.Expect(err).Should(gomega.BeNil())

	// should have an event on both listeners
	_, err = namedMap.Put(ctx, 10, "value-11")
	g.Expect(err).Should(gomega.BeNil())
	g.Eventually(func() int32 {
		return listener.updateCount
	}).Should(gomega.Equal(int32(1)), "updateCount to equal 1")

	g.Eventually(func() int32 {
		return listener2.updateCount
	}).Should(gomega.Equal(int32(1)), "updateCount to equal 1")

	// should have 1 entry for key 10
	validateKeyListenerMapSize[int, string](g, namedMap, 1)
	// should have 2 map listeners on that key
	validateKeyMapListenerSize[int, string](g, namedMap, 10, 2)

	// add a new map listener on a different key
	listener3 := NewTestMapListener[int, string]("test3")
	err = namedMap.AddKeyListener(ctx, listener3.listener, 11)
	g.Expect(err).Should(gomega.BeNil())

	// should have 2 keys
	validateKeyListenerMapSize[int, string](g, namedMap, 2)
	// should have 2 map listeners on that key
	validateKeyMapListenerSize[int, string](g, namedMap, 11, 1)

	err = namedMap.RemoveKeyListener(ctx, listener.listener, 10)
	g.Expect(err).Should(gomega.BeNil())

	err = namedMap.RemoveKeyListener(ctx, listener2.listener, 10)
	g.Expect(err).Should(gomega.BeNil())

	err = namedMap.RemoveKeyListener(ctx, listener3.listener, 11)
	g.Expect(err).Should(gomega.BeNil())

	validateKeyMapListenerSize[int, string](g, namedMap, 10, 0)
	validateKeyMapListenerSize[int, string](g, namedMap, 11, 0)
}

func DoTestMapEventsListenerValue(t *testing.T, g *gomega.WithT, cache string, keyFilter bool, f filters.Filter, lite bool) {
	ctx := context.Background()

	session := getTestSession(t, g)
	defer session.Close()

	namedMap, err := coherence.GetNamedMap[int, string](session, cache)
	g.Expect(err).Should(gomega.BeNil())

	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	listener := NewTestMapListenerValues[int, string]("test1")
	if keyFilter {
		if lite {
			err = namedMap.AddKeyListenerLite(ctx, listener.listener, 10)
		} else {
			err = namedMap.AddKeyListener(ctx, listener.listener, 10)
		}
	} else {
		if lite {
			err = namedMap.AddFilterListenerLite(ctx, listener.listener, f)
		} else {
			err = namedMap.AddFilterListener(ctx, listener.listener, f)
		}
	}
	g.Expect(err).Should(gomega.BeNil())

	// put a value, should receive an update event with the new value correct
	_, err = namedMap.Put(ctx, 10, "value-10")
	g.Expect(err).Should(gomega.BeNil())
	if !lite {
		g.Eventually(func() string {
			return *listener.newValue
		}).Should(gomega.Equal("value-10"))
	} else {
		// lite so may not get the value back
		g.Eventually(func() bool {
			return listener.newValue == nil || *listener.newValue == "value-10"
		}).Should(gomega.BeTrue())
	}

	// put a new value for key 10
	_, err = namedMap.Put(ctx, 10, "value-10-new")
	g.Expect(err).Should(gomega.BeNil())
	if !lite {
		g.Eventually(func() string {
			return *listener.newValue
		}).Should(gomega.Equal("value-10-new"))
	} else {
		// lite so may not get the value back
		g.Eventually(func() bool {
			return listener.newValue == nil || *listener.newValue == "value-10-new"
		}).Should(gomega.BeTrue())
	}

	if !lite {
		g.Eventually(func() string {
			return *listener.oldValue
		}).Should(gomega.Equal("value-10"))
	} else {
		// lite so may not get the value back
		g.Eventually(func() bool {
			return listener.oldValue == nil || *listener.oldValue == "value-10"
		}).Should(gomega.BeTrue())
	}

	// remove the entry for key 10
	_, err = namedMap.Remove(ctx, 10)
	g.Expect(err).Should(gomega.BeNil())
	if !lite {
		g.Eventually(func() string {
			return *listener.oldValue
		}).Should(gomega.Equal("value-10-new"))
	} else {
		// lite so may not get the value back
		g.Eventually(func() bool {
			return listener.newValue == nil || *listener.newValue == "value-10-new"
		}).Should(gomega.BeTrue())
	}

	g.Eventually(func() *string {
		return listener.newValue
	}).Should(gomega.BeNil())

	// remove the map listener, and we should not receive any events
	atomic.StoreInt32(&listener.eventCount, 0)

	if keyFilter {
		err = namedMap.RemoveKeyListener(ctx, listener.listener, 10)
	} else {
		err = namedMap.RemoveFilterListener(ctx, listener.listener, f)
	}
	g.Expect(err).Should(gomega.BeNil())
	utils.Sleep(5)

	// put a value, should receive an update event with the new value correct
	_, err = namedMap.Put(ctx, 10, "value-10")
	g.Expect(err).Should(gomega.BeNil())

	t.Log("Waiting for 10 seconds")

	// wait some time to ensure if we receive event is not wrong
	utils.Sleep(10)
	g.Eventually(func() int32 {
		return listener.eventCount
	}).Should(gomega.Equal(int32(0)))
}

func DoTestMapEventsFilterListener(t *testing.T, g *gomega.WithT, cache string, lite bool) {
	ctx := context.Background()

	session := getTestSession(t, g)
	defer session.Close()

	namedCache, err := coherence.GetNamedCache[int, string](session, cache)
	g.Expect(err).Should(gomega.BeNil())

	err = coherence.TestClearCache(ctx, session, cache)
	g.Expect(err).Should(gomega.BeNil())

	listener := NewTestMapListener[int, string]("test1")
	f := filters.Always()
	f2 := filters.Never()
	err = namedCache.AddFilterListener(ctx, listener.listener, f)
	g.Expect(err).Should(gomega.BeNil())

	// should have 1 entry for filter f
	validateFilterListenerMapSize[int, string](g, namedCache, 2)

	// should have 1 map listener
	validateFilterMapListenerSize[int, string](g, namedCache, f, 1)

	// put a value, should receive an update event
	_, err = namedCache.Put(ctx, 10, "value-10")
	g.Expect(err).Should(gomega.BeNil())
	g.Eventually(func() int32 {
		return listener.insertCount
	}).Should(gomega.Equal(int32(1)), "insertCount to equal 1")

	// add a new map listener on the same filter
	listener2 := NewTestMapListener[int, string]("test2")
	if lite {
		err = namedCache.AddFilterListenerLite(ctx, listener2.listener, f)
	} else {
		err = namedCache.AddFilterListener(ctx, listener2.listener, f)
	}
	g.Expect(err).Should(gomega.BeNil())

	// should have 1 entry for filter f
	validateFilterListenerMapSize[int, string](g, namedCache, 2)
	// should have 2 map listeners on same filter
	validateFilterMapListenerSize[int, string](g, namedCache, f, 2)

	// add a new map listener on a different filter
	listener3 := NewTestMapListener[int, string]("test3")
	err = namedCache.AddFilterListener(ctx, listener3.listener, f2)
	g.Expect(err).Should(gomega.BeNil())

	// should have 2 entries
	validateFilterListenerMapSize[int, string](g, namedCache, 4)
	// should have 2 map listeners on that key
	validateFilterMapListenerSize[int, string](g, namedCache, f, 2)

	err = namedCache.RemoveFilterListener(ctx, listener.listener, f)
	g.Expect(err).Should(gomega.BeNil())

	err = namedCache.RemoveFilterListener(ctx, listener2.listener, f)
	g.Expect(err).Should(gomega.BeNil())

	err = namedCache.RemoveFilterListener(ctx, listener3.listener, f2)
	g.Expect(err).Should(gomega.BeNil())

	validateFilterMapListenerSize[int, string](g, namedCache, f, 0)
	validateFilterMapListenerSize[int, string](g, namedCache, f2, 0)
}

// TestMapEventsKeyListenerValue tests basic key listeners.
func TestMapEventsListenerValue(t *testing.T) {
	DoTestMapEventsListenerValue(t, gomega.NewWithT(t), "test-key-listeners-value-lite", true, nil, true)
	DoTestMapEventsListenerValue(t, gomega.NewWithT(t), "test-key-listeners-value", true, nil, false)
	DoTestMapEventsListenerValue(t, gomega.NewWithT(t), "test-filter-listeners-value-lite", false, filters.Always(), true)
	DoTestMapEventsListenerValue(t, gomega.NewWithT(t), "test-filter-listeners-value", false, filters.Always(), false)
}

// TestMapEventsKeyListenerLite tests basic key listeners.
func TestMapEventsKeyListenerLite(t *testing.T) {
	DoTestMapEventsKeyListener(t, gomega.NewWithT(t), "test-key-listeners-lite", true)
}

// TestMapEventsKeyListenerLite tests basic key listeners.
func TestMapEventsKeyListener(t *testing.T) {
	DoTestMapEventsKeyListener(t, gomega.NewWithT(t), "test-key-listeners", false)
}

// TestMapEventsFilterListenerLite tests basic key listeners.
func TestMapEventsFilterListenerLite(t *testing.T) {
	DoTestMapEventsFilterListener(t, gomega.NewWithT(t), "test-filter-listeners-lite", true)
}

// TestMapEventsFilterListener tests basic key listeners.
func TestMapEventsFilterListener(t *testing.T) {
	DoTestMapEventsFilterListener(t, gomega.NewWithT(t), "test-filter-listeners", false)
}

func validateKeyListenerMapSize[K comparable, V any](g *gomega.WithT, namedMap coherence.NamedMap[K, V], size int) {
	keyListenerGroupMap := coherence.GetKeyListenerGroupMap[K, V](namedMap)
	g.Expect(keyListenerGroupMap).Should(gomega.Not(gomega.BeNil()))
	g.Expect(len(keyListenerGroupMap)).To(gomega.Equal(size))
}

func validateKeyMapListenerSize[K comparable, V any](g *gomega.WithT, namedMap coherence.NamedMap[K, V], key K, size int) {
	keyListenerGroupMap := coherence.GetKeyListenerGroupListeners[K, V](namedMap, key)
	g.Expect(keyListenerGroupMap).Should(gomega.Not(gomega.BeNil()))
	g.Expect(len(keyListenerGroupMap)).To(gomega.Equal(size))
}

func validateFilterListenerMapSize[K comparable, V any](g *gomega.WithT, namedMap coherence.NamedMap[K, V], size int) {
	keyListenerGroupMap := coherence.GetFilterListenerGroupMap[K, V](namedMap)
	g.Expect(keyListenerGroupMap).Should(gomega.Not(gomega.BeNil()))
	g.Expect(len(keyListenerGroupMap)).To(gomega.Equal(size))
}

func validateFilterMapListenerSize[K comparable, V any](g *gomega.WithT, namedMap coherence.NamedMap[K, V], f filters.Filter, size int) {
	keyListenerGroupMap := coherence.GetFilterListenerGroupListeners[K, V](namedMap, f)
	g.Expect(keyListenerGroupMap).Should(gomega.Not(gomega.BeNil()))
	g.Expect(len(keyListenerGroupMap)).To(gomega.Equal(size))
}

type TestMapListener[K comparable, V any] struct {
	name        string
	insertCount int32
	deleteCount int32
	updateCount int32
	listener    coherence.MapListener[K, V]
}

func NewTestMapListener[K comparable, V any](name string) *TestMapListener[K, V] {
	reconnectingListener := TestMapListener[K, V]{
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

type TestMapListenerValues[K comparable, V any] struct {
	name       string
	newValue   *V
	oldValue   *V
	listener   coherence.MapListener[K, V]
	eventCount int32
}

func NewTestMapListenerValues[K comparable, V any](name string) *TestMapListenerValues[K, V] {
	testListener := TestMapListenerValues[K, V]{
		name:     name,
		listener: coherence.NewMapListener[K, V](),
	}

	testListener.listener.OnInserted(func(e coherence.MapEvent[K, V]) {
		atomic.AddInt32(&testListener.eventCount, 1)
		testListener.oldValue = nil
		if newValue, err := e.NewValue(); err == nil {
			testListener.newValue = newValue
		}
	}).OnDeleted(func(e coherence.MapEvent[K, V]) {
		atomic.AddInt32(&testListener.eventCount, 1)
		testListener.newValue = nil
		if oldValue, err := e.OldValue(); err == nil {
			testListener.oldValue = oldValue
		}
	}).OnUpdated(func(e coherence.MapEvent[K, V]) {
		atomic.AddInt32(&testListener.eventCount, 1)
		if newValue, err := e.NewValue(); err == nil {
			testListener.newValue = newValue
		}
		if oldValue, err := e.OldValue(); err == nil {
			testListener.oldValue = oldValue
		}
	})

	return &testListener
}
