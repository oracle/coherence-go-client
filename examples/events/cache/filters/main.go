/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main shows how to listen for all events on a NamedMap or NamedCache using filters.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"
	"sync"
	"sync/atomic"
	"time"
)

type Person struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Age    int    `json:"age"`
	Salary int    `json:"salary"`
}

func (p Person) String() string {
	return fmt.Sprintf("Person{id=%d, name=%s, age=%d, salary=%d}", p.ID, p.Name, p.Age, p.Salary)
}

func main() {

	ctx := context.Background()

	// create a new Session
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	// Create a number of listener instances with various filters

	// Add a listener to respond to events where a persons age is < 30
	listenerAgeLess30 := NewCountingEventsListener[int, Person]()

	if err = namedMap.AddFilterListener(ctx, listenerAgeLess30.listener,
		filters.Less(extractors.Extract[int]("age"), 30)); err != nil {
		panic("unable to add listener")
	}
	defer unregisterListener(ctx, namedMap, listenerAgeLess30)

	// Add a listener to respond to events where a persons Name is
	listenerSalaryGreater17000 := NewCountingEventsListener[int, Person]()

	if err = namedMap.AddFilterListener(ctx, listenerSalaryGreater17000.listener,
		filters.Greater(extractors.Extract[int]("salary"), 17000)); err != nil {
		panic("unable to add listener")
	}
	defer unregisterListener(ctx, namedMap, listenerSalaryGreater17000)

	fmt.Println("Adding 10 random people")
	for i := 1; i <= 10; i++ {
		p := Person{ID: i, Name: fmt.Sprintf("Name-%d", i), Age: 26 + i, Salary: 10000 + (i * 1000)}
		_, err = namedMap.Put(ctx, p.ID, p)
		if err != nil {
			panic(err)
		}
	}

	// Display the list of people
	ch := namedMap.EntrySetFilter(ctx, filters.Always())
	for result := range ch {
		if result.Err != nil {
			panic(err)
		}

		fmt.Println("Key:", result.Key, "Value:", result.Value)
	}

	fmt.Println("Update age of person 1 to 54")
	if _, err = coherence.Invoke[int, Person, bool](ctx, namedMap, 1, processors.Update("age", 54)); err != nil {
		panic(err)
	}

	fmt.Println("Remove person 2")
	if _, err = namedMap.Remove(ctx, 2); err != nil {
		panic(err)
	}

	fmt.Println("Ensuring events are delivered")
	time.Sleep(time.Duration(5) * time.Second)
	fmt.Println("listenerAgeLess30:          inserts=", listenerAgeLess30.GetInsertCount(), "updates=", listenerAgeLess30.GetUpdateCount(),
		"deletes=", listenerAgeLess30.GetDeleteCount())
	fmt.Println("listenerSalaryGreater17000: inserts=", listenerSalaryGreater17000.GetInsertCount(), "updates=", listenerSalaryGreater17000.GetUpdateCount(),
		"deletes=", listenerSalaryGreater17000.GetDeleteCount())
}

func unregisterListener(ctx context.Context, namedMap coherence.NamedMap[int, Person], listener *CountingEventsListener[int, Person]) {
	if err := namedMap.RemoveListener(ctx, listener.listener); err != nil {
		panic(fmt.Sprintf("cannot remove listener %v", listener.listener))
	}
}

type CountingEventsListener[K comparable, V any] struct {
	listener    coherence.MapListener[K, V]
	mutex       sync.RWMutex
	insertCount int32
	deleteCount int32
	updateCount int32
}

func (c *CountingEventsListener[K, V]) GetInsertCount() int32 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.insertCount
}

func (c *CountingEventsListener[K, V]) GetDeleteCount() int32 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.deleteCount
}

func (c *CountingEventsListener[K, V]) GetUpdateCount() int32 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.updateCount
}

func NewCountingEventsListener[K comparable, V any]() *CountingEventsListener[K, V] {
	countingListener := CountingEventsListener[K, V]{
		listener: coherence.NewMapListener[K, V](),
	}

	countingListener.listener.OnAny(func(e coherence.MapEvent[K, V]) {
		countingListener.mutex.Lock()
		defer countingListener.mutex.Unlock()

		switch e.Type() {
		case coherence.EntryInserted:
			atomic.AddInt32(&countingListener.insertCount, 1)
		case coherence.EntryUpdated:
			atomic.AddInt32(&countingListener.updateCount, 1)
		case coherence.EntryDeleted:
			atomic.AddInt32(&countingListener.deleteCount, 1)
		}
	})

	return &countingListener
}
