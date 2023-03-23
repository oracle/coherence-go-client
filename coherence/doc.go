/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package coherence provides a set of functions and interfaces for Go programs to act as cache clients to a
Coherence Cluster using Google's gRPC framework for the network transport.

Your cluster must be running Coherence Community Edition (CE) 22.06.4+ or Coherence commercial
version 14.1.1.2206.4+ and must be running a gRPC Proxy.

This API provides two types of caches, NamedMap and NamedCache. NamedCache is syntactically identical in behaviour to a NamedMap,
but additionally implements the PutWithExpiry operation.

# NamedMap

NamedMap is an externally managed map-like (Java) data-structure,
mapping keys to values, supporting full concurrency of retrievals and high expected concurrency
for updates. Like traditional maps, this object cannot contain duplicate keys;
each key can map to at most one value.

Unlike traditional on-heap map implementations, objects of this interface will typically
maintain their keys and values off-heap, possibly in external processes.  Programs using
these objects may thus experience increased latency when accessing keys and values
compared to traditional on-heap map implementations as access may entail remote
network requests, together with object serialization and deserialization to and from JSON.

Great care must be exercised if mutable objects are used as map keys.  The behavior of a map
is not specified if the value of a key is changed in a manner that affects comparisons when
the key is placed in a map.  A NamedMap must not contain itself as a key or value.

Although all operations are thread-safe, retrieval operations do not entail locking, and there
is no support for locking an entire map in a way to prevent all access.  Retrievals reflect
the results of the most recently completed update operations holding upon their onset. More
formally, an update operation for a given key bears a happens-before relation with any
(non-nil) retrieval for that key reporting the updated value.

For aggregate operations such as Size, PutAll and Clear, concurrent retrievals may reflect
insertion or removal of only some entries. Similarly, values returned by streams may only reflect
the state of the elements at some point or since the creation of the stream, which are designed to
be used by only one thread at a time. Bear in mind that the results of aggregate status methods
including Size and IsEmpty are typically useful only when a map is not undergoing concurrent updates
in other threads or programs. Otherwise, the results of these methods reflect transient states that
may be adequate for monitoring or estimation purposes, but not for program control.

In addition to basic key/value store functionality NamedMap offers a rich set of primitives in order
to query, mutate, and aggregate values in the remote store. Filters offer the ability to filter and
reduce the operational result set.  Processors allow invocation of operations against a result set,
possibly mutating the values.  Lastly, Aggregators allow summarizing a result set.

# Supported Go Versions

This API fully supports Go Generics and is only supported for use with Go versions 1.19 and above.

# Obtaining an instance of a NamedMap

New NamedMap instances are constructed using the Session APIs.

Example:

	import (
	    coherence "github.com/oracle/coherence-go-client/coherence"
	)

	session, err := coherence.NewSession(ctx)
	if err != nil {
	    log.Fatal(err)
	}
	defer session.Close()

	namedMap, err := coherence.NewNamedMap[int, string](session, "customers")
	if err != nil {
	    log.Fatal(err)
	}

The NewSession() method creates a new session that will connect to a gRPC proxy server on "localhost:1408" by default.

You can specify the host and port to connect to by specifying the environment variables COHERENCE_SERVER_HOST_NAME
and COHERENCE_SERVER_GRPC_PORT. You can also pass coherence.WithAddress("host:port") to specify the gRPC host and
port to connect to. The default connection mode is with SSL enabled, but you can use plan-text via using coherence.WithPlainText().

To Configure SSL, you must first enable SSL on the gRPC Proxy, see https://docs.oracle.com/en/middleware/standalone/coherence/14.1.1.2206/develop-remote-clients/using-coherence-grpc-server.html for details.
Refer to the section on Session for more information on setting up a SSL connection on the client.

Once a session has been created, the NewNamedMap(session, name) can be used to obtain
an instance of a NamedMap. The key and value types must be provided as generic type arguments.
This identifier may be shared across clients.  It's also possible to have many NamedMap's simultaneously.

If you wish to create a NamedCache, which supports expiry, you can use the following:

	namedMap, err := coherence.NewNamedCache[int, string](session, "customers")
	if err != nil {
	    log.Fatal(err)
	}

See session.go which lists all the options supported by the Session APIs.

# Examples using a NamedMap

Note: See https://github.com/oracle/coherence-go-client/examples for more detailed examples.

Assuming a very trivial NamedMap with integer keys and string values.

	session, err := coherence.NewSession(coherence.WithPlainText())
	if err != nil {
	    log.Fatal(err)
	}

	namedMap, err := coherence.NewNamedMap[int, string](session, "my-map")
	if err != nil {
	    log.Fatal(err)
	}

Ex. 1 - Storing/retrieving/removing a value to/from the NamedMap

	ctx := context.Background()

	// put a new key / value
	if _, err = namedMap.Put(ctx, 1, "one"); err != nil {
	    log.Fatal(err)
	}

	// get the value for the given key
	if value, err = namedMap.Get(ctx, 1); err != nil {
	    log.Fatal(err)
	}
	fmt.Println("Value for key 1 is", *value)

	// update the value for key 1
	if _, err = namedMap.Put(ctx, 1, "ONE"); err != nil {
	    log.Fatal(err)
	}

	// retrieve the updated value for the given key
	if value, err = namedMap.Get(ctx, 1); err != nil {
	    log.Fatal(err)
	}
	fmt.Println("Updated value is", *value)

	if _, err = namedMap.Remove(ctx, 1); err != nil {
	    log.Fatal(err)
	}

Note: Keys and values are serialized to JSON and stored in Coherence as a com.oracle.coherence.io.json.JsonObject.
if you wish to store structs as native Java objects, then please see the section further down on "Serializing to Java Objects on the Server".

Ex. 2 - Working with structs

	type Person struct {
	    ID   int    `json:"id"`
	    Name string `json:"name"`
	    Age  int    `json:"age"`
	}

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.NewNamedMap[int, Person](session, "test")
	if err != nil {
	    log.Fatal(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
	    log.Fatal(err)
	}

	newPerson := Person{ID: 1, Name: "Tim", Age: 53}
	fmt.Println("Add new Person", newPerson)
	if _, err = namedMap.Put(ctx, newPerson.Id, newPerson); err != nil {
	    log.Fatal(err)
	}

	// retrieve the Person
	if person, err = namedMap.Get(ctx, 1); err != nil {
	    log.Fatal(err)
	}
	fmt.Println("Person from Get() is", *person)

	// Update the age using and entry processor for in-place processing
	_, err = coherence.Invoke[int, Person, bool](ctx, namedMap, 1, processors.Update("age", 56))
	if err != nil {
	    log.Fatal(err)
	}

	// retrieve the updatedPerson
	if person, err = namedMap.Get(ctx, 1); err != nil {
	    log.Fatal(err)
	}
	fmt.Println("Person is", *person)

Ex. 3 - Working with streaming filtered results using channels

	// Channels are used to deal with individual keys, values or entries
	// streamed from the backend using a filter.  Each result element is wrapped in
	// in a struct called StreamEntry which is a wrapper object
	// that wraps an error and a result. As always, the Err object must be
	// checked for errors before accessing the Val field.
	// Other functions that return channels are KeySetFilter(), ValuesFilter(),
	// InvokeAll() and InvokeAllFilter().

	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	// extractors
	age := extractors.Extract[int]("age")
	name := extractors.Extract[string]("name")

	// retrieve all people aged > 30
	ch := namedMap.EntrySetFilter(ctx, filters.Greater(age, 20))
	for result := range ch {
	    if result.Err != nil {
	        log.Fatal(err)
	    }
	    fmt.Println("Key:", result.Key, "Value:", result.Value)
	}

	// we can also do more complex filtering such as looking for people > 30 and where there name begins with 'T'
	ch := namedMap.EntrySetFilter(ctx, filters.Greater(age, 20).And(filters.Like(name, "T%", true)))

Ex. 4 - Working with KeySet(), EntrySet() and Values() results

	// The KeySet(),  EntrySet() and Values() both return all keys, entries and values respectively and can possibly
	// return big datasets when using large NamedMaps or NamedCaches. Because of this reason, both these calls
	// return "Page Iterators" which internally page the data back allowing you to more efficiently process large data sets.

	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	iter := namedMap.Values(ctx)
	for {
	    person, err = iter.Next()
	    if err == coherence.ErrDone {
	        // no more entries to iterate over
	        break
	    }
	    // check for an actual error
	    if err != nil {
	        log.Fatal(err)
	    }
	    // process the value
	    fmt.Println("Person:", *person)
	}

Ex. 5 Using entry processors for in-place processing

	// A Processor is an object that allows you to process (update) one or more NamedMap entries on the NamedMap itself,
	// instead of moving the entries to the client across the network. In other words, using processors we send
	// the processing to where the data resides thus avoiding massive data movement across the network. Processors can be
	// executed against all entries, a single key or against a set of entries that match a Filter.

	// To demonstrate this, lets assume we have a NamedMap populated with Person struct below, and we want to
	// run various scenarios to increase peoples salary by using a Multiply processor.

	type Person struct {
	    Id     int     `json:"id"`
	    Name   string  `json:"name"`
	    Salary float32 `json:"salary"`
	    Age    int     `json:"age"`
	    City   string  `json:"city"`
	}

	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")

	// 1. Increase the salary of the person with Id = 1
	newSalary, err = coherence.Invoke[int, Person, float32](ctx, namedMap, 1, processors.Multiply("salary", 1.1, true))

	city := extractors.Extract[string]("city")

	// 2. Increase the salary of all people in Perth
	ch2 := coherence.InvokeAllFilter[int, Person, float32](ctx, namedMap, filters.Equal(city, "Perth"), processors.Multiply("salary", 1.1, true)
	for e := range ch {
	    if e.Err != nil {
	        log.Fatal(e.Err)
	    }
	}

	// 3. Increase the salary of people with Id 1 and 5
	ch2 := coherence.InvokeAllKeys[int, Person, float32](ctx, namedMap, []int{1, 5}, processors.Multiply("salary", 1.1, true)
	for e := range ch {
	    if e.Err != nil {
	        log.Fatal(e.Err)
	    }
	}

Ex.6 Aggregating results

	// Aggregators can be used to perform operations against a subset of entries to obtain a single result.
	// Entry aggregation occurs in parallel across the grid to provide map-reduce support when working with
	// large amounts of data.

	// To demonstrate this, lets assume we have a NamedMap populated with Person struct as per example 5, and we want to
	// run various scenarios to perform aggregations.

	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	// Retrieve the distinct cities from all people
	var citiesValues *[]string
	citiesValues, err := coherence.Aggregate(ctx, namedMap, extractors.Extract[string]("city"))
	if err != nil {
	    log.Fatal(err)
	}
	fmt.Println(*citiesValues)
	// output: [Perth, Melbourne, Brisbane]

	age := extractors.Extract[int]("age")

	// minimum age across keys 3 and 4
	ageResult, err = coherence.AggregateKeys(ctx, namedMap, []int{3, 4}, aggregators.Min(age))

	// top 2 people by salary using filter
	var salaryResult *[]Person
	salaryResult, err = coherence.AggregateFilter[int, Person, []Person](ctx, namedMap, filters.Greater(age, 40),
	    aggregators.TopN[float32, Person](extractors.Extract[float32]("salary"), false, 2))

Ex. 7 Responding to Map events

	// The Coherence Go Client provides the ability to add a MapListener that will receive events (inserts, updates, deletes)
	// that occur against a NamedMap or NamedCache. You can listen for all events, events based upon a filter or
	// events based upon a key.

	// consider the example below where we want to listen for all 'update' events for a NamedMap.
	// We define the following type and specify it comprises a coherence.MapListener.

	type UpdateEventsListener[K comparable, V any] struct {
	    listener coherence.MapListener[K, V]
	}

	// we then define a function to return this type and register for the OnUpdated event.
	// There are also OnInserted, OnDeleted and OnAny functions.

	func NewUpdateEventsListener[K comparable, V any]() *UpdateEventsListener[K, V] {
	    exampleListener := UpdateEventsListener[K, V]{
	    listener: coherence.NewMapListener[K, V](),
	    }

	    exampleListener.listener.OnUpdated(func(e coherence.MapEvent[K, V]) {
	        key, err := e.Key()
	        if err != nil {
	            log.Fatal("unable to deserialize key")
	        }

	        newValue, err := e.NewValue()
	        if err != nil {
	            log.Fatal("unable to deserialize new value")
	        }

	        oldValue, err := e.OldValue()
	        if err != nil {
	            log.Fatal("unable to deserialize old value")
	        }
	        fmt.Printf("**EVENT=Updated: key=%v, oldValue=%v, newValue=%v\n", *key, *oldValue, *newValue)
	    })
	    return &exampleListener
	}

	// in your main code, create a new NamedMap and register the listener
	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	listener := NewUpdateEventsListener[int, Person]()
	if err = namedMap.AddListener(ctx, listener.listener); err != nil {
	    log.Fatal("unable to add listener", listener)
	}

	// ensure we unregister the listener
	defer func(ctx context.Context, namedMap coherence.NamedMap[int, Person], listener *UpdateEventsListener[int, Person]) {
	    _ = namedMap.RemoveListener(ctx, listener.listener)
	}(ctx, namedMap, listener)

	// As you carry out operations that will mutate the cache entries, update the age to 56, you will see the events printed
	_, err = coherence.Invoke[int, Person, bool](ctx, namedMap, 1, processors.Update("age", 56))
	if err != nil {
	    log.Fatal(err)
	}

	// output
	// **EVENT=Updated: key=1, oldValue={1 Tim 53}, newValue={1 Tim 53}

	// you can also listen based upon filters, for example the following would create a
	// listener for all entries where the salary is > 17000
	if err = namedMap.AddFilterListener(ctx, listener.listener,
	    filters.Greater(extractors.Extract[int]("salary"), 17000)); err != nil {
	    log.Fatal("unable to add listener", listener)
	}

	// You can also listen on a specific key, e.g. list on key 1.
	listener := NewUpdateEventsListener[int, Person]()
	if err = namedMap.AddKeyListener(ctx, listener.listener, 1); err != nil {
	    log.Fatal("unable to add listener", listener)
	}

Ex. 8 Responding to Cache Lifecycle events

	// The Coherence Go Client provides the ability to add a MapLifecycleListener that will receive events (truncated and destroyed)
	// that occur against a NamedMap or NamedCache.

	// consider the example below where we want to listen for all 'truncate' events for a NamedMap.
	// We define the following type and specify it comprises a coherence.MapLifecycleListener.

	type TruncateEventsListener[K comparable, V any] struct {
	    listener coherence.MapLifecycleListener[K, V]
	}

	// we then define a function to return this type and register for the OnTruncated event.
	// There are also OnDestroyed and OnAny functions.

	func NewTruncateEventsListener[K comparable, V any]() *TruncateEventsListener[K, V] {
	    exampleListener := TruncateEventsListener[K, V]{
	        listener: coherence.NewMapLifecycleListener[K, V](),
	    }

	    exampleListener.listener.OnTruncated(func(e coherence.MapLifecycleEvent[K, V]) {
	        fmt.Printf("**EVENT=Truncated: value=%v\n", e.Source())
	    })

	    return &exampleListener
	}

	// in your main code, create a new NamedMap and register the listener
	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	// Create a listener and add to the cache
	listener := NewTruncateEventsListener[int, Person]()

	namedMap.AddLifecycleListener(listener.listener)
	defer namedMap.RemoveLifecycleListener(listener.listener)

	newPerson := Person{ID: 1, Name: "Tim", Age: 53}
	fmt.Println("Add new Person", newPerson)
	if _, err = namedMap.Put(ctx, newPerson.Id, newPerson); err != nil {
	    log.Fatal(err)
	}

	if size, err = namedMap.Size(ctx); err != nil {
	    log.Fatal(err)
	}
	fmt.Println("Cache size is", size, "truncating cache")

	if err = namedMap.Truncate(ctx); err != nil {
	    log.Fatal(err)
	}

	time.Sleep(time.Duration(5) * time.Second)

	// output
	// Add new Person {1 Tim 53}
	// Cache size is 1 truncating cache
	// **EVENT=Truncated: value=NamedMap{name=people, format=json}

Ex. 9 Responding to Session Lifecycle events

	// The Coherence Go Client provides the ability to add a SessionLifecycleListener that will receive events (connected, closed,
	// disconnected or reconnected) that occur against the session.
	// Note: These events use and experimental gRPC API so may not be reliable or may change in the future.

	// consider the example below where we want to listen for all 'All' events for a Session.
	// We define the following type and specify it comprises a coherence.SessionLifecycleListener.

	type AllSessionLifecycleEventsListener struct {
	    listener coherence.SessionLifecycleListener
	}

	func NewAllLifecycleEventsListener() *AllSessionLifecycleEventsListener {
	    exampleListener := AllSessionLifecycleEventsListener{
		    listener: coherence.NewSessionLifecycleListener(),
	    }

	    exampleListener.listener.OnAny(func(e coherence.SessionLifecycleEvent) {
	        fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
	    })

	    return &exampleListener
	}

	// in your main code, create a new Session and register the listener

	// create a new Session
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
	    log.Fatal(err)
	}

	// Create a listener to listen for session events
	listener := NewAllLifecycleEventsListener()

	session.AddSessionLifecycleListener(listener.listener)
	defer session.RemoveSessionLifecycleListener(listener.listener)

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	// clear the Map
	if err = namedMap.Clear(ctx); err != nil {
	    log.Fatal(err)
	}

	session.Close()

	time.Sleep(time.Duration(5) * time.Second)

	// output
	// 2023/01/31 11:15:37 connected session 59f3ec81-dda1-41b7-92de-70aad3d26615 to address localhost:1408
	// 2023/01/31 11:15:38 closed session 59f3ec81-dda1-41b7-92de-70aad3d26615
	// **EVENT=session_closed: source=SessionID=59f3ec81-dda1-41b7-92de-70aad3d26615, closed=true, caches=0, maps=0

# Serializing to Java Objects on the Server

By default, the Coherence Go client serializes any keys and values to JSON and then stores them as JsonObjects in Coherence.
This is usually sufficient for most applications where you are only accessing your data via the Go Client.

If you wish to access your data via other clients such as Java, JavaScript or Python, it's best to use Java classes, known to Coherence server,
representing the data model. The following describes how to achieve interoperability with Java.

Step 1. Create your Java Classes

Firstly you must define your data model for all Java classes and configure for JSON serialization. You do not need to annotate all the attributes
with @JsonbProperty, but it is a good practice so that you have consistent names with Go. Below is a shorted version of a
Customer class without all the extras such as getters, setters, hashCode, etc, that you know you need. In the example below I am using
standard Java serialization, but you can use POF serialization if you have that configured.

	package com.oracle.demo;

	public class Customer implements Serializable {
		public Customer() {} // required

		@JsonbProperty("id")
		private int id;

		@JsonbProperty("customerName")
		private String  customerName;

		@JsonbProperty("outstandingBalance")
		private double outstandingBalance;

		...

Step 2. Define your type alias.

In the code deployed to your Coherence storage-nodes, you need to create a file in your resources root called META-INF/type-aliases.properties
which contains an alias and fully qualified class name for each of your classes.

	# Example META-INF/type-aliases.properties file
	customer=com.oracle.demo.Customer
	order.com.oracle.demo.Order

Step 3. Define your Go structs

Next you need to define your Go structs with JSON names matching your Java objects. You also need to include a Class attribute with
the JSON attribute name of "@class". We will set this in our object to the value "customer" matching the value in the type-aliases.properties
on the server.

	type Customer struct {
	    Class              string   `json:"@class"`
	    ID                 int      `json:"id"`
	    CustomerName       string   `json:"customerName"`
	    OutstandingBalance float32  `json:"outstandingBalance"`
	}

Step 4. Create and put the value

Lastly, when you create a Customer object you must set the Class value matching the alias above.

	customer := Customer{
	    Class:              "customer",
	    ID:                 1,
	    CustomerName:       "Tim",
	    OutstandingBalance: 10000,
	}

	// store the entry in Coherence, it will be stored as a com.oracle.demo.Customer POJO!

	_, err = namedMap.Put(ctx, customer.ID, customer)
	if err != nil {
	    log.Fatal(err)
	}
*/
package coherence
