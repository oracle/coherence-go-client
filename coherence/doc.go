/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package coherence provides a set of functions and interfaces for Go programs to act as cache clients to a
Coherence Cluster using gRPC for the network transport.

Your cluster must be running Coherence Community Edition (CE) 22.06.4+ or Coherence commercial
version 14.1.1.2206.4+ and must be running a gRPC Proxy.

Two interfaces, [NamedMap] and [NamedCache], are available to access Coherence caches. [NamedCache] is syntactically identical in behaviour to a [NamedMap],
but additionally implements the PutWithExpiry operation.

# Introduction

The Coherence Go client provides the following features:

  - Familiar Map-like interface for manipulating cache entries including but not limited to Put, PutWithExpiry, PutIfAbsent, PutAll, Get, GetAll, Remove, Clear, GetOrDefault, Replace, ReplaceMapping, Size, IsEmpty, ContainsKey, ContainsValue, ContainsEntry
  - Cluster-side querying, aggregation and filtering of map entries
  - Cluster-side manipulation of map entries using EntryProcessors
  - Registration of listeners to be notified of mutations such as
  - insert, update and delete on Maps, map lifecycle events such as truncated, released or destroyed
    and session lifecycle events such as connected, disconnected, reconnected and closed
  - Support for storing Go structs as JSON as well as the ability to serialize to Java objects on the server for access from other Coherence language API's
  - Full support for Go generics in all Coherence API's

For more information on Coherence caches, please see the [Coherence Documentation].

# Supported Go versions

This API fully supports Go Generics and is only supported for use with Go versions 1.19 and above.

# Obtaining a Session

Example:

	import (
	    coherence "github.com/oracle/coherence-go-client/coherence"
	)

	...

	session, err := coherence.NewSession(ctx)
	if err != nil {
	    log.Fatal(err)
	}
	defer session.Close()

The [NewSession] function creates a new session that will connect to a gRPC proxy server on "localhost:1408" by default.

You can specify the host and port to connect to by specifying the environment variable COHERENCE_SERVER_ADDRESS.
See [gRPC Naming] for information on values for this.
You can also pass [coherence.WithAddress]("host:port") to specify the gRPC host and
port to connect to. The default connection mode is with SSL enabled, but you can use plan-text via using [coherence.WithPlainText]().

To Configure SSL, you must first enable SSL on the gRPC Proxy, see [gRPC Proxy documentation] for details.
Refer to the section on [NewSession] for more information on setting up a SSL connection on the client.

# Obtaining a NamedMap or NamedCache

Once a session has been created, the [NewNamedMap](session, name, ...options) can be used to obtain
an instance of a [NamedMap]. The key and value types must be provided as generic type arguments.
This identifier may be shared across clients.  It's also possible to have many [NamedMap]'s defined and in use simultaneously.

Example:

	session, err := coherence.NewSession(ctx)
	if err != nil {
	    log.Fatal(err)
	}
	defer session.Close()

	namedMap, err := coherence.NewNamedMap[int, string](session, "customers")
	if err != nil {
	    log.Fatal(err)
	}

If you wish to create a [NamedCache], which supports expiry, you can use the [NewNamedCache] function and then use the PutWithExpiry function call.

	namedCache, err := coherence.NewNamedCache[int, string](session, "customers")
	if err != nil {
	    log.Fatal(err)
	}

	_, err = namedCache.PutWithExpiry(ctx, person1.ID, person1, time.Duration(5)*time.Second)

If your [NamedCache] requires the same expiry for every entry, you can use the [coherence.WithExpiry] cache option.
Each call to Put will use the default expiry you have specified. If you use PutWithExpiry, this will override the default
expiry for that key.

	namedCache, err := coherence.NewNamedCache[int, Person](session, "cache-expiry", coherence.WithExpiry(time.Duration(5)*time.Second))

See [SessionOptions] which lists all the options supported by the [Session] API.

# Basic CRUD operations

Note: See the [examples] on GitHub for detailed examples.

Assuming a very trivial [NamedMap] with integer keys and string values.

	session, err := coherence.NewSession(coherence.WithPlainText())
	if err != nil {
	    log.Fatal(err)
	}

	namedMap, err := coherence.NewNamedMap[int, string](session, "my-map")
	if err != nil {
	    log.Fatal(err)
	}

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

# Working with structs

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

# Querying and filtering using channels

Channels are used to deal with individual keys, values or entries
streamed from the backend using a filter or an open query.  Depending
upon the operation, each result element is wrapped in one of the structs
[StreamedEntry], [StreamedValue] or [StreamedKey] which wraps an error and a
Key and/or a Value. As always, the Err object must be checked for errors before accessing the Key or Value fields.
All functions that return channels are EntrySetFilter, KeySetFilter, ValuesFilter,
EntrySet, KeySet, Values, InvokeAll and InvokeAllFilter.

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

# Using entry processors for in-place processing

A Processor is an object that allows you to process (update) one or more [NamedMap] entries on the [NamedMap] itself,
instead of moving the entries to the client across the network. In other words, using processors we send
the processing to where the data resides thus avoiding massive data movement across the network. Processors can be
executed against all entries, a single key or against a set of entries that match a Filter.

To demonstrate this, lets assume we have a [NamedMap] populated with Person struct below, and we want to
run various scenarios to increase peoples salary by using a [processors.Multiply] processor.

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
	for result := range ch2 {
	    if result.Err != nil {
	        log.Fatal(result.Err)
	    }
	}

	// 3. Increase the salary of people with Id 1 and 5
	ch2 := coherence.InvokeAllKeys[int, Person, float32](ctx, namedMap, []int{1, 5}, processors.Multiply("salary", 1.1, true)
	for result := range ch2 {
	    if result.Err != nil {
	        log.Fatal(result.Err)
	    }
	}

# Aggregating cache data

Aggregators can be used to perform operations against a subset of entries to obtain a single result.
Entry aggregation occurs in parallel across the grid to provide map-reduce support when working with
large amounts of data.

To demonstrate this, lets assume we have a [NamedMap] populated with Person struct as per the previous example, and we want to
run various scenarios to perform aggregations.

	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	// Retrieve the distinct cities from all people
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

# Responding to cache events

he Coherence Go Client provides the ability to add a [MapListener] that will receive events (inserts, updates, deletes)
that occur against a [NamedMap] or [NamedCache]. You can listen for all events, events based upon a filter or
vents based upon a key.

	// in your main code, create a new NamedMap and register the listener
	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	listener := coherence.NewMapListener[int, Person]().OnUpdated(
	func(e coherence.MapEvent[int, Person]) {
	    key, err := e.Key()
	    if err != nil {
	        panic("unable to deserialize key")
	    }

	    newValue, err := e.NewValue()
	    if err != nil {
	        panic("unable to deserialize new value")
	    }

	    oldValue, err := e.OldValue()
	    if err != nil {
	        panic("unable to deserialize old value")
	    }

	    fmt.Printf("**EVENT=Updated: key=%v, oldValue=%v, newValue=%v\n", *key, *oldValue, *newValue)
	})

	if err = namedMap.AddListener(ctx, listener); err != nil {
	    panic(err)
	}

	// ensure we unregister the listener
	defer func(ctx context.Context, namedMap coherence.NamedMap[int, Person], listener coherence.MapListener[int, Person]) {
	    _ = namedMap.RemoveListener(ctx, listener)
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
	if err = namedMap.AddFilterListener(ctx, listener,
	    filters.Greater(extractors.Extract[int]("salary"), 17000)); err != nil {
	    log.Fatal("unable to add listener", listener, err)
	}

	// You can also listen on a specific key, e.g. list on key 1.
	listener := NewUpdateEventsListener[int, Person]()
	if err = namedMap.AddKeyListener(ctx, listener, 1); err != nil {
	    log.Fatal("unable to add listener", listener, err)
	}

# Responding to cache lifecycle events

The Coherence Go Client provides the ability to add a [MapLifecycleListener] that will receive events (truncated and destroyed)
that occur against a [NamedMap] or [NamedCache].

	// consider the example below where we want to listen for all 'truncate' events for a NamedMap.
	// in your main code, create a new NamedMap and register the listener
	namedMap, err := coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
	    log.Fatal(err)
	}

	// Create a listener and add to the cache
	listener := coherence.NewMapLifecycleListener[int, Person]().
	    OnTruncated(func(e coherence.MapLifecycleEvent[int, Person]) {
	        fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
	    })

	namedMap.AddLifecycleListener(listener)
	defer namedMap.RemoveLifecycleListener(listener)

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

# Responding to session lifecycle events

The Coherence Go Client provides the ability to add a [SessionLifecycleListener] that will receive events (connected, closed,
disconnected or reconnected) that occur against the [Session].
Note: These events use and experimental gRPC API so may not be reliable or may change in the future. This is due to the
experimental nature of the underlying gRPC API.

Consider the example below where we want to listen for all 'All' events for a [Session].
in your main code, create a new [Session] and register the listener

	// create a new Session
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
	    log.Fatal(err)
	}

	// Create a listener to listen for session events
	listener := coherence.NewSessionLifecycleListener().
	    OnAny(func(e coherence.SessionLifecycleEvent) {
	        fmt.Printf("**EVENT=%s: source=%v\n", e.Type(), e.Source())
	})

	session.AddSessionLifecycleListener(listener)
	defer session.RemoveSessionLifecycleListener(listener)

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

# Serializing to Java objects on the server

By default, the Coherence Go client serializes any keys and values to JSON and then stores them as JsonObjects in Coherence.
This is usually sufficient for most applications where you are only accessing your data via the Go Client.

If you wish to access your data via other clients such as Java, JavaScript, C++, .NET or Python, it's best to use Java classes, known to Coherence server,
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

[Coherence Documentation]: https://docs.oracle.com/en/middleware/standalone/coherence/14.1.1.2206/develop-applications/introduction-coherence-caches.html
[examples]: https://github.com/oracle/coherence-go-client/tree/main/examples
[gRPC Proxy documentation]: https://docs.oracle.com/en/middleware/standalone/coherence/14.1.1.2206/develop-remote-clients/using-coherence-grpc-server.html
[gRPC Naming]: https://github.com/grpc/grpc/blob/master/doc/naming.md
*/
package coherence
