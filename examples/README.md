# Coherence Go Client Examples

This directory contains various examples on how to use the Coherence Go client.

### Clone the Coherence Go Client Repository

```bash
git clone github.com/coherence/coherence-go-client
cd coherence-go-client/examples
```

### Install the Coherence Go Client

```bash
go get github.com/oracle/coherence-go-client/v2@latest
````

### Start a Coherence Cluster

Before running any of the examples, you must ensure a Coherence cluster is available.
For local development, we recommend using the Coherence CE Docker image; it contains
everything necessary for the client to operate correctly.

```bash
docker run -d -p 1408:1408 -p 30000:30000 ghcr.io/oracle/coherence-ce:24.09
```

### Index

* [Basic operations using primitives and structs](#basic)
* [Using near caches](#near)
* [Querying data](#querying)
* [Aggregating data](#aggregations)
* [Mutating data using entry processors](#processors)
* [Listening for map events](#map-events)
* [Listening for map lifecycle events](#lifecycle-events)
* [Listening for session lifecycle events](#session-lifecycle-events)
* [Adding indexes](#indexes)
* [Working with Queues](#queues)
* [Basic REST server](#rest)
* [Custom Comparators and Entry Processors](#custom)

### <a name="basic"></a> Basic Operations

These examples shows how to carry out basic operations against a NamedMap or NamedCache.

#### Using Primitive Types

This examples runs `Put()`, `Get()`, `Remove()` operations against a NamedMap or NamedCache with key in and value string.

Source code: [basic/crud/main.go](basic/crud/main.go)

```go
go run basic/crud/main.go
```

#### Using Structs as values

Source code: [basic/struct/main.go](basic/struct/main.go)

```go
go run basic/struct/main.go
```

#### Using Structs as keys and values

Source code: [basic/struct_keys/main.go](basic/struct_keys/main.go)

```go
go run basic/struct_keys/main.go
```

#### Using Various contains functions

Source code: [basic/contains/main.go](basic/contains/main.go)

```go
go run basic/contains/main.go
```

#### Putting entries that expire

This example uses a `NamedCache` and issues `PutWithExpiry` to expire a value after a certain time.

Source code: [basic/expiry/main.go](basic/expiry/main.go)

```go
go run basic/expiry/main.go
```

#### Setting expiry on cache creation

This example uses a `NamedCache` that has been created using `coherence.WithExpiry` option, which 
will expire **all** cache entries after the specified time without the need to use `PutWithExpiry`.

Source code: [basic/expiry_cache/main.go](basic/expiry_cache/main.go)

```go
go run basic/expiry_cache/main.go
```

### <a name="near"></a> Using near caches

These example shows how to specify a near-cache for either NamedMap or NamedCache which will
cache data accessed via Get() operations on the Go client for fast subsequent local access.
Near caches can specify time-to-live (TTL) for entries in a cache as well as number of entries or size of entryes.
Any updates of data from the back-end will update the data in the near cache or any
data removals will invalidate the near cache.

#### Near cache with 10 second TTL

Source code: [basic/near_cache/ttl/main.go](basic/near_cache/ttl/main.go)

```go
go run basic/near_cache/ttl/main.go
```
#### Near cache with high units of 1000

Source code: [basic/near_cache/high_units/main.go](basic/near_cache/high_units/main.go)

```go
go run basic/near_cache/high_units/main.go
```

#### Near cache with high units memory of 10KB

Source code: [basic/near_cache/memory/main.go](basic/near_cache/memory/main.go)

```go
go run basic/near_cache/memory/main.go
```

### <a name="querying"></a> Querying data

These examples shows how to query data operations against a NamedMap or NamedCache.

#### Querying using keys

Source code: [querying/keys/main.go](querying/keys/main.go)

```go
go run querying/keys/main.go
```

#### Querying using filters

Source code: [querying/filters/main.go](querying/filters/main.go)

```go
go run querying/filters/main.go
```

#### Querying using filters and sorted results

Source code: [querying/filters_sorted/main.go](querying/filters_sorted/main.go)

```go
go run querying/filters_sorted/main.go
```

#### Querying all data

> Note: When using open-ended queries, Coherence internally pages data to ensure that you are not
> returning all data in one large dataset. Care should still be taken to minimize occurrences of these queries
> on large caches.

Source code: [querying/main.go](querying/main.go)

```go
go run querying/main.go
```

### <a name="aggregations"></a> Aggregating data

This example shows how to carry out various aggregations against a NamedMap or NamedCache.

Source code: [aggregators/basic/main.go](aggregators/basic/main.go)

```go
go run aggregators/basic/main.go
```

This example shows how to carry out an explain plan against a NamedMap or NamedCache.

Source code: [aggregators/explain/main.go](aggregators/explain/main.go)

```go
go run aggregators/explain/main.go
```

### <a name="processors"></a> Running processors

This example shows how to run entry processors against a or NamedMap or NamedCache with a key of int and value of Person struct.

Source code: [processors/standard/main.go](processors/standard/main.go)

```go
go run processors/standard/main.go
````

This example shows how to run the same entry processors but use the utility functions to ignore the values returned.

Source code: [processors/blind/main.go](processors/blind/main.go)

```go
go run processors/blind/main.go
```

### <a name="map-events"></a> Listening for map events

These examples show how to listen for events on a NamedMap or NamedCache.

#### Listening for all cache events

Source code: [events/cache/all/main.go](events/cache/all/main.go)

```go
go run events/cache/all/main.go
```

#### Listening for cache insert events

Source code: [events/cache/insert/main.go](events/cache/insert/main.go)

```go
go run events/cache/insert/main.go
```

#### Listening for cache update events

Source code: [events/cache/update/main.go](events/cache/update/main.go)

```go
go run events/cache/update/main.go
```

#### Listening for cache delete events

Source code: [events/cache/delete/main.go](events/cache/delete/main.go)

```go
go run events/cache/delete/main.go
```

#### Listening for ache events using filters

Source code: [events/cache/filters/main.go](events/cache/filters/main.go)

```go
go run events/cache/filters/main.go
```

#### Listening for cache events using keys

Source code: [events/cache/key/main.go](events/cache/key/main.go)

```go
go run events/cache/key/main.go
```

### <a name="lifecycle-events"></a> Listening for map lifecycle events

#### Listening for truncate cache events

Source code: [events/lifecycle/truncated/main.go](events/lifecycle/truncated/main.go)

```go
go run events/lifecycle/truncated/main.go
```
#### Listening for destroyed cache lifecycle events

Source code: [events/lifecycle/destroyed/main.go](events/lifecycle/destroyed/main.go)

```go
go run events/lifecycle/destroyed/main.go
``````

#### Listening for released cache lifecycle events

Source code: [events/lifecycle/released/main.go](events/lifecycle/released/main.go)

```go
go run events/lifecycle/released/main.go
```

#### Listening for all cache lifecycle events

Source code: [events/lifecycle/all/main.go](events/lifecycle/all/main.go)

```go
go run events/lifecycle/all/main.go
```

### <a name="session-lifecycle-events"></a> Listening for Session Lifecycle Events

#### Listening for all session events

Source code: [events/session/all/main.go](events/session/all/main.go)

```go
go run events/session/all/main.go
```

### <a name="indexes"></a> Adding indexes

This example shows how to add a remove indexes on a NamedMap or NamedCache to help query or aggregation performance.

Source code: [indexes/main.go](indexes/main.go)

```go
go run indexes/main.go
```

### <a name="queues"></a> Working with Queues

This example shows how to work with both standard (NamedQueue) and blocking (NamesBlockingQueue).

> Note: This feature is currently only available when using Coherence server version CE 24.04 and above.

#### Standard 

Source code: [queues/standard/main.go](queues/standard/main.go)

```go
go run queues/standard/main.go
```

#### Dequeue (double-ended queue)

This example show how to use a Dequeue or double-ended queue. 
To run this example there are three programs:

Source code: [queues/dequeue/main.go](queues/dequeue/main.go)

```go
go run queues/dequeue/main.go
```

#### Queue Events

This example show how to listen for events on queues.

Source code: [queues/events/main.go](queues/events/main.go)

```go
go run queues/events/main.go
```

### <a name="rest"></a> Basic REST server

This example starts a listener on port localhost:17268 which provides a basic REST API providing POST, GET, PUT and DELETE operations against a NamedMap.

Source code: [rest/main.go](rest/main.go)

```go
go run rest/main.go
```

### <a name="custom"></a> Custom Comparator and Entry Processors

This advanced use-case explains how to implement custom comparators and entry processors.

Coherence ships with many out of the box processors and comparators, but if you wish to use a custom comparator
or entry processor from the Go client, there are a number of steps you need to carry out:

1. Implement your server side Java code for the comparator or entry processor
2. Add the classes to the `META-INF/type-aliases.properties` file on the server
3. Implement a Go struct to represent the server side class
4. Call your custom comparator or entry processor from the Go client

#### Custom Comparators

1. Implement `compare` method of the sample Custom Comparator and add to your server side Java project - See the sample here: [CustomComparator](custom/comparators/java/com/oracle/coherence/example/CustomComparator.java)
2. Add a `META-INF/type-aliases.properties` file in your `resources` directory, with the following contents. The class name should match your class name and `custom.comparator` must match the value in the `Type` field in the Go code: 
   ```properties
   # Custom type-aliases.properties
   custom.comparator=com.oracle.coherence.example.CustomComparator
   ```
3. Ensure your Coherence server contains the above code compiled in the correct location, as well as the resources in the correct location
4. Create a Go struct to mimic the comparator, with the following contents:
   ```go
   type CustomComparator struct {
        Type      string                                 `json:"@class,omitempty"`
        Extractor extractors.ValueExtractor[any, string] `json:"extractor"`
   }

   // Compare must be implemented, but is a no-op on the client
   func (ue *CustomComparator) Compare(_ string, _ string) (int, error) {
       return 0, nil
   }
   ```
5. Call your custom comparator from the Go client:
   ```go
   // create your custom comparator with the correct `Type` value and your chosen extractor
   customComparator := &CustomComparator{
       Type:      "custom.comparator",
       Extractor: extractors.Extract[string]("name"),
   }
   
   // use with EntrySetFilterWithComparator as example using Always filter
   ch := coherence.EntrySetFilterWithComparator(ctx, config.Schools, filters.Always(), customComparator)
   for v := range ch {
       ...   
   }
   ```

#### Custom Entry Processors

1. Implement your custom Entry Processor - See the sample here: [UppercaseProcessor](custom/entry_processors/java/com/oracle/coherence/example/UppercaseProcessor.java)
2. Add a `META-INF/type-aliases.properties` file in your `resources` directory, with the following contents. The class name should match your class name and `custom.processor` must match the value in the `Type` field in the Go code:
   ```properties
   # Custom type-aliases.properties
   custom.processor=com.oracle.coherence.example.UppercaseProcessor
   ```
3. Ensure your Coherence server contains the above code compiled in the correct location, as well as the resources in the correct location
4. Create a Go struct to mimic the entry processor, with the following contents:
   ```go
   type UppercaseProcessor struct {
       Type string `json:"@class,omitempty"`
   }

   // AndThen must be implemented to be considered a Processor.
   func (p *UppercaseProcessor) AndThen(next processors.Processor) processors.Processor {
       return next
   }

   // When must also be implemented to be considered a Processor.
   func (p *UppercaseProcessor) When(_ filters.Filter) processors.Processor {
       return nil
    }
   ```
5. Call your custom entry processor from the Go client:
   ```go
   // create your custom entry process with the correct `Type` value
   processor := &UppercaseProcessor{
       Type: "custom.processor",
   }
   
   // use with Invoke to ensure customer with key 10 as uppercase name
   // Can also be used with InvokeAll, etc
   result, err := coherence.Invoke[int, Customer, interface{}](ctx, Customer, 10, processor)
   if err != nil {
       ...
   ```
