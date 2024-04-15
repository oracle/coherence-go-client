# Coherence Go Client Examples

This directory contains various examples on how to use the Coherence Go client.

### Clone the Coherence Go Client Repository

```bash
git clone github.com/coherence/coherence-go-client
cd coherence-go-client/examples
```

### Install the Coherence Go Client

```bash
go get github.com/oracle/coherence-go-client@latest
````

### Start a Coherence Cluster

Before running any of the examples, you must ensure a Coherence cluster is available.
For local development, we recommend using the Coherence CE Docker image; it contains
everything necessary for the client to operate correctly.

```bash
docker run -d -p 1408:1408 -p 30000:30000 ghcr.io/oracle/coherence-ce:24.03
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

Source code: [aggregators/main.go](aggregators/main.go)

```go
go run aggregators/main.go
```

### <a name="processors"></a> Running processors

This example shows how to run entry processors against a or NamedMap or NamedCache with a key of int and value of Person struct.

Source code: [processors/main.go](processors/main.go)

```go
go run processors/main.go
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

#### Blocking

This example show how to use a blocking queue. Where you can try to issue a Poll() 
and provide a timeout incase there are no entries on the Queue.

To run this example there are three programs:

1. [queues/blocking/publisher/main.go](queues/blocking/publisher/main.go) - Publishes a specified number of orders to a queue
2. [queues/blocking/processor/main.go](queues/blocking/processor/main.go) - Polls() on orders-queue, processes the order and places on processed-queue
3. [queues/blocking/subscriber/main.go](queues/blocking/subscriber/main.go) - Polls() processed-queue and displays processing time

To run this example, do the following in separate command terminals:
1. Start a subscriber `go run queues/blocking/subscriber/main.go` 
2. Start one or more processors `go run queues/blocking/processor/main.go` 
3. Start a publisher and specify the number orders `go run queues/blocking/publisher/main.go 10000` 

### <a name="rest"></a> Basic REST server

This example starts a listener on port localhost:17268 which provides a basic REST API providing POST, GET, PUT and DELETE operations against a NamedMap.

Source code: [rest/main.go](rest/main.go)

```go
go run rest/main.go
```
