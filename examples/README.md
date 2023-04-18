# Coherence Go Client Examples

This directory contains various examples on how to use the Coherence Go client.

### Clone the Coherence Go Client Repository

```bash
git clone github.com/coherence/coherence-go-client
cd coherence-go-client/examples
```

### Install the Coherence Go Client

```bash
go get github.com/coherence/coherence-go-client@latest
````

### Start a Coherence Cluster

Before running any of the examples, you must ensure a Coherence cluster is available.
For local development, we recommend using the Coherence CE Docker image; it contains
everything necessary for the client to operate correctly.

```bash
docker run -d -p 1408:1408 -p 30000:30000 ghcr.io/oracle/coherence-ce:22.06.3
```

### Index

* [Basic operations using primitives and structs](#basic)
* [Querying data](#querying)
* [Aggregating data](#aggregations)
* [Mutating data using entry processors](#processors)
* [Listening for Map Events](#map-events)
* [Listening for Map Lifecycle Events](#lifecycle-events)
* [Listening for Session Lifecycle Events](#session-lifecycle-events)
* [Adding Indexes](#indexes)

### <a name="basic"></a> Basic Operations

These example shows how to carry out basic operations against a NamedMap or NamedCache.

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

This examples uses a `NamedMap` and issues `PutWithExpiry()` to expire a value after a certain time.

Source code: [basic/expiry/main.go](basic/expiry/main.go)

```go
go run basic/expiry/main.go
```

### <a name="querying"></a> Querying Data

These example shows how to query data operations against a NamedMap or NamedCache.

#### Querying Using Keys

Source code: [querying/keys/main.go](querying/keys/main.go)

```go
go run querying/keys/main.go
```

#### Querying Using Filters

Source code: [querying/filters/main.go](querying/filters/main.go)

```go
go run querying/filters/main.go
```

#### Querying All Data

> Note: When using open-ended queries, Coherence internally pages data to ensure that you are not
> returning all data in one large dataset. Care should still be taken to minimize occurrences of these queries
> on large caches.

Source code: [querying/main.go](querying/main.go)

```go
go run querying/main.go
```

### <a name="aggregations"></a> Aggregating Data

Source code: [aggregators/main.go](aggregators/main.go)

```go
go run aggregators/main.go
```

### <a name="processors"></a> Running Processors

This example shows how to run processors against a or NamedMap or NamedCache with a key of int and value of Person struct.

Source code: [processors/main.go](processors/main.go)

```go
go run processors/main.go
```

### <a name="map-events"></a> Listening for MapEvents

These examples show how to listen for events on a NamedMap or NamedCache.

#### Listening for All Cache Events

Source code: [events/cache/all/main.go](events/cache/all/main.go)

```go
go run events/cache/all/main.go
```

#### Listening for Cache Insert Events

Source code: [events/cache/insert/main.go](events/cache/insert/main.go)

```go
go run events/cache/insert/main.go
```

#### Listening for Cache Update Events

Source code: [events/cache/update/main.go](events/cache/update/main.go)

```go
go run events/cache/update/main.go
```

#### Listening for Cache Delete Events

Source code: [events/cache/delete/main.go](events/cache/delete/main.go)

```go
go run events/cache/delete/main.go
```

#### Listening for Cache Events using Filters

Source code: [events/cache/filters/main.go](events/cache/filters/main.go)

```go
go run events/cache/filters/main.go
```

#### Listening for Cache Events using Keys

Source code: [events/cache/key/main.go](events/cache/key/main.go)

```go
go run events/cache/key/main.go
```

### <a name="lifecycle-events"></a> Listening for Map Lifecycle Events

#### Listening for Truncate Cache Events

Source code: [events/lifecycle/truncated/main.go](events/lifecycle/truncated/main.go)

```go
go run events/lifecycle/truncated/main.go
```
#### Listening for Destroyed Cache Lifecycle Events

Source code: [events/lifecycle/destroyed/main.go](events/lifecycle/destroyed/main.go)

```go
go run events/lifecycle/destroyed/main.go
``````

#### Listening for Released Cache Lifecycle Events

Source code: [events/lifecycle/released/main.go](events/lifecycle/released/main.go)

```go
go run events/lifecycle/released/main.go
```

#### Listening for All Cache Lifecycle Events

Source code: [events/lifecycle/all/main.go](events/lifecycle/all/main.go)

```go
go run events/lifecycle/all/main.go
```

### <a name="session-lifecycle-events"></a> Listening for Session Lifecycle Events

#### Listening for All Session Events

Source code: [events/session/all/main.go](events/session/all/main.go)

```go
go run events/session/all/main.go

```
### <a name="indexes"></a> Adding Indexes

Source code: [indexes/main.go](indexes/main.go)

```go
go run indexes/main.go
```