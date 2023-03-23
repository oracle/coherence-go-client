# Coherence Go Client Examples

This directory contains various examples on how to use the Coherence Go client.

## Clone the Coherence Go Client Repository

```bash
git clone github.com/coherence/coherence-go-client
cd coherence-go-client/examples
```

## Install the Coherence Go Client

```bash
go get github.com/coherence/coherence-go-client
````

## Start a Coherence Cluster

Before running any of the examples, you must ensure a Coherence cluster is available.
For local development, we recommend using the Coherence CE Docker image; it contains
everything necessary for the client to operate correctly.

```bash
docker run -d -p 1408:1408 -p 30000:30000 ghcr.io/oracle/coherence-ce:22.06.3
```

## Index

* [Basic operations using primitives and structs](#basic)
* [Querying Data](#querying)
* [Aggregating Data](#aggregations)
* [Running Processors](#processors)
* [Listening for MapEvents](#map-events)
* [Listening for Map Lifecycle Events](#lifecycle-events)
* [Listening for Session Lifecycle Events](#session-lifecycle-events)

## <a name="basic"></a> Basic Operations

These example shows how to carry out basic operations against a NamedMap or NamedCache.

### Using Primitive Types

This examples runs Put(), Get(), Remove() operations against a NamedMap or NamedCache with key in and value string.

Source code: [basic/crud.go](basic/crud.go)

```go
go run basic/crud.go
```

### Using Structs

Source code: [basic/struct.go](basic/struct.go)

```go
go run basic/struct.go
```

## <a name="querying"></a> Querying Data

These example shows how to query data operations against a NamedMap or NamedCache.

### Querying Using Keys

Source code: [querying/using_keys.go](querying/using_keys.go)

```go
go run querying/using_keys.go
```

### Querying Using Filters

Source code: [querying/using_filters.go](querying/using_filters.go)

```go
go run querying/using_filters.go
```

### Querying All Data

> Note: When using open-ended queries, Coherence internally pages data to ensure that you are not
> returning all data in one large dataset. Care should still be taken to minimize occurrences of these queries
> on large caches.

Source code: [querying/open_ended.go](querying/open_ended.go)

```go
go run querying/open_ended.go
```

## <a name="aggregations"></a> Aggregating Data

Source code: [aggregators/main.go](aggregators/main.go)

```go
go run aggregators/main.go
```

## <a name="processors"></a> Running Processors

This example shows how to run processors against a or NamedMap or NamedCache with a key of int and value of Person struct.

Source code: [processors/main.go](processors/main.go)

```go
go run processors/main.go
```

## <a name="map-events"></a> Listening for MapEvents

These examples show how to listen for events on a NamedMap or NamedCache.

### Listening for All Cache Events

Source code: [events/cache/all/all_events.go](events/cache/all/all_events.go)

```go
go run events/cache/all/all_events.go
```

### Listening for Cache Insert Events

Source code: [events/cache/insert/insert_events.go](events/cache/insert/insert_events.go)

```go
go run events/cache/insert/insert_events.go
```

### Listening for Cache Update Events

Source code: [events/cache/update/update_events.go](events/cache/update/update_events.go)

```go
go run events/cache/update/update_events.go
```

### Listening for Cache Delete Events

Source code: [events/cache/delete/delete_events.go](events/cache/delete/delete_events.go)

```go
go run events/cache/delete/delete_events.go
```

## <a name="lifecycle-events"></a> Listening for Map Lifecycle Events

### Listening for Truncate Cache Events

Source code: [events/lifecycle/truncated/truncated_events.go](events/lifecycle/truncated/truncated_events.go)

```go
go run events/lifecycle/truncated/truncated_events.go
```
### Listening for Destroyed Cache Lifecycle Events

Source code: [events/lifecycle/destroyed/destroyed_events.go](events/lifecycle/destroyed/destroyed_events.go)

```go
go run events/lifecycle/destroyed/destroyed_events.go
``````

### Listening for Released Cache Lifecycle Events

Source code: [events/lifecycle/released/released_events.go](events/lifecycle/released/released_events.go)

```go
go run events/lifecycle/released/released_events.go
```

### Listening for Released Cache Lifecycle Events

Source code: [events/lifecycle/released/released_events.go](events/lifecycle/released/released_events.go)

```go
go run events/lifecycle/released/released_events.go
```
### Listening for All Cache Lifecycle Events

Source code: [events/lifecycle/all/all_events.go](events/lifecycle/all/all_events.go)

```go
go run events/lifecycle/all/all_events.go
```


## <a name="session-lifecycle-events"></a> Listening for Session Lifecycle Events

### Listening for All Session Events

Source code: [events/session/all/all_events.go](events/session/all/all_events.go)

```go
go run events/session/all/all_events.go
```