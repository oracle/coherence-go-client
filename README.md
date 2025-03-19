# Coherence Go Client

![Coherence Go Client](https://github.com/oracle/coherence-go-client/workflows/CI/badge.svg?branch=main)
[![License](http://img.shields.io/badge/license-UPL%201.0-blue.svg)](https://oss.oracle.com/licenses/upl/)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=oracle_coherence-go-client&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=oracle_coherence-go-client)

[![Go Report Card](https://goreportcard.com/badge/github.com/oracle/coherence-go-client)](https://goreportcard.com/report/github.com/oracle/coherence-go-client)
[![GitHub release (latest by date)](https://img.shields.io/github/v/release/oracle/coherence-go-client)](https://github.com/oracle/coherence-go-client/releases)
<a href="https://pkg.go.dev/github.com/oracle/coherence-go-client/v2/coherence"><img src="https://pkg.go.dev/badge/go get github.com/oracle/coherence-go-client.svg" alt="Go Reference"></a>

The Coherence Go Client allows Go applications to act as cache clients 
to a Coherence Cluster using gRPC for the network transport.

> [Coherence](https://coherence.community/) is a scalable, fault-tolerant, cloud-ready, distributed platform for building grid-based applications and reliably storing data. The product is used at scale, for both compute and raw storage, in a vast array of industries such as critical financial trading systems, high performance telecommunication products and eCommerce applications.

#### Features

* Familiar Map-like interface for manipulating cache entries including but not limited to:
  * `Put`, `PutWithExpiry`, `PutIfAbsent`, `PutAll`, `Get`, `GetAll`, `Remove`, `Clear`, `GetOrDefault`, `Replace`, `ReplaceMapping`, `Size`, `IsEmpty`, `ContainsKey`, `ContainsValue`, `ContainsEntry`
* Cluster-side querying, aggregation and filtering of map entries
* Cluster-side manipulation of map entries using EntryProcessors
* Registration of listeners to be notified of:
  * mutations such as insert, update and delete on Maps
  * map lifecycle events such as truncated, released or destroyed
  * session lifecycle events such as connected, disconnected, reconnected and closed 
* Support for storing Go structs as JSON as well as the ability to serialize to Java objects on the server for access from other Coherence language API's 
* Near cache support to cache frequently accessed data in the Go client to avoid sending requests across the network
* Support for simple and double-ended queues in Coherence Community Edition 24.09+ and commercial version 14.1.2.0+
* Full support for Go generics in all Coherence API's

#### Requirements

* Coherence CE 22.06.4+, 24.09+ or Coherence 14.1.1.2206.4+ Commercial edition with a configured [gRPCProxy](https://docs.oracle.com/en/middleware/standalone/coherence/14.1.1.2206/develop-remote-clients/using-coherence-grpc-server.html).
* Go 1.23.+

> Note: If you wish to use the queues API in the latest release, you must use CE 24.09 or commercial version 14.1.2.0.x.

> Note: If you wish to use Go versions < 1.23 you can use v2.0.0 of the client.

#### <a name="start"></a> Starting a gRPC enabled Coherence cluster

Before testing the Go client, you must ensure a Coherence cluster is available. 
For local development, we recommend using the Coherence CE Docker image; it contains 
everything necessary for the client to operate correctly.

```bash
docker run -d -p 1408:1408 -p 30000:30000 ghcr.io/oracle/coherence-ce:24.09.3
```

## Installation

```bash
go get github.com/oracle/coherence-go-client/v2@latest
````

## <a name="doc"></a>Documentation

* [Go Client API Reference for Oracle Coherence](https://pkg.go.dev/github.com/oracle/coherence-go-client/v2/coherence)
* [FAQ Page](https://github.com/oracle/coherence-go-client/wiki/FAQ)
* [Troubleshooting Guide](https://github.com/oracle/coherence-go-client/wiki/Troubleshooting)

## <a name="examples"></a>Examples

For a comprehensive set of Go client API examples, please see [examples](examples).

The following example connects to a Coherence cluster running gRPC Proxy on default
port of 1408, creates a new `NamedMap` with key `int` and value of a `string` and
issues `Put()`, `Get()` and `Size()` operations.

> Note: Keys and values can also be Go `structs`. See detailed examples [here](examples#basic).

```go
package main

import (
    "context"
    "fmt"
    "github.com/oracle/coherence-go-client/v2/coherence"
)

func main() {
    var (
        value *string
        ctx   = context.Background()
    )

    // create a new Session to the default gRPC port of 1408
    session, err := coherence.NewSession(ctx, coherence.WithPlainText())
    if err != nil {
        panic(err)
    }
    defer session.Close()

    // get a NamedMap with key of int and value of string
    namedMap, err := coherence.GetNamedMap[int, string](session, "my-map")
    if err != nil {
        panic(err)
    }

    // put a new key / value
    if _, err = namedMap.Put(ctx, 1, "one"); err != nil {
        panic(err)
    }
	
    // get the value for key 1
    if value, err = namedMap.Get(ctx, 1); err != nil {
       panic(err)
    }
    fmt.Println("Value for key 1 is", *value)

    // update the value for key 1
    if _, err = namedMap.Put(ctx, 1, "ONE"); err != nil {
        panic(err)
    }

    // get the updated value for key 1
    if value, err = namedMap.Get(ctx, 1); err != nil {
        panic(err)
    }
    fmt.Println("Updated value is", *value)

    // remove the entry
    if _, err = namedMap.Remove(ctx, 1); err != nil {
        panic(err)
    }
}
```

## Help

We have a **public Slack channel** where you can get in touch with us to ask questions about using the Coherence Go client as well as give us feedback or suggestions about what features and improvements you would like to see. We would love
to hear from you. To join our channel,
please [visit this site to get an invitation](https://join.slack.com/t/oraclecoherence/shared_invite/enQtNzcxNTQwMTAzNjE4LTJkZWI5ZDkzNGEzOTllZDgwZDU3NGM2YjY5YWYwMzM3ODdkNTU2NmNmNDFhOWIxMDZlNjg2MzE3NmMxZWMxMWE).  
The invitation email will include details of how to access our Slack
workspace.  After you are logged in, please come to `#coherence` and say, "hello!"

If you would like to raise an issue please see [here](https://github.com/oracle/coherence-go-client/issues/new/choose).

You may also find your question is already answered on our [FAQ](https://github.com/oracle/coherence-go-client/wiki/FAQ) or
[troubleshooting](https://github.com/oracle/coherence-go-client/wiki/Troubleshooting) Wiki.

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process.

## License

Copyright (c) 2024 Oracle and/or its affiliates.

Released under the Universal Permissive License v1.0 as shown at
<https://oss.oracle.com/licenses/upl/>.

