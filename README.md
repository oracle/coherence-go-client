# Coherence Go Client

![Coherence Go Client](https://github.com/oracle/coherence-go-client/workflows/CI/badge.svg?branch=main)
[![License](http://img.shields.io/badge/license-UPL%201.0-blue.svg)](https://oss.oracle.com/licenses/upl/)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=oracle_coherence-go-client&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=oracle_coherence-go-client)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=oracle_coherence-go-client&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=oracle_coherence-go-client)

[![Go Report Card](https://goreportcard.com/badge/github.com/oracle/coherence-go-client)](https://goreportcard.com/report/github.com/oracle/coherence-go-client)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/oracle/coherence-go-client)

<img src=https://oracle.github.io/coherence/assets/images/logo-red.png width="30%"><img>

The Coherence Go Client allows Go applications to act as cache clients 
to a Coherence Cluster using Google's gRPC framework for the network transport.

#### Features

* Familiar Map-like interface for manipulating cache entries based upon the full Coherence API
* Cluster-side querying, aggregation and filtering of map entries
* Cluster-side manipulation of map entries using EntryProcessors
* Registration of listeners to be notified of:
  * mutations such as insert, update and delete on Maps
  * map lifecycle events such as truncated, released or destroyed
  * session lifecycle events such as connected, disconnected, reconnected and closed 
* Support for storing Go structs as JSON as well as the ability to serialize to Java objects on the server for access from other language API's

#### Requirements

* Coherence CE 22.06.3+ or Coherence 14.1.1.2206.4 Commercial edition with a configured [gRPCProxy](https://docs.oracle.com/en/middleware/standalone/coherence/14.1.1.2206/develop-remote-clients/using-coherence-grpc-server.html).
* Go 1.19.+

#### <a name="start"></a> Starting a gRPC enabled Coherence cluster

Before testing the Go client, you must ensure a Coherence cluster is available. 
For local development, we recommend using the Coherence CE Docker image; it contains 
everything necessary for the client to operate correctly.

```bash
docker run -d -p 1408:1408 ghcr.io/oracle/coherence-ce:22.06.3
```

## <a name="installations"></a> Installation

```bash
go get github.com/coherence/coherence-go-client
````
After executing this command coherence-go-client is ready to use, and it's source will be in:

```bash
$GOPATH/src/github.com/coherence/coherence-go-client
```

## <a name="doc"></a>Documentation

TBC

## <a name="examples"></a>Examples

For details examples, please see the [Examples](examples).


The following example connects to a Coherence cluster running gRPC Proxy on default
port of 1408, creates a new `NamedMap` with key `int` and value of a `Person` and
issues `Put()`, `Get()` and `Size()` operations.

```go
import (
  "context"	
  "fmt"
  "github.com/oracle/coherence-go-client/coherence"
  "log"
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

    // create a new NamedMap with key of int and value of string
    namedMap, err := coherence.NewNamedMap[int, string](session, "my-map")
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

We have a **public Slack channel** where you can get in touch with us to ask questions about using the Coherence CLI
or give us feedback or suggestions about what features and improvements you would like to see. We would love
to hear from you. To join our channel,
please [visit this site to get an invitation](https://join.slack.com/t/oraclecoherence/shared_invite/enQtNzcxNTQwMTAzNjE4LTJkZWI5ZDkzNGEzOTllZDgwZDU3NGM2YjY5YWYwMzM3ODdkNTU2NmNmNDFhOWIxMDZlNjg2MzE3NmMxZWMxMWE).  
The invitation email will include details of how to access our Slack
workspace.  After you are logged in, please come to `#coherence` and say, "hello!"

If you would like to raise an issue please see [here](https://github.com/oracle/coherence-go-client/issues/new/choose).

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process

## License

Copyright (c) 2022, 2023 Oracle and/or its affiliates.

Released under the Universal Permissive License v1.0 as shown at
<https://oss.oracle.com/licenses/upl/>.

