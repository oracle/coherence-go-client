//
// Copyright (c) 2022, 2025 Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at
// https://oss.oracle.com/licenses/upl.
//
module github.com/oracle/coherence-go-client/v2/test

go 1.23.0

toolchain go1.23.7

require (
	github.com/google/uuid v1.6.0
	github.com/onsi/gomega v1.37.0
	github.com/oracle/coherence-go-client/v2 v2.3.0
)

require (
	github.com/google/go-cmp v0.7.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250528174236-200df99c418a // indirect
	google.golang.org/grpc v1.73.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/oracle/coherence-go-client/v2 => ../
