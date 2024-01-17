#!/bin/bash

#
# Copyright (c) 2022, 2023 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#

# Run compatability tests
set -e

# Set the following to include long running streaming tests
# INCLUDE_LONG_RUNNING=true

echo "Coherence CE 22.06.7"
COHERENCE_VERSION=22.06.7 PROFILES=,-jakarta,javax make clean generate-proto build-test-images test-e2e-standalone

echo "Coherence CE 22.06.7 with scope"
COHERENCE_VERSION=22.06.7 PROFILES=,-jakarta,javax,scope make clean generate-proto build-test-images test-e2e-standalone-scope

echo "Coherence CE 22.06.7 with SSL using env"
SECURE=env COHERENCE_IGNORE_INVALID_CERTS=true \
  COHERENCE_TLS_CERTS_PATH=`pwd`/test/utils/certs/guardians-ca.crt \
  COHERENCE_TLS_CLIENT_CERT=`pwd`/test/utils/certs/star-lord.crt \
  COHERENCE_TLS_CLIENT_KEY=`pwd`/test/utils/certs/star-lord.key \
  COHERENCE_VERSION=22.06.7 PROFILES=,secure make clean certs generate-proto build-test-images test-e2e-standalone

echo "Coherence CE 22.06.7 with SSL using options"
# suite_test.go takes the below env vars and then populates the session options
SECURE=options COHERENCE_IGNORE_INVALID_CERTS_OPTION=true \
  COHERENCE_TLS_CERTS_PATH_OPTION=`pwd`/test/utils/certs/guardians-ca.crt \
  COHERENCE_TLS_CLIENT_CERT_OPTION=`pwd`/test/utils/certs/star-lord.crt \
  COHERENCE_TLS_CLIENT_KEY_OPTION=`pwd`/test/utils/certs/star-lord.key \
  COHERENCE_VERSION=22.06.7 PROFILES=,secure make clean certs generate-proto build-test-images test-e2e-standalone

echo "Coherence CE 22.06.7 with SSL using tlsConfig"
# suite_test.go takes the below env vars and then creates a tls.Config and passes to  the WithTLSConfig
SECURE=tlsConfig COHERENCE_IGNORE_INVALID_CERTS_OPTION=true \
  COHERENCE_TLS_CERTS_PATH_OPTION=`pwd`/test/utils/certs/guardians-ca.crt \
  COHERENCE_TLS_CLIENT_CERT_OPTION=`pwd`/test/utils/certs/star-lord.crt \
  COHERENCE_TLS_CLIENT_KEY_OPTION=`pwd`/test/utils/certs/star-lord.key \
  COHERENCE_VERSION=22.06.7 PROFILES=,secure make clean certs generate-proto build-test-images test-e2e-standalone

echo "Coherence CE 23.09.1"
COHERENCE_BASE_IMAGE=gcr.io/distroless/java17 PROFILES=,jakarta,-javax COHERENCE_VERSION=23.09.1 make clean generate-proto build-test-images test-e2e-standalone

