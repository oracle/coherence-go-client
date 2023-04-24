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

echo "Coherence CE 22.06.4"
COHERENCE_VERSION=22.06.4 PROFILES=,-jakarta,javax make clean generate-proto build-test-images test-e2e-standalone

echo "Coherence CE 22.06.4 with scope"
COHERENCE_VERSION=22.06.4 PROFILES=,-jakarta,javax,scope make clean generate-proto build-test-images test-e2e-standalone-scope

echo "Coherence CE 22.06.4 with SSL"
SECURE=true COHERENCE_IGNORE_INVALID_CERTS=true \
  COHERENCE_TLS_CERTS_PATH=`pwd`/test/utils/certs/guardians-ca.crt \
  COHERENCE_TLS_CLIENT_CERT=`pwd`/test/utils/certs/star-lord.crt \
  COHERENCE_TLS_CLIENT_KEY=`pwd`/test/utils/certs/star-lord.key \
  COHERENCE_VERSION=22.06.4 PROFILES=,secure make clean certs generate-proto build-test-images test-e2e-standalone

echo "Coherence CE 23.03"
COHERENCE_BASE_IMAGE=gcr.io/distroless/java17 PROFILES=,jakarta,-javax COHERENCE_VERSION=23.03 make clean generate-proto build-test-images test-e2e-standalone

