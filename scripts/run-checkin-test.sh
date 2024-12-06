#!/bin/bash

#
# Copyright (c) 2022, 2024 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#

# This script runs some tests that should succeed to be sure we can push
set -e

echo "Coherence CE 24.09 All Tests gRPC v1"
COHERENCE_BASE_IMAGE=gcr.io/distroless/java17 PROFILES=,jakarta,-javax COHERENCE_VERSION=24.09 make clean generate-proto generate-proto-v1 build-test-images test-e2e-standalone

echo "Coherence CE 24.09 with queues"
COHERENCE_BASE_IMAGE=gcr.io/distroless/java17 PROFILES=,jakarta,-javax,queues COHERENCE_VERSION=24.09 make clean generate-proto generate-proto-v1 build-test-images test-e2e-standalone-queues

echo "Coherence CE 22.06.10"
COHERENCE_VERSION=22.06.10 PROFILES=,-jakarta,javax make clean generate-proto build-test-images test-e2e-standalone