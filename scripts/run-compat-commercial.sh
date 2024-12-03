#!/bin/bash

#
# Copyright (c) 2022, 2024 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#

# Run compatability tests for commercial
set -e

# Coherence 14.1.1.2206.6 - javax
echo "Coherence GE 14.1.1.2206.7"
COHERENCE_VERSION=14.1.1-2206-7 COHERENCE_GROUP_ID=com.oracle.coherence make clean generate-proto build-test-images test-e2e-standalone

echo "Coherence GE 15.1.1-0-0-SNAPSHOT" - Jakarta
PROFILES=,jakarta,-javax COHERENCE_BASE_IMAGE=gcr.io/distroless/java17 COHERENCE_VERSION=15.1.1-0-0-SNAPSHOT COHERENCE_GROUP_ID=com.oracle.coherence make clean generate-proto build-test-images test-e2e-standalone

COHERENCE_BASE_IMAGE=gcr.io/distroless/java17 PROFILES=,-jakarta,javax COHERENCE_GROUP_ID=com.oracle.coherence COHERENCE_VERSION=14.1.2-0-1-SNAPSHOT make clean generate-proto generate-proto-v1 build-test-images test-e2e-standalone

COHERENCE_BASE_IMAGE=gcr.io/distroless/java17 PROFILES=,-jakarta,javax,queues COHERENCE_GROUP_ID=com.oracle.coherence COHERENCE_VERSION=14.1.2-0-1-SNAPSHOT make clean generate-proto generate-proto-v1 build-test-images test-e2e-standalone-queues
