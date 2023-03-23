#!/bin/bash

#
# Copyright (c) 2022 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#

# Run compatability tests for commercial
set -e

# Coherence 14.1.1.2206.x - javax
echo "Coherence GE 14.1.1.2206.2"
COHERENCE_VERSION=14.1.1-2206-2 COHERENCE_GROUP_ID=com.oracle.coherence make clean generate-proto build-test-images test-e2e-standalone

echo "Coherence GE 15.1.1-0-0-SNAPSHOT" - Jakarta
PROFILES=,jakarta,-javax COHERENCE_BASE_IMAGE=gcr.io/distroless/java17 COHERENCE_VERSION=15.1.1-0-0-SNAPSHOT COHERENCE_GROUP_ID=com.oracle.coherence make clean generate-proto build-test-images test-e2e-standalone

