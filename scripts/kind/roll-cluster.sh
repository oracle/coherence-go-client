#!/bin/sh
#
# Copyright (c) 2020, 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# http://oss.oracle.com/licenses/upl.
#

set -o errexit
maxIterations=10

if [ ! -z "$MAX_ITERATIONS" ]; then
  maxIterations=$MAX_ITERATIONS
fi

if [ -z "$NAMESPACE" ]; then
  NAMESPACE=coherence-perf
fi

echo "`date`: Starting rolling restart, iterations=$maxIterations"

for i in $(seq 1 $maxIterations); do

  echo "`date`: Iteration $i of $maxIterations"

  if [ $((i%2)) -eq 0 ]; then
    INITIAL_HEAP=1000m
  else
    INITIAL_HEAP=1001m
  fi

  export INITIAL_HEAP

  envsubst < ./scripts/kind/coherence-cluster.yaml | kubectl -n $NAMESPACE apply -f -

  echo "`date`: Waiting for cluster to be updated..."
  kubectl rollout status statefulset/perf-cluster -n $NAMESPACE --timeout=300s

  echo "`date`: Sleeping 10..."
  sleep 10
done


