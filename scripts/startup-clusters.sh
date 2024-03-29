#!/bin/bash

#
# Copyright (c) 2022, 2023 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#

# Startup Coherence clusters for use with discovery tests
# $1 = coherence jar
# $2 = logs dir
# $3 = cluster port

set -e

LOGSDIR=$1
CLUSTER_PORT=$2
COHERENCE_GROUP_ID=$3
COHERENCE_VERSION=$4
TAG="shutMeDownPlease"
CLUSTERS=3

PROFILE="-P-javax,-jakarta,"
if [ "$COHERENCE_GROUP_ID" == "com.oracle.coherence" ] ; then
   PROFILE="-P commercial"
fi

COHERENCE_STARTUP="-Dcoherence.wka=127.0.0.1 -Dcoherence.ttl=0 -Dcoherence.clusterport=${CLUSTER_PORT} -D${TAG} -Dcoherence.management.http=all -Dcoherence.management.http.port=0"

set -x
echo "Generating Classpath..."
CP=`mvn -f java/coherence-go-test/pom.xml dependency:build-classpath -Dcoherence.group.id=${COHERENCE_GROUP_ID} -Dcoherence.version=${COHERENCE_VERSION} ${PROFILE} | sed -ne '/Dependencies classpath/,$ p' | sed '/INFO/d'`

echo "Starting $CLUSTERS clusters..."
for i in $(seq 1 $CLUSTERS)
do
  cluster=cluster${i}
  echo "Starting $cluster ..."
  java ${COHERENCE_STARTUP} -Dcoherence.cluster=$cluster -cp ${CP} com.tangosol.net.DefaultCacheServer > ${LOGSDIR}/${cluster}.log 2>&1 &
  sleep 3
done

sleep 30


