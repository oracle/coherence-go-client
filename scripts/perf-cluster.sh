#!/bin/bash

#
# Copyright (c) 2025 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#

# Run Performance Test
# environment variables COM accepted
# Arguments:
# 1 - directory for cohctl config
# 2 - coherence version
# 3 - start or stop
pwd

if [ $# -ne 3 ] ; then
   echo "Usage: $0 directory Coherence-Version [start|stop]"
   exit
fi

CONFIG_DIR=$1
VERSION=$2
COMMAND=$3

if [ ! -d $CONFIG_DIR ]; then
  echo "${CONFIG_DIR} is not a directory"
  exit 1
fi

DIR=`pwd`
OUTPUT=`mktemp`

mkdir -p ${CONFIG_DIR}
trap "rm -rf ${OUTPUT}" EXIT SIGINT

echo
echo "Config Dir: ${CONFIG_DIR}"
echo "Version:    ${VERSION}"
echo "Commercial: ${COM}"
echo

type cohctl
ret=$?
if [ $ret -ne 0 ]; then
  echo "cohctl must be in path"
  exit 1
fi

# Build the Java project so we get any deps downloaded

COHERENCE_GROUP_ID=com.oracle.coherence.ce
if [ ! -z "$COM" ] ; then
  COHERENCE_GROUP_ID=com.oracle.coherence
fi

# Default command
COHCTL="cohctl --config-dir ${CONFIG_DIR}"

function pause() {
   echo "sleeping..."
   sleep 5
}

function message() {
    echo "========================================================="
    echo "$*"
}

function save_logs() {
    mkdir -p build/_output/test-logs
    cp ${CONFIG_DIR}/logs/local/*.log build/_output/test-logs || true
}

function runCommand() {
    echo "========================================================="
    echo "Running command: cohctl $*"
    $COHCTL $* > $OUTPUT 2>&1
    ret=$?
    cat $OUTPUT
    if [ $ret -ne 0 ] ; then
      echo "Command failed"
      # copy the log files
      save_logs
      exit 1
    fi
}

runCommand version
runCommand set debug on

if [ "${COMMAND}" == "start" ]; then
  # Create a cluster
  message "Create Cluster"
  runCommand create cluster local -y -v $VERSION $COM -S com.tangosol.net.Coherence -a coherence-grpc,coherence-grpc-proxy --machine machine1 -M 2g
  runCommand monitor health -n localhost:7574 -I -T 120 -w
elif [ "${COMMAND}" == "stop" ]; then
  runCommand stop cluster local -y
  runCommand remove cluster local -y
elif [ "${COMMAND}" == "status" ]; then
  runCommand get members
else
  echo "Invalid command ${COMMAND}"
  exit 1
fi
