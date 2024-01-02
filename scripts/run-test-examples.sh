#!/bin/bash

#
# Copyright (c) 2022, 2024 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#


function pause() {
   echo "sleeping..."
   sleep 5
}

function wait_for_ready() {
  counter=10
  PORT=$1
  if [ -z "$PORT" ] ; then
    PORT=30000
  fi
  pause
  echo "waiting for management to be ready on ${PORT}..."
  while [ $counter -gt 0 ]
  do
    curl http://127.0.0.1:${PORT}/management/coherence/cluster > /dev/null 2>&1
    ret=$?
    if [ $ret -eq 0 ] ; then
        echo "Management ready"
        pause
        return 0
    fi
    pause
    let counter=counter-1
  done
  echo "Management failed to be ready"
  save_logs
  exit 1
}

wait_for_ready

set -e
cd examples

find . -type f -name '*.go' | grep -v people_listen | grep -v people_insert | grep -v doc.go | grep -v rest | while read file
do
  echo
  echo "==========================================="
  echo $file
  echo "==========================================="

  go run -race $file
done

# Special case for REST server example
go run -race rest/main.go &
PID=$!

sleep 10

# Get all PIDS
ALL_PIDS=`ps -ef | grep $PID | grep -v grep | awk '{print $2}' | tr '\n' ' '`
echo "PIDS: ALL_PIDS"

trap "kill -9 $ALL_PIDS || true" EXIT SIGINT SIGQUIT

curl -X GET -i http://localhost:17268/people | grep Address
curl -X GET -i http://localhost:17268/people/1 | grep '"id":1'
curl -X DELETE -i http://localhost:17268/people/1
curl -X GET -i http://localhost:17268/people/1 | grep 404
curl -X POST -i http://localhost:17268/people/1 -d '{"id":1,"name":"Person-1","address":"Address 1","city":"Adelaide","age":16}'
curl -X GET -i http://localhost:17268/people/1 | grep Address
curl -X PUT -i http://localhost:17268/people/1 -d '{"id":1,"name":"Person-1","address":"Address 1","city":"Singapore","age":16}'
curl -X GET -i http://localhost:17268/people/1 | grep Singapore
