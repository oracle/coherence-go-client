#!/bin/bash

#
# Copyright (c) 2025 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#

# $3 = true to enable TLS test

set -e

if [ $# -lt 2 ]; then
   echo "You must specify the dapr test directory and dapr dir to install to"
   exit 1
fi

TLS=false

if [ $# -eq 3 -a "$3" == "true" ]; then
  TLS=true
fi

DIR=`pwd`
DAPR_TEST_DIR=$1
DAPR_TEST_HOME=$2

mkdir -p $DAPR_TEST_HOME

echo "DAPR Test Dir:  $DAPR_TEST_DIR"
echo "DAPR Test Home: $DAPR_TEST_HOME"
echo "TLS:            $TLS"

echo "Install DAPR"
OS=`uname`

echo "Listing docker images"
docker ps

if [ "$OS" == "Linux" ]; then
   wget -q https://raw.githubusercontent.com/dapr/cli/master/install/install.sh -O - | /bin/bash
   curl -sL https://raw.githubusercontent.com/oracle/coherence-cli/main/scripts/install.sh | bash
   cohctl version
   # Wait for coherence
   cohctl monitor health -e http://127.0.0.1:6676/,http://127.0.0.1:6677/ -T 120 -w -I
   cohctl add cluster default -u http://127.0.0.1:30000/management/coherence/cluster
   cohctl version
else
   echo "Assuming installed"
   type dapr
fi

dapr init
dapr version
ls -l ~/.dapr

echo
echo "Cloning repositories..."
cd $DAPR_TEST_HOME
rm -rf dapr || true
git clone https://github.com/dapr/dapr.git
cd dapr

# Test with the current go client
go mod edit -replace github.com/oracle/coherence-go-client/v2=../../../..

echo "Building dapr core..."
make modtidy-all
make DEBUG=1 build

export DAPR_DIST=$(echo dist/*/debug)
DAPR_HOME=$DAPR_TEST_HOME/dapr/$DAPR_DIST
DAPR_BIN=$DAPR_HOME/daprd

ls -l $DAPR_BIN

echo "Install $DAPR_BIN to ~/.dapr/bin"
cp $DAPR_BIN ~/.dapr/bin

echo "Running Test"

cd $DAPR_TEST_DIR/my-dapr-app

go mod tidy

COMPONENTS=./components/
if [ "$TLS" == "true" ]; then
  COMPONENTS=./components-tls/
  echo "DIR = $DIR"
  pwd
  ls -l ../../utils/certs/guardians-ca.crt
fi

echo "Running DAPR with component $COMPONENTS"

dapr run --app-id myapp --resources-path $COMPONENTS --log-level debug  -- go run main.go

# Verify the caches
cohctl get caches -o wide




