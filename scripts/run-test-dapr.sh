#!/bin/bash

#
# Copyright (c) 2025 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#

if [ $# -ne 2 ]; then
   echo "You must specify the dapr test directory and dapr dir to install to"
   exit 1
fi

DAPR_TEST_DIR=$1
DAPR_TEST_HOME=$2

mkdir -p $DAPR_TEST_HOME

echo "DAPR Test Dir:  $DAPR_TEST_DIR"
echo "DAPR Test Home: $DAPR_TEST_HOME"

echo "Install DAPR"
OS=`uname`

if [ "$OS" == "Linux" ]; then
   wget -q https://raw.githubusercontent.com/dapr/cli/master/install/install.sh -O - | /bin/bash
   curl -sL https://raw.githubusercontent.com/oracle/coherence-cli/main/scripts/install.sh | bash
   cohctl version
   cohctl add cluster default -u http://localhost:30000/management/coherence/cluster
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
rm -rf components-contrib dapr || true
git clone https://github.com/dapr/components-contrib.git
git clone https://github.com/dapr/dapr.git
cd dapr
go mod edit -replace github.com/dapr/components-contrib=../components-contrib

# Temporary workaround until coherence in DAPR core

cat > cmd/daprd/components/state_coherence.go <<EOF
//go:build allcomponents

/*
Copyright 2025 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package components

import (
	coherence "github.com/dapr/components-contrib/state/coherence"
	stateLoader "github.com/dapr/dapr/pkg/components/state"
)

func init() {
	stateLoader.DefaultRegistry.RegisterComponent(coherence.NewCoherenceStateStore, "coherence")
}
EOF

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

cd $DAPR_TEST_DIR

cd my-dapr-app
go mod tidy

dapr run --app-id myapp --resources-path ./components/ --log-level debug  -- go run main.go

# Verify the caches
cohctl get cache -o wide




