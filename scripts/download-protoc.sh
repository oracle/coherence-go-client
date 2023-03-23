#!/bin/bash

#
# Copyright (c) 2022, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#
# Download Protoc

if [ $# -ne 1 ] ; then
   echo "Please supply tools directory"
   exit 1
fi

export TOOLS_DIRECTORY=$1
set -e

if [ "`uname -a | grep Darwin`" ] ; then
   curl -Lo ${TOOLS_DIRECTORY}/file.zip https://github.com/protocolbuffers/protobuf/releases/download/v3.19.2/protoc-3.19.2-osx-x86_64.zip
else
	  curl -Lo ${TOOLS_DIRECTORY}/file.zip https://github.com/protocolbuffers/protobuf/releases/download/v3.19.2/protoc-3.19.2-linux-x86_64.zip
fi

cd ${TOOLS_DIRECTORY}
unzip -od ${TOOLS_DIRECTORY} ${TOOLS_DIRECTORY}/file.zip