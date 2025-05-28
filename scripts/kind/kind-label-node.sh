#!/usr/bin/env bash

#
# Copyright (c) 2020, 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# http://oss.oracle.com/licenses/upl.
#

PREFIX=go-client

kubectl label node ${PREFIX}-worker topology.kubernetes.io/zone=zone-one --overwrite
kubectl label node ${PREFIX}-worker topology.kubernetes.io/region=one --overwrite
kubectl label node ${PREFIX}-worker oci.oraclecloud.com/fault-domain=fd-one --overwrite
kubectl label node ${PREFIX}-worker coherence.oracle.com/site=site-one --overwrite
kubectl label node ${PREFIX}-worker coherence.oracle.com/rack=rack-one --overwrite
kubectl label node ${PREFIX}-worker2 topology.kubernetes.io/zone=zone-two --overwrite || true
kubectl label node ${PREFIX}-worker2 topology.kubernetes.io/region=two --overwrite || true
kubectl label node ${PREFIX}-worker2 oci.oraclecloud.com/fault-domain=fd-two --overwrite || true
kubectl label node ${PREFIX}-worker2 coherence.oracle.com/site=site-two --overwrite || true
kubectl label node ${PREFIX}-worker2 coherence.oracle.com/rack=rack-two --overwrite || true
kubectl label node ${PREFIX}-worker3 topology.kubernetes.io/zone=zone-three --overwrite || true
kubectl label node ${PREFIX}-worker3 topology.kubernetes.io/region=three --overwrite || true
kubectl label node ${PREFIX}-worker3 oci.oraclecloud.com/fault-domain=fd-three --overwrite || true
kubectl label node ${PREFIX}-worker3 coherence.oracle.com/site=site-three --overwrite || true
kubectl label node ${PREFIX}-worker3 coherence.oracle.com/rack=rack-three --overwrite || true

