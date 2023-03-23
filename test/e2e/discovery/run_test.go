/*
 * Copyright (c) 2022, 2023, Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence/discovery"
	"github.com/oracle/coherence-go-client/test/utils"
	"strings"
	"testing"
)

// TestNSLookupCommands tests for discovery
func TestNSLookupCommands(t *testing.T) {
	var (
		g            = gomega.NewGomegaWithT(t)
		result       string
		clusterPorts []discovery.ClusterNSPort
	)

	// sleep to ensure the clusters are ready
	utils.Sleep(20)

	ns, err := discovery.Open("127.0.0.1:7574", 5)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	defer func() {
		_ = ns.Close()
	}()

	// lookup the first cluster which should always be cluster1 due to startup order
	result, err = ns.Lookup(discovery.ClusterNameLookup)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(strings.Contains(result, "cluster1")).To(gomega.Equal(true))

	// lookup the foreign clusters
	result, err = ns.Lookup(discovery.NSPrefix + discovery.ClusterForeignLookup)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(strings.Contains(result, "cluster2")).To(gomega.Equal(true))
	g.Expect(strings.Contains(result, "cluster3")).To(gomega.Equal(true))

	// get the cluster info
	result, err = ns.Lookup(discovery.ClusterInfoLookup)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(strings.Contains(result, "127.0.0.1")).To(gomega.Equal(true))
	g.Expect(strings.Contains(result, "ServiceJoined")).To(gomega.Equal(true))

	// discover name service ports
	clusterPorts, err = ns.DiscoverNameServicePorts()
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(len(clusterPorts)).To(gomega.Equal(3))

	// close the original ns lookup
	closeSilent(ns)

	count := 0
	// validate at least each cluster is there and lookup details for each
	for _, v := range clusterPorts {
		if v.ClusterName == "cluster1" || v.ClusterName == "cluster2" || v.ClusterName == "cluster3" {
			count++
		}

		var (
			nsNew             *discovery.NSLookup
			addressPort       = fmt.Sprintf("%s:%d", v.HostName, v.Port)
			discoveredCluster discovery.DiscoveredCluster
		)

		// connect to the discovered cluster
		nsNew, err = discovery.Open(addressPort, 5)
		g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

		discoveredCluster, err = nsNew.DiscoverClusterInfo()
		g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
		g.Expect(discoveredCluster.Host).To(gomega.Equal(v.HostName))
		g.Expect(discoveredCluster.ClusterName).To(gomega.Equal(v.ClusterName))
		g.Expect(discoveredCluster.NSPort).To(gomega.Equal(v.Port))
		g.Expect(discoveredCluster.NSPort).To(gomega.Equal(v.Port))
		g.Expect(len(discoveredCluster.MetricsURLs)).To(gomega.Equal(0))
		g.Expect(len(discoveredCluster.ManagementURLs)).To(gomega.Equal(1))
		g.Expect(len(discoveredCluster.JMXURLs)).To(gomega.Equal(1))

		closeSilent(nsNew)
	}
	g.Expect(count).To(gomega.Equal(3))
}

// closeSilent closes a NsLookup connection and ignores if it is nil
func closeSilent(ns *discovery.NSLookup) {
	if ns != nil {
		_ = ns.Close()
	}
}
