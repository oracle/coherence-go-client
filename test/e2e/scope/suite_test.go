/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package scope

import (
	"fmt"
	"github.com/oracle/coherence-go-client/test/utils"
	"os"
	"testing"
)

const secure = "SECURE"

// The entry point for the test suite
func TestMain(m *testing.M) {
	var (
		httpPort   = 30000
		restPort   = 8080
		err        error
		exitCode   int
		grpcPort   = 1408
		secureMode string
	)

	if val := os.Getenv(secure); val != "" {
		secureMode = val
	}

	context := utils.TestContext{ClusterName: "cluster1", GrpcPort: grpcPort, HTTPPort: httpPort,
		URL: utils.GetManagementURL(httpPort), ExpectedServers: 2, RestURL: utils.GetRestURL(restPort),
		HostName: "127.0.0.1", SecureMode: secureMode}
	utils.SetTestContext(&context)

	fileName := utils.GetFilePath("docker-compose-2-members.yaml")
	err = utils.StartCoherenceCluster(fileName, context.URL)
	if err != nil {
		fmt.Println(err)
		exitCode = 1
	} else {
		if err = utils.WaitForHTTPBalancedServices(context.RestURL+"/balanced", 120); err != nil {
			fmt.Printf("Unable to wait for balanced services: %s\n" + err.Error())
			exitCode = 1
		} else {
			exitCode = m.Run()
		}
	}

	fmt.Printf("Tests completed with return code %d\n", exitCode)
	if exitCode != 0 {
		//collect logs from docker images
		_ = utils.CollectDockerLogs()
	}
	_, _ = utils.DockerComposeDown(fileName)
	os.Exit(exitCode)
}
