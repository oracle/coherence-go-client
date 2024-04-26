/*
 * Copyright (c) 2022, 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package utils

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/onsi/gomega"
	coherence "github.com/oracle/coherence-go-client/coherence"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// TestContext is a context to pass to tests
type TestContext struct {
	ClusterName     string
	HostName        string
	GrpcPort        int
	HTTPPort        int
	URL             string
	RestURL         string
	ExpectedServers int
	Username        string
	Password        string
	SecureMode      string // value of "env" means read from environment and "options" for options to NewSession()
	ClientCertPath  string
	ClientKeyPath   string
	CaCertPath      string
}

var (
	currentTestContext *TestContext
	emptyByte          = make([]byte, 0)
	localCtx           = context.Background()
)

const (
	// the following env options are used to set the SSL mode via coherence.With* options or tlsCOnfig rather
	// than environment variables
	envTLSCertPath        = "COHERENCE_TLS_CERTS_PATH_OPTION"
	envTLSClientCert      = "COHERENCE_TLS_CLIENT_CERT_OPTION"
	envTLSClientKey       = "COHERENCE_TLS_CLIENT_KEY_OPTION"
	envIgnoreInvalidCerts = "COHERENCE_IGNORE_INVALID_CERTS_OPTION"
)

// SetTestContext sets the current context
func SetTestContext(context *TestContext) {
	currentTestContext = context
}

// GetTestContext gets the current context
func GetTestContext() *TestContext {
	return currentTestContext
}

// CreateTempDirectory creates a temporary directory
func CreateTempDirectory(pattern string) string {
	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		fmt.Println("Unable to create temporary directory " + err.Error())
	}
	defer os.RemoveAll(dir)

	return dir
}

// FileExistsInDirectory returns true if a file exists in a directory
func FileExistsInDirectory(dir string, file string) bool {
	files, err := os.ReadDir(dir)

	if err != nil {
		return false
	}

	for _, f := range files {
		if f.Name() == file {
			return true
		}
	}
	return false
}

// GetFilePath returns the file path of a file
func GetFilePath(fileName string) string {
	_, c, _, _ := runtime.Caller(0)
	dir := filepath.Dir(c)
	return dir + string(os.PathSeparator) + fileName
}

// StartCoherenceCluster starts a Coherence cluster
func StartCoherenceCluster(fileName, url string) error {
	output, err := DockerComposeUp(fileName)
	if err != nil {
		return errors.New(output + ": " + err.Error())
	}
	// wait for ready
	err = WaitForHTTPReady(url, 120)
	if err != nil {
		fmt.Println("Collecting logs from docker...")
		_ = CollectDockerLogs()
		return errors.New("Unable to start cluster: " + err.Error())
	}

	// sleep extra time for gRPC to be ready
	Sleep(10)
	return nil
}

// DockerComposeUp runs docker compose up on a given file
func DockerComposeUp(composeFile string) (string, error) {
	command, args := getDockerComposeCommand([]string{"-f", composeFile, "--env-file", "../../../test/utils/.env", "up", "-d"}...)
	fmt.Printf("Issuing %s up with file %v\n", command, composeFile)

	output, err := ExecuteHostCommand(command, args...)

	if err != nil {
		fmt.Println(output)
		return "", err
	}
	fmt.Println(output)

	return output, err
}

// CollectDockerLogs collects docker logs
func CollectDockerLogs() error {
	var (
		output    string
		err       error
		logs      string
		file      *os.File
		directory = GetFilePath("../../build/_output/test-logs/")
	)
	output, err = ExecuteHostCommand("docker", "ps", "-q")
	if err != nil {
		return err
	}

	for _, container := range strings.Split(output, "\n") {
		if container == "" {
			continue
		}

		logs, err = ExecuteHostCommand("docker", "logs", container)
		if err != nil {
			return err
		}

		//write to build output directory
		fileName := filepath.Join(directory, container+".logs")

		fmt.Println("Dumping logs for " + container + " to " + fileName)

		file, err = os.Create(fileName)
		if err != nil {
			return err
		}
		_, err = file.WriteString(logs)
		if err != nil {
			return err
		}

		_ = file.Close()
	}

	return nil
}

// DockerComposeDown runs docker compose down on a given file
func DockerComposeDown(composeFile string) (string, error) {
	fmt.Println("Issuing docker compose down with file " + composeFile)
	// sleep as sometimes docker compose networks are not completely stopped
	Sleep(5)

	command, args := getDockerComposeCommand([]string{"-f", composeFile, "down"}...)

	output, err := ExecuteHostCommand(command, args...)

	if err != nil {
		fmt.Println(output)
		return "", err
	}
	return output, err
}

// GetManagementURL returns the management URL given a management port
func GetManagementURL(httpPort int) string {
	return fmt.Sprintf("http://localhost:%d/management/coherence/cluster", httpPort)
}

// GetGrpcURL returns the gRPC URL given a host and port
func GetGrpcURL(hostname string, port int) string {
	return fmt.Sprintf("%s:%d", hostname, port)
}

// GetRestURL returns the REST URL
func GetRestURL(restPort int) string {
	return fmt.Sprintf("http://localhost:%d", restPort)
}

// IssueGetRequest issues a HTTP GET request using the URL
func IssueGetRequest(url string) ([]byte, error) {
	resp, err := http.Get(url) //nolint
	if err != nil {
		return emptyByte, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return emptyByte, err
	}

	if resp.StatusCode != 200 {
		return emptyByte, errors.New("Did not receive a 200 response code: " + resp.Status + ", error=" + string(body))
	}

	return body, nil
}

// IssuePostRequest issues a HTTP POST request using the URL
func IssuePostRequest(url string) ([]byte, error) {
	resp, err := issueRequest("POST", url, emptyByte)

	if err != nil {
		return emptyByte, err
	}

	if resp.StatusCode != 200 {
		return emptyByte, errors.New("Did not receive a 200 response code: " + resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return emptyByte, err
	}

	return body, nil
}

func issueRequest(requestType, url string, data []byte) (*http.Response, error) {
	var (
		err error
		req *http.Request
	)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: false, MinVersion: tls.VersionTLS12},
	}
	cookies, _ := cookiejar.New(nil)
	client := &http.Client{Transport: tr,
		Timeout: time.Duration(120) * time.Second,
		Jar:     cookies,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}}
	req, err = http.NewRequest(requestType, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

// WaitForHTTPReady waits for the HTTP endpoint to be ready
func WaitForHTTPReady(url string, timeout int) error {
	var duration = 0
	for duration < timeout {
		_, err := IssueGetRequest(url)
		if err != nil {
			// unable to connect, so wait 5 seconds
			fmt.Println("Waiting to connect to " + url + ", sleeping 5")
			Sleep(5)
			duration += 5
		} else {
			fmt.Println("HTTP endpoint ready")
			return nil
		}
	}

	return fmt.Errorf("unable to connect to url %s after %d seconds", url, timeout)
}

// WaitForHTTPBalancedServices waits for all services to be balanced
func WaitForHTTPBalancedServices(url string, timeout int) error {
	var duration = 0
	fmt.Println("Waiting for services to be balanced...")
	for duration < timeout {
		content, err := IssueGetRequest(url)
		if err != nil {
			// unable to connect, so wait 5 seconds
			fmt.Println("Waiting for services" + url + ", sleeping 5")
			Sleep(5)
			duration += 5
		} else {
			var contentString = string(content)
			if contentString == "OK" {
				fmt.Println("All services balanced")
				return nil
			}
			fmt.Println("\n", contentString)
			Sleep(5)
			duration += 5
		}
	}

	return fmt.Errorf("unable to connect to url %s after %d seconds", url, timeout)
}

// Sleep will sleep for a duration of seconds
func Sleep(seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
}

// ExecuteHostCommand executes a host command
func ExecuteHostCommand(name string, arg ...string) (string, error) {
	cmd := exec.Command(name, arg...)
	stdout, err := cmd.CombinedOutput()

	var stringStdOut = string(stdout)

	if err != nil {
		return stringStdOut, err
	}

	return stringStdOut, nil
}

// GetSession returns a coherence session for testing
func GetSession(options ...func(session *coherence.SessionOptions)) (*coherence.Session, error) {
	var (
		testContext = GetTestContext()
		grpcURL     = GetGrpcURL(testContext.HostName, testContext.GrpcPort)
	)

	sessionOptions := make([]func(session *coherence.SessionOptions), 0)
	sessionOptions = append(sessionOptions, coherence.WithAddress(grpcURL))
	if len(options) > 0 {
		sessionOptions = append(sessionOptions, options...)
	}

	// If SecureMode is empty then it's plain text
	if testContext.SecureMode == "" {
		sessionOptions = append(sessionOptions, coherence.WithPlainText())
	} else {
		log.Println("Secure mode is", testContext.SecureMode)
		// must be TLS so check if we need to read from "env" or "options"
		if testContext.SecureMode == "options" {
			// if we read from options we need to store the values from the env
			sessionOptions = append(sessionOptions, coherence.WithTLSClientCert(os.Getenv(envTLSClientCert)),
				coherence.WithTLSCertsPath(os.Getenv(envTLSCertPath)),
				coherence.WithTLSClientKey(os.Getenv(envTLSClientKey)))
			if os.Getenv(envIgnoreInvalidCerts) == "true" {
				sessionOptions = append(sessionOptions, coherence.WithIgnoreInvalidCerts())
			}
			log.Println(sessionOptions)
		} else if testContext.SecureMode == "tlsConfig" {
			// read the environment variables as above and create the TLS options
			config, err := createTLSOptions(os.Getenv(envTLSClientCert), os.Getenv(envTLSClientKey), os.Getenv(envTLSCertPath),
				os.Getenv(envIgnoreInvalidCerts) == "true")
			if err != nil {
				return nil, err
			}
			sessionOptions = append(sessionOptions, coherence.WithTLSConfig(config))
		}
	}

	return coherence.NewSession(localCtx, sessionOptions...)
}

// createTLSOptions creates tls.Config for testing.
func createTLSOptions(clientCertPath, clientKeyPath, certsPath string, ignoreInvalidCerts bool) (*tls.Config, error) {
	var (
		cp           *x509.CertPool
		certData     []byte
		certificates = make([]tls.Certificate, 0)
		err          error
	)
	log.Println("creating tls.Config")
	if certsPath != "" {
		cp = x509.NewCertPool()

		if err = validateFilePath(certsPath); err != nil {
			return nil, err
		}

		certData, err = os.ReadFile(certsPath)
		if err != nil {
			return nil, err
		}

		if !cp.AppendCertsFromPEM(certData) {
			return nil, errors.New("credentials: failed to append certificates")
		}
	}

	if clientCertPath != "" && clientKeyPath != "" {
		log.Println("loading client certificate and key, cert=", clientCertPath, "key=", clientKeyPath)
		if err = validateFilePath(clientCertPath); err != nil {
			return nil, err
		}
		if err = validateFilePath(clientKeyPath); err != nil {
			return nil, err
		}
		var clientCert tls.Certificate
		clientCert, err = tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			return nil, err
		}
		certificates = []tls.Certificate{clientCert}
	}

	config := &tls.Config{
		InsecureSkipVerify: ignoreInvalidCerts, //nolint
		RootCAs:            cp,
		Certificates:       certificates,
	}

	return config, nil
}

// validateFilePath checks to see if a file path is valid.
func validateFilePath(file string) error {
	if _, err := os.Stat(file); err == nil {
		return nil
	}

	return fmt.Errorf("%s is not a valid file", file)
}

func GetNamedMapWithScope[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName, _ string) coherence.NamedMap[K, V] {
	namedCache, err := coherence.GetNamedMap[K, V](session, cacheName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedCache.Clear(localCtx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	return namedCache
}

func GetNamedCacheWithScope[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName, _ string) coherence.NamedCache[K, V] {
	namedCache, err := coherence.GetNamedCache[K, V](session, cacheName)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = namedCache.Clear(localCtx)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	return namedCache
}

func GetNamedMap[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName string) coherence.NamedMap[K, V] {
	return GetNamedMapWithScope[K, V](g, session, cacheName, "")
}

func GetNamedCache[K comparable, V any](g *gomega.WithT, session *coherence.Session, cacheName string) coherence.NamedCache[K, V] {
	return GetNamedCacheWithScope[K, V](g, session, cacheName, "")
}

func AssertSize[K comparable, V any](g *gomega.WithT, namedMap coherence.NamedMap[K, V], expectedSize int) {
	size, err := namedMap.Size(localCtx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(expectedSize))
}

func ClearNamedMap[K comparable, V any](g *gomega.WithT, namedCache coherence.NamedMap[K, V]) {
	err := namedCache.Clear(localCtx)
	g.Expect(err).NotTo(gomega.HaveOccurred())
}

func AssertPersonResult(g *gomega.WithT, result Person, expectedValue Person) {
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.Name).To(gomega.Equal(expectedValue.Name))
	g.Expect(result.ID).To(gomega.Equal(expectedValue.ID))
}

type Person struct {
	ID          int      `json:"id"`
	Name        string   `json:"name"`
	HomeAddress Address  `json:"homeAddress"`
	Age         int      `json:"age"`
	Salary      float32  `json:"salary"`
	Languages   []string `json:"languages"`
	Phone       string   `json:"phone"`
	Department  string   `json:"department"`
}

type BooleanTest struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

type VersionedPerson struct {
	Version int     `json:"@version"`
	ID      int     `json:"id"`
	Name    string  `json:"name"`
	Age     int     `json:"age"`
	Salary  float32 `json:"salary"`
}

type Address struct {
	Address1 string `json:"address1"`
	Address2 string `json:"address2"`
	City     string `json:"city"`
	State    string `json:"state"`
	PostCode int    `json:"postCode"`
}

// RunKeyValueTest runs a basic Put/Get test against various key/ values
func RunKeyValueTest[K comparable, V any](g *gomega.WithT, cache coherence.NamedMap[K, V], key K, value V) {
	var (
		result   *V
		err      = cache.Clear(localCtx)
		oldValue *V
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = cache.Put(localCtx, key, value)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	result, err = cache.Get(localCtx, key)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(*result).To(gomega.Equal(value))

	oldValue, err = cache.Remove(localCtx, key)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.Equal(result))
}

// RunKeyValueTestNamedCache runs a basic Put/Get test against various key/ values
func RunKeyValueTestNamedCache[K comparable, V any](g *gomega.WithT, cache coherence.NamedCache[K, V], key K, value V) {
	var (
		result interface{}
		err    = cache.Clear(localCtx)
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = cache.Put(localCtx, key, value)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	result, err = cache.Get(localCtx, key)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(result).To(gomega.Equal(value))
}

// getDockerComposeCommand returns true if we should use "docker-compose" (v1).
func useDockerComposeV1() bool {
	return os.Getenv("DOCKER_COMPOSE_V1") != ""
}

func getDockerComposeCommand(arguments ...string) (string, []string) {
	command := "docker"
	args := arguments
	if useDockerComposeV1() {
		command = "docker-compose"
	} else {
		finalArgs := []string{"compose"}
		args = append(finalArgs, args...)
	}

	return command, args
}
