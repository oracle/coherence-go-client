# ----------------------------------------------------------------------------------------------------------------------
# Copyright (c) 2021, 2023 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl.
#
# ----------------------------------------------------------------------------------------------------------------------
# This is the Makefile to build the Coherence Go Client
# ----------------------------------------------------------------------------------------------------------------------

# This is the version of the coherence-go-client
VERSION ?=1.0.1
CURRDIR := $(shell pwd)
USER_ID := $(shell echo "`id -u`:`id -g`")

override BUILD_BIN           := $(CURRDIR)/bin
override PROTO_DIR			 := $(CURRDIR)/etc/proto

# ----------------------------------------------------------------------------------------------------------------------
# Set the location of various build tools
# ----------------------------------------------------------------------------------------------------------------------
override BUILD_OUTPUT        := $(CURRDIR)/build/_output
override BUILD_BIN           := $(CURRDIR)/bin
override PROTO_OUT           := $(CURRDIR)/proto
override BUILD_TARGETS       := $(BUILD_OUTPUT)/targets
override TEST_LOGS_DIR       := $(BUILD_OUTPUT)/test-logs
override COVERAGE_DIR        := $(BUILD_OUTPUT)/coverage
override COPYRIGHT_JAR       := glassfish-copyright-maven-plugin-2.4.jar
override BUILD_CERTS         := $(CURRDIR)/test/utils/certs
override ENV_FILE            := test/utils/.env

# Maven version is always 1.0.0 as it is only for testing
MVN_VERSION ?= 1.0.0

# Coherence CE version to run base tests against
COHERENCE_VERSION ?= 22.06.4
COHERENCE_GROUP_ID ?= com.oracle.coherence.ce
COHERENCE_WKA1 ?= server1
COHERENCE_WKA2 ?= server1
CLUSTER_PORT ?= 7574
# Profiles to include for building
PROFILES ?=
COHERENCE_BASE_IMAGE ?= gcr.io/distroless/java:11

# ----------------------------------------------------------------------------------------------------------------------
# Set the location of various build tools
# ----------------------------------------------------------------------------------------------------------------------
TOOLS_DIRECTORY   = $(CURRDIR)/build/tools
TOOLS_BIN         = $(TOOLS_DIRECTORY)/bin

# ----------------------------------------------------------------------------------------------------------------------
# The test application images used in integration tests
# ----------------------------------------------------------------------------------------------------------------------
RELEASE_IMAGE_PREFIX     ?= ghcr.io/oracle/
TEST_APPLICATION_IMAGE_1 := $(RELEASE_IMAGE_PREFIX)coherence-go-test-1:1.0.0
TEST_APPLICATION_IMAGE_2 := $(RELEASE_IMAGE_PREFIX)coherence-go-test-2:1.0.0
GO_TEST_FLAGS ?= -timeout 50m

# ----------------------------------------------------------------------------------------------------------------------
# Options to append to the Maven command
# ----------------------------------------------------------------------------------------------------------------------
MAVEN_OPTIONS ?= -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=3
MAVEN_BUILD_OPTS :=$(USE_MAVEN_SETTINGS) -Drevision=$(MVN_VERSION) -Dcoherence.version=$(COHERENCE_VERSION) -Dcoherence.group.id=$(COHERENCE_GROUP_ID) $(MAVEN_OPTIONS)

CURRDIR := $(shell pwd)
GOROOT=$(shell go env GOROOT)

# ----------------------------------------------------------------------------------------------------------------------
# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
# ----------------------------------------------------------------------------------------------------------------------
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# ----------------------------------------------------------------------------------------------------------------------
# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
# ----------------------------------------------------------------------------------------------------------------------
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

GITCOMMIT              ?= $(shell git rev-list -1 HEAD)
GITREPO                := https://github.com/oracle/coherence-go-client.git
SOURCE_DATE_EPOCH      := $(shell git show -s --format=format:%ct HEAD)
DATE_FMT               := "%Y-%m-%dT%H:%M:%SZ"
BUILD_DATE             := $(shell date -u -d "@$SOURCE_DATE_EPOCH" "+${DATE_FMT}" 2>/dev/null || date -u -r "${SOURCE_DATE_EPOCH}" "+${DATE_FMT}" 2>/dev/null || date -u "+${DATE_FMT}")
BUILD_USER             := $(shell whoami)
GOS              = $(shell find . -type f -name "*.go" ! -name "*_test.go")

# ======================================================================================================================
# Makefile targets start here
# ======================================================================================================================

# ----------------------------------------------------------------------------------------------------------------------
# Display the Makefile help - this is a list of the targets with a description.
# This target MUST be the first target in the Makefile so that it is run when running make with no arguments
# ----------------------------------------------------------------------------------------------------------------------
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)


# ======================================================================================================================
# Build targets
# ======================================================================================================================
##@ Build

# ----------------------------------------------------------------------------------------------------------------------
# Clean-up all of the build artifacts
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: clean
clean: ## Cleans the build
	@echo "Cleaning Project"
	-rm -rf $(BUILD_OUTPUT)
	-rm -rf bin
	-rm -rf $(BUILD_CERTS)
	@mkdir -p $(TEST_LOGS_DIR)
	@mkdir -p $(COVERAGE_DIR)
	@mkdir -p $(BUILD_OUTPUT)
	@mkdir -p $(PROTO_OUT)
	@mkdir -p $(BUILD_CERTS)
	mvn -B -f java/pom.xml $(MAVEN_BUILD_OPTS) clean

.PHONY: certs
certs: ## Generates certificates for TLS tests
	@echo "Generating certs"
	./scripts/keys.sh $(BUILD_CERTS)

# ----------------------------------------------------------------------------------------------------------------------
# Configure the build properties
# ----------------------------------------------------------------------------------------------------------------------
$(BUILD_PROPS):
	@echo "Creating build directories"
	@mkdir -p $(BUILD_OUTPUT)
	@mkdir -p $(BUILD_BIN)

# ----------------------------------------------------------------------------------------------------------------------
# Build the Coherence Go Client Test Image
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: build-test-images
build-test-images: ## Build the Test images
	@echo "${MAVEN_BUILD_OPTS}"
	mvn -B -f java/pom.xml clean package jib:dockerBuild -DskipTests -P member1$(PROFILES) -Djib.to.image=$(TEST_APPLICATION_IMAGE_1) -Dcoherence.test.base.image=$(COHERENCE_BASE_IMAGE) $(MAVEN_BUILD_OPTS)
	mvn -B -f java/pom.xml clean package jib:dockerBuild -DskipTests -P member2$(PROFILES) -Djib.to.image=$(TEST_APPLICATION_IMAGE_2) -Dcoherence.test.base.image=$(COHERENCE_BASE_IMAGE) $(MAVEN_BUILD_OPTS)
	echo "CURRENT_UID=$(USER_ID)" >> $(ENV_FILE)


# ----------------------------------------------------------------------------------------------------------------------
# Performs a copyright check.
# To add exclusions add the file or folder pattern using the -X parameter.
# Add directories to be scanned at the end of the parameter list.
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: copyright
copyright: getcopyright ## Check copyright headers
	@java -cp scripts/$(COPYRIGHT_JAR) \
	  org.glassfish.copyright.Copyright -C scripts/copyright.txt \
	  -X bin/ \
	  -X ./test/test/utils.go \
	  -X dependency-reduced-pom.xml \
	  -X binaries/ \
	  -X build/ \
	  -X proto/ \
	  -X /Dockerfile \
	  -X .Dockerfile \
	  -X go.sum \
	  -X HEADER.txt \
	  -X .iml \
	  -X .jar \
	  -X jib-cache/ \
	  -X .jks \
	  -X .json \
	  -X LICENSE.txt \
	  -X Makefile \
	  -X cohctl-terminal.gif \
	  -X .md \
	  -X .mvn/ \
	  -X mvnw \
	  -X mvnw.cmd \
	  -X .png \
	  -X .sh \
	  -X temp/ \
	  -X .proto \
	  -X /test-report.xml \
	  -X THIRD_PARTY_LICENSES.txt \
	  -X .tpl \
	  -X .txt \
	  -X test_utils/certs \
	  -X pkg/data/assets/

# ----------------------------------------------------------------------------------------------------------------------
# Executes golangci-lint to perform various code review checks on the source.
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: golangci
golangci: $(TOOLS_BIN)/golangci-lint ## Go code review
	$(TOOLS_BIN)/golangci-lint run -v --timeout=5m  ./...
	cd examples && $(TOOLS_BIN)/golangci-lint run -v --timeout=5m  ./...

# ----------------------------------------------------------------------------------------------------------------------
# Download and build proto files
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: generate-proto
generate-proto: $(TOOLS_BIN)/protoc ## Generate Proto Files
	mkdir -p $(PROTO_DIR) || true
	curl -o $(PROTO_DIR)/services.proto https://raw.githubusercontent.com/oracle/coherence/22.06.4/prj/coherence-grpc/src/main/proto/services.proto
	curl -o $(PROTO_DIR)/messages.proto https://raw.githubusercontent.com/oracle/coherence/22.06.4/prj/coherence-grpc/src/main/proto/messages.proto
	echo "" >> $(PROTO_DIR)/services.proto
	echo "" >> $(PROTO_DIR)/messages.proto
	echo 'option go_package = "github.com/oracle/coherence-go-client/proto";' >> $(PROTO_DIR)/services.proto
	echo 'option go_package = "github.com/oracle/coherence-go-client/proto";' >> $(PROTO_DIR)/messages.proto
	$(TOOLS_BIN)/protoc --proto_path=./etc/proto --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative etc/proto/messages.proto etc/proto/services.proto


# ----------------------------------------------------------------------------------------------------------------------
# Show the local documentation
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: show-docs
show-docs:   ## Show the Documentation
	@echo "Serving documentation on http://localhost:6060/pkg/github.com/oracle/coherence-go-client/"
	go install golang.org/x/tools/cmd/godoc@latest
	godoc -goroot $(GOROOT)	-http=:6060


# ======================================================================================================================
# Miscellaneous targets
# ======================================================================================================================
##@ Miscellaneous

.PHONY: trivy-scan
trivy-scan: gettrivy ## Scan the CLI using trivy
	$(TOOLS_BIN)/trivy fs --exit-code 1 --skip-dirs "./java" .


# ======================================================================================================================
# Test targets
# ======================================================================================================================
##@ Test

# ----------------------------------------------------------------------------------------------------------------------
# Executes the Go unit tests
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test
test: test-clean gotestsum $(BUILD_PROPS) ## Run the unit tests
	CGO_ENABLED=0 $(GOTESTSUM) --format testname --junitfile $(TEST_LOGS_DIR)/coherence-test.xml \
	  -- $(GO_TEST_FLAGS) -v -coverprofile=$(COVERAGE_DIR)/cover-unit.out ./coherence/...
	go tool cover -html=$(COVERAGE_DIR)/cover-unit.out -o $(COVERAGE_DIR)/cover-unit.html


# ----------------------------------------------------------------------------------------------------------------------
# Executes the Go end to end tests for standalone Coherence
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-e2e-standalone
test-e2e-standalone: test-clean test gotestsum $(BUILD_PROPS) ## Run e2e tests with Coherence
	CGO_ENABLED=0 $(GOTESTSUM) --format testname --junitfile $(TEST_LOGS_DIR)/go-client-test.xml \
	  -- $(GO_TEST_FLAGS) -v -coverprofile=$(COVERAGE_DIR)/cover-functional.out -v ./test/e2e/standalone/... -coverpkg=./coherence/...
	go tool cover -html=$(COVERAGE_DIR)/cover-functional.out -o $(COVERAGE_DIR)/cover-functional.html
	@echo
	@echo "**** CODE COVERAGE ****"
	@cat $(COVERAGE_DIR)/cover-functional.html | grep 'github.com/oracle/coherence-go-client/coherence' | grep option | sed 's/^.*github/github/' | sed 's,</option.*,,'

# ----------------------------------------------------------------------------------------------------------------------
# Executes the Go end to end tests for standalone Coherence with Scope set
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-e2e-standalone-scope
test-e2e-standalone-scope: test-clean test gotestsum $(BUILD_PROPS) ## Run e2e tests with Coherence with Scope set
	CGO_ENABLED=0 $(GOTESTSUM) --format testname --junitfile $(TEST_LOGS_DIR)/go-client-test-scope.xml \
	  -- $(GO_TEST_FLAGS) -v -coverprofile=$(COVERAGE_DIR)/cover-functional-scope.out -v ./test/e2e/scope/... -coverpkg=./coherence/...
	go tool cover -html=$(COVERAGE_DIR)/cover-functional-scope.out -o $(COVERAGE_DIR)/cover-functional-scope.html
	@echo
	@echo "**** CODE COVERAGE ****"
	@cat $(COVERAGE_DIR)/cover-functional-scope.html | grep 'github.com/oracle/coherence-go-client/coherence' | grep option | sed 's/^.*github/github/' | sed 's,</option.*,,'

# ----------------------------------------------------------------------------------------------------------------------
# Executes the test of the examples
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-examples
test-examples: test-clean gotestsum $(BUILD_PROPS) ## Run examples tests with Coherence
	./scripts/run-test-examples.sh


# ----------------------------------------------------------------------------------------------------------------------
# Startup cluster members via docker compose
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-cluster-startup
test-cluster-startup: $(BUILD_PROPS) ## Startup any test cluster members using docker-compose
	cd test/utils && docker-compose -f docker-compose-2-members.yaml up -d

# ----------------------------------------------------------------------------------------------------------------------
# Shutdown any cluster members via docker compose
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-cluster-shutdown
test-cluster-shutdown: ## Shutdown any test cluster members using docker-compose
	cd test/utils && docker-compose -f docker-compose-2-members.yaml down || true


# ----------------------------------------------------------------------------------------------------------------------
# Startup standalone coherence via java -jar
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-coherence-startup
test-coherence-startup: ## Startup standalone cluster
	scripts/startup-clusters.sh $(TEST_LOGS_DIR) $(CLUSTER_PORT) $(COHERENCE_GROUP_ID) ${COHERENCE_VERSION}
	@echo "Clusters started up"

# ----------------------------------------------------------------------------------------------------------------------
# Shutdown coherence via java -jar
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-coherence-shutdown
test-coherence-shutdown: ## shutdown standalone cluster
	@ps -ef | grep shutMeDownPlease | grep -v grep | awk '{print $$2}' | xargs kill -9 || true
	@echo "Clusters shutdown"


# ----------------------------------------------------------------------------------------------------------------------
# Find or download gotestsum
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: gotestsum
GOTESTSUM = $(TOOLS_BIN)/gotestsum
gotestsum: ## Download gotestsum locally if necessary.
	GOBIN=`pwd`/build/tools/bin go install gotest.tools/gotestsum@v1.8.1

# ----------------------------------------------------------------------------------------------------------------------
# Cleans the test cache
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-clean
test-clean: gotestsum ## Clean the go test cache
	@echo "Cleaning test cache"
	@mkdir -p $(TEST_LOGS_DIR)
	go clean -testcache


# ----------------------------------------------------------------------------------------------------------------------
# Executes the Go discovery tests for standalone Coherence
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: test-discovery
test-discovery: test-clean gotestsum $(BUILD_PROPS) ## Run Discovery tests with Coherence
	make test-coherence-shutdown || true
	make test-coherence-startup
	CGO_ENABLED=0 $(GOTESTSUM) --format testname --junitfile $(TEST_LOGS_DIR)/cohctl-test-discovery.xml \
	  -- $(GO_TEST_FLAGS) -v  ./test/e2e/discovery/...
	make test-coherence-shutdown

# ----------------------------------------------------------------------------------------------------------------------
# Obtain the golangci-lint binary
# ----------------------------------------------------------------------------------------------------------------------
$(TOOLS_BIN)/golangci-lint:
	@mkdir -p $(TOOLS_BIN)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(TOOLS_BIN) v1.52.2


# ----------------------------------------------------------------------------------------------------------------------
# Find or download copyright
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: getcopyright
getcopyright: ## Download copyright jar locally if necessary.
	@test -f scripts/$(COPYRIGHT_JAR)  || curl -o scripts/$(COPYRIGHT_JAR) \
		https://repo.maven.apache.org/maven2/org/glassfish/copyright/glassfish-copyright-maven-plugin/2.4/glassfish-copyright-maven-plugin-2.4.jar


# ----------------------------------------------------------------------------------------------------------------------
# Obtain the protoc binary
# ----------------------------------------------------------------------------------------------------------------------
$(TOOLS_BIN)/protoc:
	@mkdir -p $(TOOLS_BIN)
	./scripts/download-protoc.sh $(TOOLS_DIRECTORY)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.30.0
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0


# go-get-tool will 'go get' any package $2 and install it to $1.
PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
define go-get-tool
@[ -f $(1) ] || { \
set -e ;\
TMP_DIR=$$(mktemp -d) ;\
cd $$TMP_DIR ;\
go mod init tmp ;\
echo "Downloading $(2) into $(TOOLS_BIN)" ;\
GOBIN=$(TOOLS_BIN) go get $(2) ;\
rm -rf $$TMP_DIR ;\
}
endef

# ----------------------------------------------------------------------------------------------------------------------
# Find or download trivy
# ----------------------------------------------------------------------------------------------------------------------
.PHONY: gettrivy
gettrivy:
	@mkdir -p $(TOOLS_BIN)
	curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b $(TOOLS_BIN) v0.38.3
