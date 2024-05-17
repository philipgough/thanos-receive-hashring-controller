PROJECT_PATH := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
BIN_FOLDER=$(PROJECT_PATH)/bin

REGISTRY ?= quay.io/philipgough
TAG ?= latest
IMAGE_NAME = hashring-controller:$(TAG)

.PHONY: test
test: unit-test integration-test ## Run all tests

.PHONY: unit-test
unit-test: ## Run unit tests
	go test ./...

.PHONY: integration-test
integration-test: tools ## Run integration tests
	go test -tags integration ./...

.PHONY: build-image
build-image: ## Build docker image
	docker build --tag $(REGISTRY)/$(IMAGE_NAME) .

.PHONY: tools
tools:
	GOBIN=$(BIN_FOLDER) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
	$(BIN_FOLDER)/setup-envtest use --print env

