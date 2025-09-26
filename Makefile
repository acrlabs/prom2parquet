ARTIFACTS ?= prom2parquet
COVERAGE_DIR=$(BUILD_DIR)/coverage
GO_COVER_FILE=$(COVERAGE_DIR)/go-coverage.txt

include build/base.mk
include build/image.mk
include build/k8s.mk

build:
	CGO_ENABLED=0 go build -ldflags "-s -w" -trimpath -o $(BUILD_DIR)/prom2parquet ./cmd/.

lint:
	golangci-lint run

test:
	mkdir -p $(COVERAGE_DIR)
	go test -v -coverprofile=$(GO_COVER_FILE) ./...

cover:
	go tool cover -func=$(GO_COVER_FILE)

publish:
	DOCKER_REGISTRY=quay.io/appliedcomputing make image
