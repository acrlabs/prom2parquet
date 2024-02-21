ARTIFACTS ?= prom2parquet
COVERAGE_DIR=$(BUILD_DIR)/coverage
GO_COVER_FILE=$(COVERAGE_DIR)/go-coverage.txt

include build/base.mk
include build/k8s.mk

$(ARTIFACTS)::
	CGO_ENABLED=0 go build -trimpath -o $(BUILD_DIR)/$@ ./cmd/.

lint:
	golangci-lint run

test:
	mkdir -p $(COVERAGE_DIR)
	go test -coverprofile=$(GO_COVER_FILE) ./...

cover:
	go tool cover -func=$(GO_COVER_FILE)
