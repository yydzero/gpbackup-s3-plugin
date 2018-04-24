all: depend build test

SHELL := /bin/bash
.DEFAULT_GOAL := all
S3_PLUGIN=gp_s3plugin
DIR_PATH=$(shell dirname `pwd`)
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')

DEST = .

GOFLAGS :=

dependencies :
		go get github.com/alecthomas/gometalinter
		gometalinter --install
		go get github.com/golang/dep/cmd/dep
		dep ensure
		@cd vendor/golang.org/x/tools/cmd/goimports; go install .
		@cd vendor/github.com/onsi/ginkgo/ginkgo; go install .

format :
		goimports -w .
		gofmt -w -s .

lint :
		! gofmt -l s3plugin/ | read
		gometalinter --config=gometalinter.config -s vendor ./...

unit :
		ginkgo -r -randomizeSuites -noisySkippings=false -randomizeAllSpecs s3plugin 2>&1

test : lint unit

depend : dependencies

build :
		go build -tags '$(S3_PLUGIN)' $(GOFLAGS) -o $(BIN_DIR)/$(S3_PLUGIN)

build_linux :
		env GOOS=linux GOARCH=amd64 go build -tags '$(S3_PLUGIN)' $(GOFLAGS) -o $(BIN_DIR)/$(S3_PLUGIN)

build_mac :
		env GOOS=darwin GOARCH=amd64 go build -tags '$(S3_PLUGIN)' $(GOFLAGS) -o $(BIN_DIR)/$(S3_PLUGIN)

clean :
		# Build artifacts
		rm -f $(BIN_DIR)/$(S3_PLUGIN)
		# Test artifacts
		rm -rf /tmp/go-build*
		rm -rf /tmp/gexec_artifacts*
		rm -rf /tmp/ginkgo*
