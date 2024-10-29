FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(CURDIR)/bin:$(CURDIR)/tools/bin:$(path_to_add):$(PATH)
TIFLOW_CDC_PKG := github.com/pingcap/tiflow
CDC_PKG := github.com/flowbehappy/tigate
# DBUS_SESSION_BUS_ADDRESS pulsar client use dbus to detect the connection status,
# but it will not exit when the connection is closed.
# I try to use leak_helper to detect goroutine leak,but it does not work.
# https://github.com/benthosdev/benthos/issues/1184 suggest to use environment variable to disable dbus.
export DBUS_SESSION_BUS_ADDRESS := /dev/null

SHELL := /usr/bin/env bash

TEST_DIR := /tmp/tidb_cdc_test
DM_TEST_DIR := /tmp/dm_test
ENGINE_TEST_DIR := /tmp/engine_test

GO       := GO111MODULE=on go
ifeq (${CDC_ENABLE_VENDOR}, 1)
GOVENDORFLAG := -mod=vendor
endif

BUILDTS := $(shell date -u '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITBRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GOVERSION := $(shell go version)

# Since TiDB add a new dependency on github.com/cloudfoundry/gosigar,
# We need to add CGO_ENABLED=1 to make it work when build TiCDC in Darwin OS.
# These logic is to check if the OS is Darwin, if so, add CGO_ENABLED=1.
# ref: https://github.com/cloudfoundry/gosigar/issues/58#issuecomment-1150925711
# ref: https://github.com/pingcap/tidb/pull/39526#issuecomment-1407952955
OS := "$(shell go env GOOS)"
SED_IN_PLACE ?= $(shell which sed)
IS_ALPINE := $(shell grep -qi Alpine /etc/os-release && echo 1)
ifeq (${OS}, "linux")
	CGO := 0
	SED_IN_PLACE += -i
else ifeq (${OS}, "darwin")
	CGO := 1
	SED_IN_PLACE += -i ''
endif

BUILD_FLAG =
GOEXPERIMENT=
ifeq ("${ENABLE_FIPS}", "1")
	BUILD_FLAG = -tags boringcrypto
	GOEXPERIMENT = GOEXPERIMENT=boringcrypto
	CGO = 1
endif

RELEASE_VERSION =
ifeq ($(RELEASE_VERSION),)
	RELEASE_VERSION := v8.2.0-master
	release_version_regex := ^v[0-9]\..*$$
	release_branch_regex := "^release-[0-9]\.[0-9].*$$|^HEAD$$|^.*/*tags/v[0-9]\.[0-9]\..*$$"
	ifneq ($(shell git rev-parse --abbrev-ref HEAD | grep -E $(release_branch_regex)),)
		# If we are in release branch, try to use tag version.
		ifneq ($(shell git describe --tags --dirty | grep -E $(release_version_regex)),)
			RELEASE_VERSION := $(shell git describe --tags --dirty)
		endif
	else ifneq ($(shell git status --porcelain),)
		# Add -dirty if the working tree is dirty for non release branch.
		RELEASE_VERSION := $(RELEASE_VERSION)-dirty
	endif
endif

# Version LDFLAGS.
LDFLAGS += -X "$(CDC_PKG)/version.ReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(CDC_PKG)/version.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(CDC_PKG)/version.GitHash=$(GITHASH)"
LDFLAGS += -X "$(CDC_PKG)/version.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(CDC_PKG)/version.GoVersion=$(GOVERSION)"
LDFLAGS += -X "github.com/pingcap/tidb/pkg/parser/mysql.TiDBReleaseVersion=$(RELEASE_VERSION)"

# For Tiflow CDC
LDFLAGS += -X "$(TIFLOW_CDC_PKG)/pkg/version.ReleaseVersion=v8.4.0-alpha-44-gdd2d54ad4"
LDFLAGS += -X "$(TIFLOW_CDC_PKG)/pkg/version.GitHash=dd2d54ad4c196606d038da6686462cbfe1109894"
LDFLAGS += -X "$(TIFLOW_CDC_PKG)/pkg/version.GitBranch=master"
LDFLAGS += -X "$(TIFLOW_CDC_PKG)/pkg/version.BuildTS=$(BUILDTS)"

CONSUMER_BUILD_FLAG=
ifeq ("${IS_ALPINE}", "1")
	CONSUMER_BUILD_FLAG = -tags musl
endif
GOBUILD  := $(GOEXPERIMENT) CGO_ENABLED=$(CGO) $(GO) build $(BUILD_FLAG) -trimpath $(GOVENDORFLAG)

tools/bin/protoc:
	@echo "download protoc"
	./scripts/download-protoc.sh

tools/bin/protoc-gen-go:
	@echo "download protoc-gen-go"
	./scripts/download-protoc-gen-go.sh

generate-protobuf: 
	@echo "generate-protobuf"
	./scripts/generate-protobuf.sh

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd