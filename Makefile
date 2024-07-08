FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(CURDIR)/bin:$(CURDIR)/tools/bin:$(path_to_add):$(PATH)

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

CONSUMER_BUILD_FLAG=
ifeq ("${IS_ALPINE}", "1")
	CONSUMER_BUILD_FLAG = -tags musl
endif
GOBUILD  := $(GOEXPERIMENT) CGO_ENABLED=$(CGO) $(GO) build $(BUILD_FLAG) -trimpath $(GOVENDORFLAG)

generate-protobuf: 
	@echo "generate-protobuf"
	./scripts/generate-protobuf.sh

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd