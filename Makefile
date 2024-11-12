FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1  }  }'

PROJECT=ticdc

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(CURDIR)/bin:$(CURDIR)/tools/bin:$(path_to_add):$(PATH)
TIFLOW_CDC_PKG := github.com/pingcap/tiflow
CDC_PKG := github.com/pingcap/ticdc
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

GOTEST := CGO_ENABLED=1 $(GO) test -p 3 --race --tags=intest

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

PACKAGE_LIST := go list ./... | grep -vE 'vendor|proto|ticdc/tests|integration|testing_utils|pb|pbmock|ticdc/bin'
PACKAGES := $$($(PACKAGE_LIST))

FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor|_gen|proto|pb\.go|pb\.gw\.go|_mock.go')
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1  }  }'

FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/$(PROJECT)/"}|grep -v "github.com/pingcap/$(PROJECT)"; done)
FAILPOINT := tools/bin/failpoint-ctl
FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

include tools/Makefile

generate-protobuf: 
	@echo "generate-protobuf"
	./scripts/generate-protobuf.sh

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd

fmt: tools/bin/gofumports tools/bin/shfmt tools/bin/gci
	@echo "run gci (format imports)"
	tools/bin/gci write $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run gofumports"
	tools/bin/gofumports -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run shfmt"
	tools/bin/shfmt -d -w .
	@echo "check log style"
	scripts/check-log-style.sh
	@make check-diff-line-width

integration_test_build: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/ticdc/... \
		-o bin/cdc.test github.com/pingcap/ticdc/cmd \
	|| { $(FAILPOINT_DISABLE); echo "Failed to build cdc.test"; exit 1; }
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/main.go \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

failpoint-enable: check_failpoint_ctl
	$(FAILPOINT_ENABLE)

failpoint-disable: check_failpoint_ctl
	$(FAILPOINT_DISABLE)

check_failpoint_ctl: tools/bin/failpoint-ctl

integration_test: integration_test_mysql

integration_test_mysql:
	tests/integration_tests/run.sh mysql "$(CASE)" "$(NEWARCH)" "$(START_AT)"

unit_test: check_failpoint_ctl generate-protobuf
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

errdoc: tools/bin/errdoc-gen
	@echo "generator errors.toml"
	./tools/check/check-errdoc.sh