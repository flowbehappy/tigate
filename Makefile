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

generate-protobuf: 
	@echo "generate-protobuf"
	./scripts/generate-protobuf.sh