#!/usr/bin/env bash
# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

TOOLS_BIN_DIR="tools/bin"
TOOLS_INCLUDE_DIR="tools/include"
PROTO_DIR="proto"
TiCDC_SOURCE_DIR="./"
INCLUDE="-I $PROTO_DIR \
	-I $TOOLS_INCLUDE_DIR \
	-I $TiCDC_SOURCE_DIR"

PROTOC="$TOOLS_BIN_DIR/protoc"
GO="$TOOLS_BIN_DIR/protoc-gen-go"
GO_GRPC="$TOOLS_BIN_DIR/protoc-gen-go-grpc"
GOGO_FASTER=$TOOLS_BIN_DIR/protoc-gen-gogofaster

function generate() {
	local out_dir=$1
	local proto_file=$2
	local gogo_option=
	if [ $# -eq 3 ]; then
		gogo_option=$3
	fi

	echo "generate $proto_file..."
	$PROTOC $INCLUDE \
		--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
		--plugin=protoc-gen-go-grpc="$GO_GRPC" \
		--go-grpc_out=$out_dir \
		--gogofaster_out=$gogo_option:$out_dir $proto_file
}

for tool in $PROTOC $GO $GO_GRPC $GOGO_FASTER; do
	if [ ! -x $tool ]; then
		echo "$tool does not exist, please run 'make $tool' first."
		exit 1
	fi
done

for pb in $(find eventpb -name '*.proto'); do
	# Output generated go files next to protobuf files.
	generate ./ $pb paths="source_relative"
done


for pb in $(find heartbeatpb -name '*.proto'); do
	# Output generated go files next to protobuf files.
	generate ./ $pb paths="source_relative"
done

for pb in $(find logservice/logservicepb -name '*.proto'); do
	# Output generated go files next to protobuf files.
	generate ./ $pb paths="source_relative"
done

# for pb in $(find pkg/messaging/proto -name '*.proto'); do
# 	# Output generated go files next to protobuf files.
# 	generate ./pkg/messaging/proto $pb paths="source_relative"
# done

