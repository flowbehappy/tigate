// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package replica

import "github.com/pingcap/ticdc/heartbeatpb"

type Checker interface {
	UpdateStatus(span *SpanReplication, status *heartbeatpb.TableSpanStatus)
	Check() []*SpanReplication
}

type OpType int

const (
	OpSplit         OpType = iota
	OpMerge                // merge to one span
	OpMergeAndSplit        // remove old spans and split to other spans
)

type CheckResult struct {
	OpType OpType
	Spans  []*SpanReplication
}
