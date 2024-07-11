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

package sink

import "github.com/flowbehappy/tigate/pkg/common"

type Sink interface {
	AddDMLEvent(tableSpan *common.TableSpan, event *common.TxnEvent)
	AddDDLAndSyncPointEvent(tableSpan *common.TableSpan, event *common.TxnEvent)
	IsEmpty(tableSpan *common.TableSpan) bool
	AddTableSpan(tableSpan *common.TableSpan)
	RemoveTableSpan(tableSpan *common.TableSpan)
	GetSmallestCommitTs(tableSpan *common.TableSpan) uint64
}
