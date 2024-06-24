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

package dispatchermanager

import (
	"new_arch/downstreamadapter/dispatcher"
	"new_arch/heartbeatpb"
)

type EventDispatcherManager struct {
	dispatcherMap      map[*Span]*dispatcher.TableEventDispatcher
	eventCollect       *EventCollector
	heartbeatCollector *HeartbeatCollector
}

func newEventDispatcherManager() *EventDispatcherManager {
}

func (e *EventDispatcherManager) CollectHeartbeatInfo() *heartbeatpb.HeartBeatRequest {
	/* 这里定义一下 checkpointTs
	   依旧表示的是小于这个 ts 的值已经落盘到 downstream 了。
	   我们计算的方式为：
	   1. 如果 dispatcher 中目前还有 event，则我们就拿要往下写的最大的 commitTs 的那条 event.CommitTs - 1 为 checkpointTs
	   2. dispatcher 在从 logService 拉数据的时候，会标记到 resolvedTs 为多少（这里指的是完整的 resolvedTs），所以 checkpointTs <= resolvedTs

	   另外我们对于 目前处于 blocked 状态的 dispatcher，我们也会记录他 blocked 住的 ts，以及 blocked 住的 tableSpan
	   后面我们要测一下这个 msg 的大小，以及 collect 的耗时
	*/
	var message heartbeatpb.HeartBeatRequest
	for _, dispatcher := range e.dispatcherMap {
		heartbeatInfo := dispatcher.CollectHeartbeatInfo()
		message.Progress = append(message.Progress, &heartbeatpb.TableSpanProgress{
			CheckpointTs:   heartbeatInfo.CheckpointTs,
			BlockTs:        heartbeatInfo.BlockTs,
			BlockTableSpan: heartbeatInfo.BlockTableSpan,
			IsBlocked:      heartbeatInfo.IsBlocked,
		})
	}
	return &message
}
