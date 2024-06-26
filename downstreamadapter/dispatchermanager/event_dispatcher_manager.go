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
	"new_arch/downstreamadapter/sink"
	"new_arch/heartbeatpb"
	"new_arch/utils/threadpool"
	"sync/atomic"
)

var uniqueEventDispatcherID uint64 = 0

const workerCount = 8

func genUniqueEventDispatcherID() uint64 {
	return atomic.AddUint64(&uniqueEventDispatcherID, 1)
}

type EventDispatcherManager struct {
	DispatcherMap       map[*Span]*dispatcher.TableEventDispatcher
	EventCollector      *EventCollector
	HeartbeatCollector  *HeartbeatCollector
	Id                  uint64
	ChangefeedID        uint64
	SinkType            string
	Sink                sink.Sink
	WorkerTaskScheduler *threadpool.TaskScheduler
	SinkConfig          *Config
	LogServiceAddr      string // log service master addr
}

func (e *EventDispatcherManager) Init() {
	// 初始化 sink
	if e.SinkType == "Mysql" {
		e.Sink = sink.NewMysqlSink(workerCount, e.SinkConfig)
	}

	e.EventCollector = newEventCollector(e.LogServiceAddr)
	// 初始化 table trigger event dispatcher， 注册自己
}

// 收到 rpc 请求创建，需要通过 event dispatcher manager 来
func (e *EventDispatcherManager) NewTableEventDispatcher(tableSpan *Span, startTs uint64) *dispatcher.TableEventDispatcher {
	// 创建新的 event dispatcher，同时需要把这个去 logService 注册，并且把自己加到对应的某个处理 thread 里
	if len(e.DispatcherMap) == 0 {
		e.Init()
	}

	dispatcher := dispatcher.TableEventDispatcher{
		Id:            genUniqueEventDispatcherID(),
		Ch:            make(chan *Event, 1000),
		TableSpan:     tableSpan,
		Sink:          e.Sink,
		State:         dispatcher.NewState(),
		ResolvedTs:    startTs,
		HeartbeatChan: make(chan *dispatcher.HeartBeatResponseMessage, 100),
	}

	e.EventCollector.RegisterDispatcher(&dispatcher)

	return &dispatcher
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
