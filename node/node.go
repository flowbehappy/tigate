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

package node

import (
	"new_arch/downstreamadapter/dispatcher"
	"new_arch/downstreamadapter/dispatchermanager"
	"new_arch/utils/threadpool"
	"sync/atomic"
)

var uniqueEventDispatcherManagerID uint64 = 0

func genUniqueEventDispatcherManagerID() uint64 {
	return atomic.AddUint64(&uniqueEventDispatcherManagerID, 1)
}

// 代表每个节点
type Node struct {
	workerTaskScheduler          *threadpool.TaskScheduler
	eventDispatcherTaskScheduler *threadpool.TaskScheduler
	sinkDispatcherTaskScheduler  *threadpool.TaskScheduler
}

func NewNode() *Node {
	node := Node{
		workerTaskScheduler:          threadpool.NewTaskScheduler(),
		eventDispatcherTaskScheduler: threadpool.NewTaskScheduler(),
		sinkDispatcherTaskScheduler:  threadpool.NewTaskScheduler(),
	}

	return &node
}

// 收到 create event dispatcher manager 时候创建，会可能是一个空的 manager
func (n *Node) NewEventDispatcherManager(changefeedID uint64, sinkType string, addr string, config *ChangefeedConfig) *dispatchermanager.EventDispatcherManager {
	return &dispatchermanager.EventDispatcherManager{
		DispatcherMap:                make(map[*Span]*dispatcher.TableEventDispatcher),
		ChangefeedID:                 changefeedID,
		Id:                           genUniqueEventDispatcherManagerID(),
		SinkType:                     sinkType,
		WorkerTaskScheduler:          n.workerTaskScheduler,
		EventDispatcherTaskScheduler: n.eventDispatcherTaskScheduler,
		SinkDispatcherTaskScheduler:  n.sinkDispatcherTaskScheduler,
		SinkConfig:                   config.SinkConfig,
		LogServiceAddr:               addr, // 这个需要其他机制来更新
		EnableSyncPoint:              config.EnableSyncPoint,
		SyncPointInterval:            config.SyncPointInterval,
	}
}
