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
	"sync/atomic"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
)

var uniqueEventDispatcherManagerID uint64 = 0

func genUniqueEventDispatcherManagerID() uint64 {
	return atomic.AddUint64(&uniqueEventDispatcherManagerID, 1)
}

/*
Instance represents a server instance.
Instance has 4 task scheduler to manage various tasks in all dispatchers in this instance:
1. Worker Task Scheduler: manage MysqlWorkerDDLEventTasks and MysqlWorkerDMLEventTasks in all Sink, which works for flush events to downstream
2. Event Dispatcher Task Scheduler: manage the EventDispatcherTasks of all dispatchers, which works for dispatching events to Sink.
3. Sink Task Scheduler: manage the SinkTasks of all Sinks, which works for dealing with events' conflictors, and push events to Workers.
4. Heartbeat Task Scheduler: manage the HeartbeatTasks of all dispatchers, which works for collecting heartbeat message from dispatchers, and dispatching heartbeat response to disptachers.

Instance also has:
1. a HeartBeatCollector, which is responsible for the heartbeat communication with the maintainer.
2. an EventCollector, which is responsible for collecting events from LogService, and dispatching them to dispatchers.
*/

type Instance struct {
	heartBeatCollector        *HeartBeatCollector
	eventCollector            *EventCollector
	eventDispatcherManagerMap map[uint64]*dispatchermanager.EventDispatcherManager
}

func NewInstance() *Instance {
	instance := Instance{
		heartBeatCollector: newHeartBeatCollector(addr, 1000),
		eventCollector:     newEventCollector(logServiceAddr),
	}

	return &instance
}

// 收到 create event dispatcher manager 时候创建，会可能是一个空的 manager
func (i *Instance) NewEventDispatcherManager(changefeedID uint64, config *ChangefeedConfig) *dispatchermanager.EventDispatcherManager {
	eventDispatcherManager := dispatchermanager.EventDispatcherManager{
		DispatcherMap:          make(map[*Span]*dispatcher.TableEventDispatcher),
		ChangefeedID:           changefeedID,
		Id:                     genUniqueEventDispatcherManagerID(),
		HeartbeatResponseQueue: dispatchermanager.NewHeartbeatResponseQueue(),
		SinkType:               config.sinkType,
		SinkConfig:             config.SinkConfig,
		EnableSyncPoint:        config.EnableSyncPoint,
		SyncPointInterval:      config.SyncPointInterval,
		Filter:                 config.Filter, // TODO
		EventCollector:         i.eventCollector,
	}
	i.eventDispatcherManagerMap[eventDispatcherManager.ChangefeedID] = &eventDispatcherManager
	return &eventDispatcherManager
}
