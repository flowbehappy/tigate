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

package threadpool

import (
	"sync"
)

/*
TaskSchedulerInstance is a singleton instance. It contains the necessary task schedulers for one instance.
*/
type TaskSchedulerInstance struct {
	// WorkerTaskScheduler          *TaskScheduler
	// EventDispatcherTaskScheduler *TaskScheduler
	// SinkTaskScheduler            *TaskScheduler
	// HeartbeatTaskScheduler       *TaskScheduler
	// EventServiceTaskScheduler    *TaskScheduler
	// MaintainerTaskScheduler *TaskScheduler
}

var TaskSchedulers *TaskSchedulerInstance
var once sync.Once

func GetTaskSchedulerInstance() *TaskSchedulerInstance {
	if TaskSchedulers == nil {
		once.Do(func() {
			TaskSchedulers = &TaskSchedulerInstance{
				// WorkerTaskScheduler:          NewTaskSchedulerDefault("WorkerTask"),
				// EventDispatcherTaskScheduler: NewTaskSchedulerDefault("EventDispatcherTask"),
				// SinkTaskScheduler:            NewTaskSchedulerDefault("SinkTask"),
				// HeartbeatTaskScheduler:       NewTaskSchedulerDefault("HeartbeatTask"),
				// EventServiceTaskScheduler:    NewTaskSchedulerDefault("EventServiceTask"),
				// MaintainerTaskScheduler: NewTaskSchedulerDefault("MaintainerTask"),
			}
		})
	}
	return TaskSchedulers
}

func SetTaskSchedulerInstance(taskSchedulerInstance *TaskSchedulerInstance) {
	TaskSchedulers = taskSchedulerInstance
}
