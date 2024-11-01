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

package conflictdetector

import (
	"github.com/pingcap/ticdc/utils/threadpool"
)

// 每个 conflict detector 对应一个 notifyTask
// 从 notify 的 chan 拿函数，执行
type NotifyTask struct {
	notifiedChan *chan func()
	taskStatus   threadpool.TaskStatus
}

/*
func newNotifyTask(notifiedChan *chan func()) *NotifyTask {
	return &NotifyTask{
		notifiedChan: notifiedChan,
		taskStatus:   threadpool.CPUTask,
	}
}

func (t *NotifyTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}

func (t *NotifyTask) SetStatus(taskStatus threadpool.TaskStatus) {
	t.taskStatus = taskStatus
}

func (t *NotifyTask) Execute() (threadpool.TaskStatus, time.Time) {
	// timer := time.NewTimer(timeout)
	for {
		select {
		case function := <-*t.notifiedChan:
			if function != nil {
				function()
			}
		// case <-timer.C:
		// 	return threadpool.CPUTask
		default:
			// 要不要等一段时间？
			return threadpool.CPUTask, time.Time{}
		}
	}
}

func (t *NotifyTask) Await() threadpool.TaskStatus {
	log.Warn("NotifyTask should not call await()")
	return threadpool.Done
}

func (t *NotifyTask) Release() {
	// TODO:
}

func (t *NotifyTask) Cancel() {
}
*/
