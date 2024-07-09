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
	"time"

	"github.com/flowbehappy/tigate/utils/threadpool"

	"github.com/pingcap/log"
)

// 每个 conflict detector 对应一个 notifyTask
// 从 notify 的 chan 拿函数，执行
type NotifyTask struct {
	notifiedChan *chan func()
	taskStatus   threadpool.TaskStatus
}

func newNotifyTask(notifiedChan *chan func()) *NotifyTask {
	return &NotifyTask{
		notifiedChan: notifiedChan,
		taskStatus:   threadpool.Running,
	}
}

func (t *NotifyTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}

func (t *NotifyTask) SetStatus(taskStatus threadpool.TaskStatus) {
	t.taskStatus = taskStatus
}

func (t *NotifyTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	timer := time.NewTimer(timeout)
	for {
		select {
		case function := <-*t.notifiedChan:
			if function != nil {
				function()
			}
		case <-timer.C:
			return threadpool.Running
		default:
			// 要不要等一段时间？
			return threadpool.Running
		}
	}
}

func (t *NotifyTask) Await() threadpool.TaskStatus {
	log.Warn("NotifyTask should not call await()")
	return threadpool.Failed
}

func (t *NotifyTask) Release() {
	// TODO:
}
