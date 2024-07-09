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

import (
	"time"

	"github.com/flowbehappy/tigate/common"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/conflictdetector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
)

// 先写一个插入数据的 task，看看后面 reactor 的要不要写一个新的 task
// 同个 table 的 add event 是需要满足前后顺序的，所以这里的 task 还是只应该有 dispatcher 个
// 那也就是她的 task 是每一个新的 table 出现就创建，然后永远在轮训，直到 dispatcher 被移除了 -- 所以要有个 table 和 task 的 map
type MysqlSinkTask struct {
	conflictDetector *conflictdetector.ConflictDetector // 这个会被多个任务共用，所以需要做到内部 thread safe
	tableSpan        *common.TableSpan
	tableProgress    *types.TableProgress
	eventCh          chan *common.TxnEvent
	taskStatus       threadpool.TaskStatus
}

// 这个任务本身就是把
func newMysqlSinkTask(tableSpan *common.TableSpan, tableProgress *types.TableProgress, eventCh chan *common.TxnEvent, conflictDetector *conflictdetector.ConflictDetector) *MysqlSinkTask {
	return &MysqlSinkTask{
		conflictDetector: conflictDetector,
		tableSpan:        tableSpan,
		tableProgress:    tableProgress,
		eventCh:          eventCh,
		taskStatus:       threadpool.Running,
	}
}

func (t *MysqlSinkTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}

func (t *MysqlSinkTask) SetStatus(taskStatus threadpool.TaskStatus) {
	t.taskStatus = taskStatus
}

func (t *MysqlSinkTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	// 从 pending 的 task 里面拿出来塞下去，指导超时或者没有 event 了
	if t.taskStatus == threadpool.Failed {
		return t.taskStatus
	}
	timer := time.NewTimer(timeout)
	for {
		select {
		case event := <-t.eventCh:
			t.conflictDetector.Add(event, t.tableProgress) // 这个要测过一次要多少，有可能直接超时了;以及这个写法要测过不知道对不对
		case <-timer.C:
			return threadpool.Running
		default:
			// 也就是还没超时，但是 event 也没有，那就直接把任务扔回去
			if !timer.Stop() {
				<-timer.C
			}
			return threadpool.Running
		}
	}
}

func (t *MysqlSinkTask) Cancel() {
	t.taskStatus = threadpool.Failed
}

func (t *MysqlSinkTask) Await() threadpool.TaskStatus {
	log.Error("MysqlSinkTask should not call await()")
	return threadpool.Failed
}

func (t *MysqlSinkTask) Release() {
	//?给我整不会了感觉啥也不用干啊，是不是因为 go 自动回收所以啥也不用干？
}
