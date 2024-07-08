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

package worker

import (
	"database/sql"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/utils/threadpool"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// MysqlWorker is use to flush the event downstream
type MysqlWorker struct { // TODO:这个可以同时做两个 flush 么？先这么做，不行后面拆个 worker 出来
	eventChan   <-chan *Event // 获取到能往下游写的 events
	mysqlWriter MysqlWriter   // 实际负责做 flush 操作
}

type MysqlDDLWorker struct {
	MysqlWriter MysqlWriter // 实际负责做 flush 操作
}

// 这个  task 是单次出现的，执行完就结束，用于处理 ddl 和 sync point event
type MysqlWorkerDDLEventTask struct {
	worker     *MysqlDDLWorker
	event      *Event
	taskStatus threadpool.TaskStatus
	callback   func()
}

func NewMysqlWorkerDDLEventTask(worker *MysqlDDLWorker, event *Event) *MysqlWorkerDDLEventTask {
	return &MysqlWorkerDDLEventTask{
		worker:     worker,
		event:      event,
		taskStatus: threadpool.IO,
	}
}

func (t *MysqlWorkerDDLEventTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}

func (t *MysqlWorkerDDLEventTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	t.worker.MysqlWriter.FlushDDLEvent(t.event)
	return threadpool.Success
}

func (t *MysqlWorkerDDLEventTask) Await() threadpool.TaskStatus {
	log.Error("MysqlWorkerDDLEventTask should not call await()")
	return threadpool.Failed
}

func (t *MysqlWorkerDDLEventTask) Release() {
	//
}

// 这个 task 应该是 event dispatcher manager 创建以后，会直接生成好的 task，扔到一个单独的 threadpool 中
// task 的 生命周期应该是跟 event dispatcher manager 一样
// 这个 task 只处理 dml event
type MysqlWorkerDMLEventTask struct {
	worker     *MysqlWorker
	taskStatus threadpool.TaskStatus
	maxRows    int
	events     []*Event
	ticker     *time.Ticker
}

func NewMysqlWorkerDMLEventTask(eventChan <-chan *Event, db *sql.DB, config *writer.MysqlConfig, maxRows int) *MysqlWorkerDMLEventTask {
	return &MysqlWorkerDMLEventTask{
		worker: &MysqlWorker{
			eventChan:   eventChan,
			mysqlWriter: writer.NewMysqlWriter(db, config),
		},
		taskStatus: threadpool.Running,
		maxRows:    maxRows,
	}
}

func (t *MysqlWorkerDMLEventTask) GetStatus() threadpool.TaskStatus {
	return t.taskStatus
}
func (t *MysqlWorkerDMLEventTask) Execute(timeout time.Duration) threadpool.TaskStatus {
	switch t.taskStatus {
	case threadpool.Running:
		return t.executeImpl()
	case threadpool.IO:
		return t.executeIOImpl()
	default:
		log.Error("Unexpected task status: ", zap.Any("status", t.taskStatus))
		return threadpool.Failed
	}
}

func (t *MysqlWorkerDMLEventTask) executeIOImpl() threadpool.TaskStatus {
	if len(t.events) == 0 {
		log.Warning("here is no events to flush")
		return threadpool.Running
	}
	// flush events
	err := t.worker.mysqlWriter.flush(t.events)
	if err != nil {
		log.Error("Failed to flush events", err)
		return threadpool.Failed
	}
	t.events = nil
	return threadpool.Running
}

func (t *MysqlWorkerDMLEventTask) executeImpl() threadpool.TaskStatus {
	// check events is empty
	if len(t.events) > 0 {
		log.Error("events is not empty in MysqlWorkerTask")
		return threadpool.Failed
	}
	rows := 0

	// check if channel has events, if not directly back to taskQueue
	select {
	case txnEvent := <-t.worker.eventChan:
		t.events = append(t.events, txnEvent)
		rows += txnEvent.rows
		if rows >= t.maxRows {
			return threadpool.IO
		}
	default:
		return threadpool.Running
	}

	// get enough events or wait for 10 millseconds to make task go to IO Status. -- 这边可以考虑到底是拿不到 event 就 换出去 flush 好还是要等好，具体等多久好
	// 这边甚至可以想一下是不是不用等 10ms，没数据就直接刷下去，flush 时间远超过这个攒批时间的
	timer := time.NewTimer(10 * time.Microsecond)
	for {
		select {
		case txnEvent := <-t.worker.eventChan:
			t.events = append(t.events, txnEvent)
			rows += txnEvent.rows
			if rows >= t.maxRows {
				if !timer.Stop() {
					<-timer.C
				}
				return threadpool.IO
			}
		case <-timer.C:
			if rows == 0 {
				return threadpool.Running
			}
		}
	}
}

func (t *MysqlWorkerDMLEventTask) Await() threadpool.TaskStatus {
	log.Error("MysqlWorkerTask should not call await()")
	return threadpool.Failed
}

// 只有重启或者出问题的时候才 release
func (t *MysqlWorkerDMLEventTask) Release() {
	// 直接关闭应该就可以把？
	// TODO:需要取出 events 么
	// 不知道要干嘛，不干会咋样么
}
