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
	"context"
	"database/sql"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/downstreamadapter/worker"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"

	"github.com/pingcap/tiflow/cdc/model"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector. TODO:check this
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

// mysql sink 负责 mysql 类型下游的 sink 模块
// 一个 event dispatcher manager 对应一个 mysqlSink
// 实现 Sink 的接口
type MysqlSink struct {
	changefeedID model.ChangeFeedID

	ddlWorker   *worker.MysqlDDLWorker
	dmlWorker   []*worker.MysqlWorker
	workerCount int
}

// event dispatcher manager 初始化的时候创建 mysqlSink 对象
func NewMysqlSink(changefeedID model.ChangeFeedID, workerCount int, cfg *writer.MysqlConfig, db *sql.DB) *MysqlSink {
	ctx := context.Background()
	mysqlSink := MysqlSink{
		changefeedID: changefeedID,
		dmlWorker:    make([]*worker.MysqlWorker, workerCount),
		workerCount:  workerCount,
	}

	for i := 0; i < workerCount; i++ {
		mysqlSink.dmlWorker[i] = worker.NewMysqlWorker(db, cfg, i, mysqlSink.changefeedID, ctx, cfg.MaxTxnRow)
	}
	mysqlSink.ddlWorker = worker.NewMysqlDDLWorker(db, cfg, mysqlSink.changefeedID, ctx)

	return &mysqlSink
}

func (s *MysqlSink) SinkType() SinkType {
	return MysqlSinkType
}

func (s *MysqlSink) AddDMLEvent(event *commonEvent.DMLEvent, tableProgress *types.TableProgress) {
	if event.Len() == 0 {
		return
	}

	tableProgress.Add(event)

	// TODO:后续再优化这里的逻辑，目前有个问题是 physical table id 好像都是偶数？这个后面改个能见人的方法
	index := int64(event.PhysicalTableID) % int64(s.workerCount)
	s.dmlWorker[index].GetEventChan() <- event
}

func (s *MysqlSink) PassDDLAndSyncPointEvent(event *commonEvent.DDLEvent, tableProgress *types.TableProgress) {
	tableProgress.Pass(event)
}

func (s *MysqlSink) AddDDLAndSyncPointEvent(event *commonEvent.DDLEvent, tableProgress *types.TableProgress) {
	tableProgress.Add(event)
	s.ddlWorker.GetDDLEventChan() <- event
}

func (s *MysqlSink) AddCheckpointTs(ts uint64, tableNames []*commonEvent.SchemaTableName) {}

func (s *MysqlSink) Close() {}
