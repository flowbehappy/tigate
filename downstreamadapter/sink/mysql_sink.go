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
	"net/url"

	"github.com/pingcap/ticdc/downstreamadapter/sink/types"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"golang.org/x/sync/errgroup"

	utils "github.com/pingcap/tiflow/pkg/util"
)

const (
	prime = 31
)

// MysqlSink is responsible for writing data to mysql downstream.
// Including DDL and DML.
type MysqlSink struct {
	changefeedID common.ChangeFeedID

	ddlWorker   *worker.MysqlDDLWorker
	dmlWorker   []*worker.MysqlDMLWorker
	workerCount int

	db         *sql.DB
	errgroup   *errgroup.Group
	statistics *metrics.Statistics
	errCh      chan error
}

func NewMysqlSink(ctx context.Context, changefeedID common.ChangeFeedID, workerCount int, config *config.ChangefeedConfig, sinkURI *url.URL, errCh chan error) (*MysqlSink, error) {
	errgroup, ctx := errgroup.WithContext(ctx)
	mysqlSink := MysqlSink{
		changefeedID: changefeedID,
		dmlWorker:    make([]*worker.MysqlDMLWorker, workerCount),
		workerCount:  workerCount,
		errgroup:     errgroup,
		statistics:   metrics.NewStatistics(changefeedID, "TxnSink"),
		errCh:        errCh,
	}

	cfg, db, err := mysql.NewMysqlConfigAndDB(ctx, changefeedID, sinkURI)
	if err != nil {
		return nil, err
	}
	cfg.SyncPointRetention = utils.GetOrZero(config.SyncPointRetention)

	for i := 0; i < workerCount; i++ {
		mysqlSink.dmlWorker[i] = worker.NewMysqlDMLWorker(ctx, db, cfg, i, mysqlSink.changefeedID, errgroup, mysqlSink.statistics)
	}
	mysqlSink.ddlWorker = worker.NewMysqlDDLWorker(ctx, db, cfg, mysqlSink.changefeedID, errgroup, mysqlSink.statistics)
	mysqlSink.db = db

	go mysqlSink.run()

	return &mysqlSink, nil
}

func (s *MysqlSink) run() {
	for i := 0; i < s.workerCount; i++ {
		s.dmlWorker[i].Run()
	}
	s.errCh <- s.errgroup.Wait()
}

func (s *MysqlSink) SinkType() SinkType {
	return MysqlSinkType
}

func (s *MysqlSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}

func (s *MysqlSink) AddDMLEvent(event *commonEvent.DMLEvent, tableProgress *types.TableProgress) {
	if event.Len() == 0 {
		return
	}

	tableProgress.Add(event)

	// Considering that the parity of tableID is not necessarily even,
	// directly dividing by the number of buckets may cause unevenness between buckets.
	// Therefore, we first take the modulus of the prime number and then take the modulus of the bucket.
	index := int64(event.PhysicalTableID) % prime % int64(s.workerCount)
	s.dmlWorker[index].GetEventChan() <- event
}

func (s *MysqlSink) PassBlockEvent(event commonEvent.BlockEvent, tableProgress *types.TableProgress) {
	tableProgress.Pass(event)
	event.PostFlush()
}

func (s *MysqlSink) WriteBlockEvent(event commonEvent.BlockEvent, tableProgress *types.TableProgress) error {
	tableProgress.Add(event)
	return s.ddlWorker.WriteBlockEvent(event)
}

func (s *MysqlSink) AddCheckpointTs(ts uint64) {}

func (s *MysqlSink) CheckStartTsList(tableIds []int64, startTsList []int64) ([]int64, error) {
	return s.ddlWorker.CheckStartTsList(tableIds, startTsList)
}

func (s *MysqlSink) Close(removeDDLTsItem bool) error {
	if removeDDLTsItem {
		return s.ddlWorker.RemoveDDLTsItem()
	}
	for i := 0; i < s.workerCount; i++ {
		s.dmlWorker[i].Close()
	}

	s.ddlWorker.Close()

	s.db.Close()
	s.statistics.Close()
	return nil
}
