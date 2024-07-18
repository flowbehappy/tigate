// Copyright 2022 PingCAP, Inc.
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

package puller

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/logservice/eventsource"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/logservice/txnutil"
	"github.com/flowbehappy/tigate/logservice/upstream"
	"github.com/flowbehappy/tigate/mounter"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	ddlPullerStuckWarnDuration = 30 * time.Second
	// ddl puller should never filter any DDL jobs even if
	// the changefeed is in BDR mode, because the DDL jobs should
	// be filtered before they are sent to the sink
	ddlPullerFilterLoop = false
)

// DDLJobPuller is used to pull ddl job from TiKV.
type DDLJobPuller interface {
}

// Note: All unexported methods of `ddlJobPullerImpl` should
// be called in the same one goroutine.
type ddlJobPullerImpl struct {
	mp            *MultiplexingPuller
	kvStorage     kv.Storage
	schemaStore   schemastore.SchemaStore
	resolvedTs    uint64
	schemaVersion int64
	// ddlTableInfo is initialized when receive the first concurrent DDL job.
	ddlTableInfo *mounter.DDLTableInfo
}

// DDLPullerTableName is the fake table name for ddl puller
const DDLPullerTableName = "DDL_PULLER"

// NewDDLJobPuller creates a new NewDDLJobPuller,
// which fetches ddl events starting from checkpointTs.
func NewDDLJobPuller(
	ctx context.Context,
	up *upstream.Upstream,
	checkpointTs uint64,
	schemaStore schemastore.SchemaStore,
) DDLJobPuller {
	pdCli := up.PDClient
	regionCache := up.RegionCache
	kvStorage := up.KVStorage
	pdClock := up.PDClock

	ddlSpans := spanz.GetAllDDLSpan()
	for i := range ddlSpans {
		// NOTE(qupeng): It's better to use different table id when use sharedKvClient.
		ddlSpans[i].TableID = int64(-1) - int64(i)
	}

	ddlJobPuller := &ddlJobPullerImpl{
		schemaStore: schemaStore,
		kvStorage:   kvStorage,
	}

	grpcPool := eventsource.NewConnAndClientPool(up.SecurityConfig)
	clientConfig := &eventsource.SharedClientConfig{
		KVClientWorkerConcurrent:     16,
		KVClientGrpcStreamConcurrent: 4,
		KVClientAdvanceIntervalInMs:  300,
	}
	client := eventsource.NewSharedClient(
		clientConfig,
		ddlPullerFilterLoop,
		pdCli, grpcPool, regionCache, pdClock,
		txnutil.NewLockerResolver(kvStorage.(tikv.Storage)),
	)

	slots, hasher := 1, func(tablepb.Span, int) int { return 0 }
	ddlJobPuller.mp = NewMultiplexingPuller(client, up.PDClock, ddlJobPuller.Input, slots, hasher, 1)
	ddlJobPuller.mp.Subscribe(ddlSpans, eventsource.Ts(checkpointTs), DDLPullerTableName)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return ddlJobPuller.mp.Run(ctx)
	})

	return ddlJobPuller
}

// Input receives the raw kv entry and put it into the input channel.
func (p *ddlJobPullerImpl) Input(
	ctx context.Context,
	rawEvent *common.RawKVEntry,
	_ []tablepb.Span,
) error {
	if rawEvent == nil {
		return nil
	}

	if rawEvent.OpType == common.OpTypeResolved {
		p.schemaStore.AdvanceResolvedTS(schemastore.Timestamp(rawEvent.CRTs))
		return nil
	}

	job, err := p.unmarshalDDL(rawEvent)
	if err != nil {
		return errors.Trace(err)
	}

	if job == nil {
		return nil
	}

	if job.BinlogInfo.FinishedTS <= p.getResolvedTs() ||
		job.BinlogInfo.SchemaVersion <= p.schemaVersion {
		log.Info("ddl job finishedTs less than puller resolvedTs,"+
			"discard the ddl job",
			zap.String("schema", job.SchemaName),
			zap.String("table", job.TableName),
			zap.Uint64("startTs", job.StartTS),
			zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
			zap.String("query", job.Query),
			zap.Uint64("pullerResolvedTs", p.getResolvedTs()))
		return nil
	}

	p.schemaStore.WriteDDLEvent(schemastore.DDLEvent{
		Job:      job,
		CommitTS: schemastore.Timestamp(rawEvent.CRTs),
	})

	p.setResolvedTs(job.BinlogInfo.FinishedTS)
	p.schemaVersion = job.BinlogInfo.SchemaVersion

	return nil
}

func (p *ddlJobPullerImpl) unmarshalDDL(rawKV *common.RawKVEntry) (*model.Job, error) {
	if rawKV.OpType != common.OpTypePut {
		return nil, nil
	}
	if p.ddlTableInfo == nil && !mounter.IsLegacyFormatJob(rawKV) {
		err := p.initDDLTableInfo()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return mounter.ParseDDLJob(rawKV, p.ddlTableInfo)
}

func (p *ddlJobPullerImpl) getResolvedTs() uint64 {
	return atomic.LoadUint64(&p.resolvedTs)
}

func (p *ddlJobPullerImpl) setResolvedTs(ts uint64) {
	atomic.StoreUint64(&p.resolvedTs, ts)
}

func (p *ddlJobPullerImpl) initDDLTableInfo() error {
	version, err := p.kvStorage.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	snap := eventsource.GetSnapshotMeta(p.kvStorage, version.Ver)

	dbInfos, err := snap.ListDatabases()
	if err != nil {
		return cerror.WrapError(cerror.ErrMetaListDatabases, err)
	}

	db, err := findDBByName(dbInfos, mysql.SystemDB)
	if err != nil {
		return errors.Trace(err)
	}

	tbls, err := snap.ListTables(db.ID)
	if err != nil {
		return errors.Trace(err)
	}

	// for tidb_ddl_job
	tableInfo, err := findTableByName(tbls, "tidb_ddl_job")
	if err != nil {
		return errors.Trace(err)
	}

	col, err := findColumnByName(tableInfo.Columns, "job_meta")
	if err != nil {
		return errors.Trace(err)
	}

	p.ddlTableInfo = &mounter.DDLTableInfo{}
	p.ddlTableInfo.DDLJobTable = common.WrapTableInfo(db.ID, db.Name.L, 0, tableInfo)
	p.ddlTableInfo.JobMetaColumnIDinJobTable = col.ID

	// for tidb_ddl_history
	historyTableInfo, err := findTableByName(tbls, "tidb_ddl_history")
	if err != nil {
		return errors.Trace(err)
	}

	historyTableCol, err := findColumnByName(historyTableInfo.Columns, "job_meta")
	if err != nil {
		return errors.Trace(err)
	}

	p.ddlTableInfo.DDLHistoryTable = common.WrapTableInfo(db.ID, db.Name.L, 0, historyTableInfo)
	p.ddlTableInfo.JobMetaColumnIDinHistoryTable = historyTableCol.ID

	return nil
}

// Below are some helper functions for ddl puller.
func findDBByName(dbs []*model.DBInfo, name string) (*model.DBInfo, error) {
	for _, db := range dbs {
		if db.Name.L == name {
			return db, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find schema %s", name))
}

func findTableByName(tbls []*model.TableInfo, name string) (*model.TableInfo, error) {
	for _, t := range tbls {
		if t.Name.L == name {
			return t, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find table %s", name))
}

func findColumnByName(cols []*model.ColumnInfo, name string) (*model.ColumnInfo, error) {
	for _, c := range cols {
		if c.Name.L == name {
			return c, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find column %s", name))
}
