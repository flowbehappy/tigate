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

package schemastore

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

type ddlJobFetcher struct {
	puller *logpuller.LogPullerMultiSpan

	cacheDDLEvent func(ddlEvent DDLJobWithCommitTs)

	advanceResolvedTs func(resolvedTS uint64)

	// ddlTableInfo is initialized when receive the first concurrent DDL job.
	ddlTableInfo *event.DDLTableInfo
	// kvStorage is used to init `ddlTableInfo`
	kvStorage kv.Storage
}

func newDDLJobFetcher(
	subClient *logpuller.SubscriptionClient,
	pdCli pd.Client,
	pdClock pdutil.Clock,
	kvStorage kv.Storage,
	startTs uint64,
	cacheDDLEvent func(ddlEvent DDLJobWithCommitTs),
	advanceResolvedTs func(resolvedTS uint64),
) *ddlJobFetcher {
	ddlJobFetcher := &ddlJobFetcher{
		cacheDDLEvent:     cacheDDLEvent,
		advanceResolvedTs: advanceResolvedTs,
		kvStorage:         kvStorage,
	}
	ddlSpans := getAllDDLSpan()
	ddlJobFetcher.puller = logpuller.NewLogPullerMultiSpan(subClient, pdClock, ddlSpans, startTs, ddlJobFetcher.input, advanceResolvedTs)

	return ddlJobFetcher
}

func (p *ddlJobFetcher) run(ctx context.Context) error {
	return p.puller.Run(ctx)
}

func (p *ddlJobFetcher) close(ctx context.Context) error {
	return p.puller.Close(ctx)
}

func (p *ddlJobFetcher) input(kvs []common.RawKVEntry, _ func()) bool {
	for _, kv := range kvs {
		job, err := p.unmarshalDDL(&kv)
		if err != nil {
			log.Fatal("unmarshal ddl failed", zap.Any("kv", kv), zap.Error(err))
		}

		if job == nil {
			continue
		}

		// cache ddl job in memory until the resolve ts pass its commit ts
		p.cacheDDLEvent(DDLJobWithCommitTs{
			Job:      job,
			CommitTs: kv.CRTs,
		})
	}
	return false
}

func (p *ddlJobFetcher) unmarshalDDL(rawKV *common.RawKVEntry) (*model.Job, error) {
	if rawKV.OpType != common.OpTypePut {
		return nil, nil
	}
	if p.ddlTableInfo == nil && !event.IsLegacyFormatJob(rawKV) {
		log.Info("begin to init ddl table info")
		err := p.initDDLTableInfo()
		if err != nil {
			log.Error("init ddl table info failed", zap.Error(err))
			return nil, errors.Trace(err)
		}
	}

	return event.ParseDDLJob(rawKV, p.ddlTableInfo)
}

func (p *ddlJobFetcher) initDDLTableInfo() error {
	version, err := p.kvStorage.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	snap := logpuller.GetSnapshotMeta(p.kvStorage, version.Ver)

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

	p.ddlTableInfo = &event.DDLTableInfo{}
	p.ddlTableInfo.DDLJobTable = common.WrapTableInfo(db.ID, db.Name.L, tableInfo)
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

	p.ddlTableInfo.DDLHistoryTable = common.WrapTableInfo(db.ID, db.Name.L, historyTableInfo)
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

const (
	// JobTableID is the id of `tidb_ddl_job`.
	JobTableID = ddl.JobTableID
	// JobHistoryID is the id of `tidb_ddl_history`
	JobHistoryID = ddl.HistoryTableID
)

func getAllDDLSpan() []heartbeatpb.TableSpan {
	spans := make([]heartbeatpb.TableSpan, 0, 2)
	start, end := common.GetTableRange(JobTableID)
	spans = append(spans, heartbeatpb.TableSpan{
		TableID:  JobTableID,
		StartKey: common.ToComparableKey(start),
		EndKey:   common.ToComparableKey(end),
	})
	start, end = common.GetTableRange(JobHistoryID)
	spans = append(spans, heartbeatpb.TableSpan{
		TableID:  JobHistoryID,
		StartKey: common.ToComparableKey(start),
		EndKey:   common.ToComparableKey(end),
	})
	return spans
}
