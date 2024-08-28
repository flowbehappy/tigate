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

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/logservice/txnutil"
	"github.com/flowbehappy/tigate/mounter"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

type ddlJobFetcher struct {
	puller *logpuller.LogPullerMultiSpan

	writeDDLEvent     func(ddlEvent DDLEvent) error
	advanceResolvedTs func(resolvedTS common.Ts) error

	// ddlTableInfo is initialized when receive the first concurrent DDL job.
	ddlTableInfo *mounter.DDLTableInfo
	// kvStorage is used to init `ddlTableInfo`
	kvStorage kv.Storage
}

func newDDLJobFetcher(
	pdCli pd.Client,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	kvStorage kv.Storage,
	startTs common.Ts,
	writeDDLEvent func(ddlEvent DDLEvent) error,
	advanceResolvedTs func(resolvedTS common.Ts) error,
) *ddlJobFetcher {
	clientConfig := &logpuller.SubscriptionClientConfig{
		RegionRequestWorkerPerStore:   1,
		ChangeEventProcessorNum:       4,
		AdvanceResolvedTsIntervalInMs: 300,
	}
	client := logpuller.NewSubscriptionClient(
		"schemaStore",
		clientConfig,
		pdCli,
		regionCache,
		pdClock,
		txnutil.NewLockerResolver(kvStorage.(tikv.Storage)),
		&security.Credential{},
	)

	ddlJobFetcher := &ddlJobFetcher{
		writeDDLEvent:     writeDDLEvent,
		advanceResolvedTs: advanceResolvedTs,
		kvStorage:         kvStorage,
	}
	ddlSpans := getAllDDLSpan()
	ddlJobFetcher.puller = logpuller.NewLogPullerMultiSpan(client, pdClock, ddlSpans, startTs, ddlJobFetcher.input)

	return ddlJobFetcher
}

func (p *ddlJobFetcher) close(ctx context.Context) error {
	return p.puller.Close(ctx)
}

func (p *ddlJobFetcher) input(ctx context.Context, rawEvent *common.RawKVEntry) error {
	if rawEvent == nil {
		return nil
	}

	if rawEvent.IsResolved() {
		p.advanceResolvedTs(common.Ts(rawEvent.CRTs))
		return nil
	}

	job, err := p.unmarshalDDL(rawEvent)
	if err != nil {
		return errors.Trace(err)
	}

	if job == nil {
		return nil
	}

	p.writeDDLEvent(DDLEvent{
		Job:      job,
		CommitTS: common.Ts(rawEvent.CRTs),
	})

	return nil
}

func (p *ddlJobFetcher) unmarshalDDL(rawKV *common.RawKVEntry) (*model.Job, error) {
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

const (
	// JobTableID is the id of `tidb_ddl_job`.
	JobTableID = ddl.JobTableID
	// JobHistoryID is the id of `tidb_ddl_history`
	JobHistoryID = ddl.HistoryTableID
)

func getAllDDLSpan() []common.TableSpan {
	spans := make([]common.TableSpan, 0, 2)
	start, end := common.GetTableRange(JobTableID)
	spans = append(spans, common.TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  JobTableID,
			StartKey: common.ToComparableKey(start),
			EndKey:   common.ToComparableKey(end),
		},
	})
	start, end = common.GetTableRange(JobHistoryID)
	spans = append(spans, common.TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  JobHistoryID,
			StartKey: common.ToComparableKey(start),
			EndKey:   common.ToComparableKey(end),
		},
	})
	return spans
}
