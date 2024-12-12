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
	"math"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/heap"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var once sync.Once
var ddlTableInfo *event.DDLTableInfo

type ddlJobFetcher struct {
	resolvedTsTracker struct {
		sync.Mutex
		resolvedTsItemMap map[logpuller.SubscriptionID]*resolvedTsItem
		resolvedTsHeap    *heap.Heap[*resolvedTsItem]
	}

	// cacheDDLEvent and advanceResolvedTs may be called concurrently
	// the only gurantee is that when call advanceResolvedTs with ts, all ddl job with commit ts <= ts has been passed to cacheDDLEvent
	cacheDDLEvent     func(ddlEvent DDLJobWithCommitTs)
	advanceResolvedTs func(resolvedTS uint64)

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
	ddlJobFetcher.resolvedTsTracker.resolvedTsItemMap = make(map[logpuller.SubscriptionID]*resolvedTsItem)
	ddlJobFetcher.resolvedTsTracker.resolvedTsHeap = heap.NewHeap[*resolvedTsItem]()

	for _, span := range getAllDDLSpan() {
		subID := subClient.AllocSubscriptionID()
		item := &resolvedTsItem{
			resolvedTs: 0,
		}
		ddlJobFetcher.resolvedTsTracker.resolvedTsItemMap[subID] = item
		ddlJobFetcher.resolvedTsTracker.resolvedTsHeap.AddOrUpdate(item)
		advanceSubSpanResolvedTs := func(ts uint64) {
			ddlJobFetcher.tryAdvanceResolvedTs(subID, ts)
		}
		subClient.Subscribe(subID, span, startTs, ddlJobFetcher.input, advanceSubSpanResolvedTs, 0)
	}

	return ddlJobFetcher
}

func (p *ddlJobFetcher) run(ctx context.Context) error {
	return nil
}

func (p *ddlJobFetcher) close(ctx context.Context) error {
	return nil
}

func (p *ddlJobFetcher) tryAdvanceResolvedTs(subID logpuller.SubscriptionID, newResolvedTs uint64) {
	p.resolvedTsTracker.Lock()
	defer p.resolvedTsTracker.Unlock()
	item, ok := p.resolvedTsTracker.resolvedTsItemMap[subID]
	if !ok {
		log.Panic("unknown zubscriptionID, should not happen",
			zap.Uint64("subID", uint64(subID)))
	}
	if newResolvedTs < item.resolvedTs {
		log.Panic("resolved ts should not fallback",
			zap.Uint64("newResolvedTs", newResolvedTs),
			zap.Uint64("oldResolvedTs", item.resolvedTs))
	}
	item.resolvedTs = newResolvedTs
	p.resolvedTsTracker.resolvedTsHeap.AddOrUpdate(item)

	minResolvedTsItem, ok := p.resolvedTsTracker.resolvedTsHeap.PeekTop()
	if !ok || minResolvedTsItem.resolvedTs == math.MaxUint64 {
		log.Panic("should not happen")
	}
	// minResolvedTsItem may be 0, it it ok to send it because it will be filtered later.
	// it is ok to send redundant resolved ts to advanceResolvedTs.
	p.advanceResolvedTs(minResolvedTsItem.resolvedTs)
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

// unmarshalDDL unmarshals a ddl job from a raw kv entry.
func (p *ddlJobFetcher) unmarshalDDL(rawKV *common.RawKVEntry) (*model.Job, error) {
	if rawKV.OpType != common.OpTypePut {
		return nil, nil
	}
	if !event.IsLegacyFormatJob(rawKV) {
		once.Do(func() {
			if err := initDDLTableInfo(p.kvStorage); err != nil {
				log.Fatal("init ddl table info failed", zap.Error(err))
			}
		})
	}

	return event.ParseDDLJob(rawKV, ddlTableInfo)
}

func initDDLTableInfo(kvStorage kv.Storage) error {
	version, err := kvStorage.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	snap := logpuller.GetSnapshotMeta(kvStorage, version.Ver)

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

	ddlTableInfo = &event.DDLTableInfo{}
	ddlTableInfo.DDLJobTable = common.WrapTableInfo(db.ID, db.Name.L, tableInfo)
	ddlTableInfo.JobMetaColumnIDinJobTable = col.ID

	// for tidb_ddl_history
	historyTableInfo, err := findTableByName(tbls, "tidb_ddl_history")
	if err != nil {
		return errors.Trace(err)
	}

	historyTableCol, err := findColumnByName(historyTableInfo.Columns, "job_meta")
	if err != nil {
		return errors.Trace(err)
	}

	ddlTableInfo.DDLHistoryTable = common.WrapTableInfo(db.ID, db.Name.L, historyTableInfo)
	ddlTableInfo.JobMetaColumnIDinHistoryTable = historyTableCol.ID

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

type resolvedTsItem struct {
	resolvedTs uint64
	heapIndex  int
}

func (m *resolvedTsItem) SetHeapIndex(index int) { m.heapIndex = index }

func (m *resolvedTsItem) GetHeapIndex() int { return m.heapIndex }

func (m *resolvedTsItem) LessThan(other *resolvedTsItem) bool {
	return m.resolvedTs < other.resolvedTs
}
