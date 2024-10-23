// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	 http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package event

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	ticonfig "github.com/pingcap/tidb/pkg/config"
	tiddl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	timeta "github.com/pingcap/tidb/pkg/meta"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// EventTestHelper is a test helper for generating test events
type EventTestHelper struct {
	t       testing.TB
	tk      *testkit.TestKit
	storage kv.Storage
	domain  *domain.Domain
	mounter Mounter

	tableInfos map[string]*common.TableInfo
}

// NewEventTestHelper creates a SchemaTestHelper
func NewEventTestHelper(t testing.TB) *EventTestHelper {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		conf.AlterPrimaryKey = true
	})
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(t, store)

	require.NoError(t, err)

	mounter := NewMounter(time.Local)

	return &EventTestHelper{
		t:          t,
		tk:         tk,
		storage:    store,
		domain:     domain,
		mounter:    mounter,
		tableInfos: make(map[string]*common.TableInfo),
	}
}

func (s *EventTestHelper) ApplyJob(job *timodel.Job) {
	key := toTableInfosKey(job.SchemaName, job.TableName)
	log.Info("apply job", zap.String("jobKey", key), zap.Any("job", job))
	info := common.WrapTableInfo(
		job.SchemaID,
		job.SchemaName,
		job.BinlogInfo.TableInfo)
	info.InitPreSQLs()
	s.tableInfos[key] = info
}

func (s *EventTestHelper) GetTableInfo(job *timodel.Job) *common.TableInfo {
	key := toTableInfosKey(job.SchemaName, job.TableName)
	log.Info("apply job", zap.String("jobKey", key), zap.Any("job", job))
	return s.tableInfos[key]
}

// DDL2Job executes the DDL stmt and returns the DDL job
func (s *EventTestHelper) DDL2Job(ddl string) *timodel.Job {
	s.tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(s.GetCurrentMeta(), 1)
	require.Nil(s.t, err)
	require.Len(s.t, jobs, 1)
	// Set State from Synced to Done.
	// Because jobs are put to history queue after TiDB alter its state from
	// Done to Synced.
	jobs[0].State = timodel.JobStateDone
	res := jobs[0]
	s.ApplyJob(res)
	if res.Type != timodel.ActionRenameTables {
		return res
	}

	// the RawArgs field in job fetched from tidb snapshot meta is incorrent,
	// so we manually construct `job.RawArgs` to do the workaround.
	// we assume the old schema name is same as the new schema name here.
	// for example, "ALTER TABLE RENAME test.t1 TO test.t1, test.t2 to test.t22", schema name is "test"
	schema := strings.Split(strings.Split(strings.Split(res.Query, ",")[1], " ")[1], ".")[0]
	tableNum := len(res.BinlogInfo.MultipleTableInfos)
	oldSchemaIDs := make([]int64, tableNum)
	for i := 0; i < tableNum; i++ {
		oldSchemaIDs[i] = res.SchemaID
	}
	oldTableIDs := make([]int64, tableNum)
	for i := 0; i < tableNum; i++ {
		oldTableIDs[i] = res.BinlogInfo.MultipleTableInfos[i].ID
	}
	newTableNames := make([]timodel.CIStr, tableNum)
	for i := 0; i < tableNum; i++ {
		newTableNames[i] = res.BinlogInfo.MultipleTableInfos[i].Name
	}
	oldSchemaNames := make([]timodel.CIStr, tableNum)
	for i := 0; i < tableNum; i++ {
		oldSchemaNames[i] = timodel.NewCIStr(schema)
	}
	newSchemaIDs := oldSchemaIDs

	args := []interface{}{
		oldSchemaIDs, newSchemaIDs,
		newTableNames, oldTableIDs, oldSchemaNames,
	}
	rawArgs, err := json.Marshal(args)
	require.NoError(s.t, err)
	res.RawArgs = rawArgs
	return res
}

// DDL2Jobs executes the DDL statement and return the corresponding DDL jobs.
// It is mainly used for "DROP TABLE" and "DROP VIEW" statement because
// multiple jobs will be generated after executing these two types of
// DDL statements.
func (s *EventTestHelper) DDL2Jobs(ddl string, jobCnt int) []*timodel.Job {
	s.tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(s.GetCurrentMeta(), jobCnt)
	require.Nil(s.t, err)
	require.Len(s.t, jobs, jobCnt)
	// Set State from Synced to Done.
	// Because jobs are put to history queue after TiDB alter its state from
	// Done to Synced.
	for i, job := range jobs {
		jobs[i].State = timodel.JobStateDone
		s.ApplyJob(job)
	}
	return jobs
}

// DML2Event execute the dml(s) and return the corresponding DMLEvent.
// Note:
// 1. It dose not support `delete` since the key value cannot be found
// after the query executed.
// 2. You must execute create table statement before calling this function.
// 3. You must set the preRow of the DMLEvent by yourself, since we can not get it from TiDB.
func (s *EventTestHelper) DML2Event(schema, table string, dml ...string) *DMLEvent {
	key := toTableInfosKey(schema, table)
	log.Info("dml2event", zap.String("key", key))
	tableInfo, ok := s.tableInfos[key]
	require.True(s.t, ok)
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS
	dmlEvent := NewDMLEvent(did, tableInfo.ID, ts-1, ts+1, tableInfo)
	rawKvs := s.DML2RawKv(schema, table, dml...)
	for _, rawKV := range rawKvs {
		err := dmlEvent.AppendRow(rawKV, s.mounter.DecodeToChunk)
		require.NoError(s.t, err)
	}
	return dmlEvent
}

func (s EventTestHelper) DML2RawKv(schema, table string, dml ...string) []*common.RawKVEntry {
	tableInfo, ok := s.tableInfos[toTableInfosKey(schema, table)]
	require.True(s.t, ok)
	ts := tableInfo.UpdateTS
	var rawKVs []*common.RawKVEntry
	for _, dml := range dml {
		s.tk.MustExec(dml)
		key, value := s.getLastKeyValue(tableInfo.ID)
		rawKV := &common.RawKVEntry{
			OpType:   common.OpTypePut,
			Key:      key,
			Value:    value,
			OldValue: nil,
			StartTs:  ts - 1,
			CRTs:     ts + 1,
		}
		rawKVs = append(rawKVs, rawKV)
	}
	return rawKVs
}

func (s *EventTestHelper) getLastKeyValue(tableID int64) (key, value []byte) {
	txn, err := s.storage.Begin()
	require.NoError(s.t, err)
	defer txn.Rollback() //nolint:errcheck

	start, end := common.GetTableRange(tableID)
	iter, err := txn.Iter(start, end)
	require.NoError(s.t, err)
	defer iter.Close()
	for iter.Valid() {
		key = iter.Key()
		value = iter.Value()
		err = iter.Next()
		require.NoError(s.t, err)
	}
	return key, value
}

// Storage returns the tikv storage
func (s *EventTestHelper) Storage() kv.Storage {
	return s.storage
}

// Tk returns the TestKit
func (s *EventTestHelper) Tk() *testkit.TestKit {
	return s.tk
}

// GetCurrentMeta return the current meta snapshot
func (s *EventTestHelper) GetCurrentMeta() *timeta.Meta {
	ver, err := s.storage.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(s.t, err)
	return timeta.NewSnapshotMeta(s.storage.GetSnapshot(ver))
}

// Close closes the helper
func (s *EventTestHelper) Close() {
	s.domain.Close()
	s.storage.Close() //nolint:errcheck
}

func toTableInfosKey(schema, table string) string {
	return schema + "." + table
}
