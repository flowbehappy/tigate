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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// Data format:
//  1. snapshot data
//     {prefix11}{snapshot_ts}{table_id} -> table info and schema id
//     {prefix12}{snapshot_ts}{schema_id} -> database info
//  2. ddl jobs
//     {prefix21}{finished_ddl_ts} -> ddl job
//     NOTE: {finished_ddl_ts} must be unique
//  3. metadata
//     {key31} -> {snapshot_ts}
//     {key32} -> {max_finished_ddl_ts}{schema_version}{resolved_ts}
//     the valid data keys on disk includes snapshot data at `snapshot_ts`
//     and ddl jobs in the range (snapshot_ts, max_finished_ddl_ts]
//     and we will pull ddl job from `resolved_ts` at restart if the current gc ts is smaller than resolved_ts.

const snapshotSchemaKeyPrefix = "ss_"
const snapshotTableKeyPrefix = "st_"

const ddlKeyPrefix = "ds_"

func gcTsKey() []byte {
	return []byte("gc")
}

func upperBoundKey() []byte {
	return []byte("up")
}

func schemaInfoKey(ts common.Ts, schemaID int64) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(snapshotSchemaKeyPrefix)
	if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, schemaID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func tableInfoKey(ts common.Ts, tableID common.TableID) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(snapshotTableKeyPrefix)
	if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, tableID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ddlJobKey(ts common.Ts) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(ddlKeyPrefix)
	if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readGcTs(db *pebble.DB) (uint64, error) {
	snap := db.NewSnapshot()
	defer snap.Close()
	value, closer, err := snap.Get(gcTsKey())
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	var gcTs uint64
	buf := bytes.NewBuffer(value)
	if err := binary.Read(buf, binary.BigEndian, &gcTs); err != nil {
		log.Fatal("read gc ts failed", zap.Error(err))
	}
	return gcTs, nil
}

func writeGcTs(db *pebble.DB, gcTs common.Ts) {
	batch := db.NewBatch()
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, uint64(gcTs))
	if err != nil {
		log.Fatal("marshal gc ts failed")
	}
	batch.Set(gcTsKey(), buf.Bytes(), pebble.NoSync)
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Fatal("write gc ts failed")
	}
}

func readUpperBoundMeta(db *pebble.DB) (upperBoundMeta, error) {
	snap := db.NewSnapshot()
	defer snap.Close()
	value, closer, err := snap.Get(upperBoundKey())
	if err != nil {
		return upperBoundMeta{}, err
	}
	defer closer.Close()
	var meta upperBoundMeta
	err = json.Unmarshal(value, &meta)
	if err != nil {
		log.Fatal("unmarshal upper bound meta failed", zap.Error(err))
	}
	return meta, nil
}

func writeUpperBoundMeta(db *pebble.DB, upperBound upperBoundMeta) {
	batch := db.NewBatch()
	value, err := json.Marshal(upperBound)
	if err != nil {
		log.Fatal("marshal upper bound failed")
	}
	batch.Set(upperBoundKey(), value, pebble.NoSync)
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Fatal("write upper bound failed")
	}
}

func loadTablesInKVSnap(snap *pebble.Snapshot, snapVersion common.Ts) (map[common.TableID]bool, error) {
	tablesInKVSnap := make(map[common.TableID]bool)

	startKey, err := tableInfoKey(snapVersion, 0)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	endKey, err := tableInfoKey(snapVersion, math.MaxInt64)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		var table_info_entry TableInfoEntry
		if err := json.Unmarshal(snapIter.Value(), &table_info_entry); err != nil {
			log.Fatal("unmarshal table info entry failed", zap.Error(err))
		}

		tbNameInfo := model.TableNameInfo{}
		if err := json.Unmarshal(table_info_entry.TableInfo, &tbNameInfo); err != nil {
			log.Fatal("unmarshal table name info failed", zap.Error(err))
		}
		tablesInKVSnap[tbNameInfo.ID] = true
	}

	return tablesInKVSnap, nil
}

// load the database info and ddl history in the range (snapVersion, upperBound]
func loadDatabaseInfoAndDDLHistory(
	snap *pebble.Snapshot,
	snapVersion common.Ts,
	upperBound upperBoundMeta,
) (map[common.SchemaID]*DatabaseInfo, map[common.TableID][]uint64, []uint64, error) {
	// load database info at `snapVersion`
	databaseMap := loadDatabaseInfoFromSnap(snap, snapVersion)

	tablesDDLHistory := make(map[common.TableID][]uint64)
	tableTriggerDDLHistory := make([]uint64, 0)

	// apply ddl jobs
	startKey, err := ddlJobKey(snapVersion)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	endKey, err := ddlJobKey(upperBound.FinishedDDLTs)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		var ddlEvent DDLEvent
		err = json.Unmarshal(snapIter.Value(), &ddlEvent)
		if err != nil {
			log.Fatal("unmarshal ddl job failed", zap.Error(err))
		}
		updateDatabaseInfo(ddlEvent.Job, databaseMap)
		updateDDLHistory(ddlEvent.Job, tablesDDLHistory, tableTriggerDDLHistory)
	}

	return databaseMap, tablesDDLHistory, tableTriggerDDLHistory, nil
}

func loadDatabaseInfoFromSnap(snap *pebble.Snapshot, snapVersion common.Ts) map[common.SchemaID]*DatabaseInfo {
	databaseMap := make(map[common.SchemaID]*DatabaseInfo)

	startKey, err := schemaInfoKey(snapVersion, 0)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	endKey, err := schemaInfoKey(snapVersion, math.MaxInt64)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		var dbInfo model.DBInfo
		if err := json.Unmarshal(snapIter.Value(), &dbInfo); err != nil {
			log.Fatal("unmarshal db info failed", zap.Error(err))
		}

		databaseInfo := &DatabaseInfo{
			Name:          dbInfo.Name.O,
			Tables:        make([]common.TableID, 0),
			CreateVersion: snapVersion,
			DeleteVersion: common.Ts(math.MaxUint64),
		}
		databaseMap[common.SchemaID(dbInfo.ID)] = databaseInfo
	}

	return databaseMap
}

func readSchemaIDAndTableInfoFromKVSnap(snap *pebble.Snapshot, tableID common.TableID, version common.Ts) (common.SchemaID, *model.TableInfo) {
	targetKey, err := tableInfoKey(version, tableID)
	if err != nil {
		log.Fatal("generate table info failed", zap.Error(err))
	}
	value, closer, err := snap.Get(targetKey)
	if err != nil {
		log.Fatal("get table info failed", zap.Error(err))
	}
	defer closer.Close()

	var table_info_entry TableInfoEntry
	err = json.Unmarshal(value, &table_info_entry)
	if err != nil {
		log.Fatal("unmarshal table info entry failed", zap.Error(err))
	}

	if table_info_entry.Version != 1 {
		log.Panic("unknown table info entry version")
	}

	tableInfo := &model.TableInfo{}
	err = json.Unmarshal(table_info_entry.TableInfo, tableInfo)
	if err != nil {
		log.Fatal("unmarshal table info failed", zap.Error(err))
	}
	return common.SchemaID(table_info_entry.SchemaID), tableInfo
}

func readDDLEvent(snap *pebble.Snapshot, version common.Ts) DDLEvent {
	ddlKey, err := ddlJobKey(version)
	if err != nil {
		log.Fatal("generate ddl job key failed", zap.Error(err))
	}
	ddlValue, closer, err := snap.Get(ddlKey)
	if err != nil {
		log.Fatal("get ddl job failed", zap.Error(err))
	}
	defer closer.Close()
	var ddlEvent DDLEvent
	err = json.Unmarshal(ddlValue, &ddlEvent)
	if err != nil {
		log.Fatal("unmarshal ddl job failed", zap.Error(err))
	}
	return ddlEvent
}

func writeDDLEvents(db *pebble.DB, ddlEvents ...DDLEvent) error {
	batch := db.NewBatch()
	for _, event := range ddlEvents {
		ddlKey, err := ddlJobKey(common.Ts(event.Job.BinlogInfo.FinishedTS))
		if err != nil {
			return err
		}
		ddlValue, err := json.Marshal(event)
		if err != nil {
			return err
		}
		batch.Set(ddlKey, ddlValue, pebble.NoSync)
	}
	return batch.Commit(pebble.NoSync)
}

const mTablePrefix = "Table"

func isTableRawKey(key []byte) bool {
	return strings.HasPrefix(string(key), mTablePrefix)
}

func writeSchemaInfoToBatch(batch *pebble.Batch, ts common.Ts, info *model.DBInfo) {
	schemaKey, err := schemaInfoKey(ts, int64(info.ID))
	if err != nil {
		log.Fatal("generate schema key failed", zap.Error(err))
	}
	schemaValue, err := json.Marshal(info)
	if err != nil {
		log.Fatal("marshal schema info failed", zap.Error(err))
	}
	batch.Set(schemaKey, schemaValue, pebble.NoSync)
}

func writeTableInfoToBatch(batch *pebble.Batch, ts common.Ts, schemaID int64, tableInfoValue []byte) common.TableID {
	tbNameInfo := model.TableNameInfo{}
	if err := json.Unmarshal(tableInfoValue, &tbNameInfo); err != nil {
		log.Fatal("unmarshal table info failed", zap.Error(err))
	}
	tableKey, err := tableInfoKey(ts, common.TableID(tbNameInfo.ID))
	if err != nil {
		log.Fatal("generate table key failed", zap.Error(err))
	}
	tableInfoEntry := TableInfoEntry{
		Version:   1,
		SchemaID:  schemaID,
		TableInfo: tableInfoValue,
	}
	tableInfoEntryValue, err := json.Marshal(tableInfoEntry)
	if err != nil {
		log.Fatal("marshal table info entry failed", zap.Error(err))
	}
	batch.Set(tableKey, tableInfoEntryValue, pebble.NoSync)
	return tbNameInfo.ID
}

func writeSchemaSnapshotAndMeta(
	db *pebble.DB,
	tiStore kv.Storage,
	snapTs common.Ts,
) (map[common.SchemaID]*DatabaseInfo, map[common.TableID]bool, upperBoundMeta, error) {
	meta := logpuller.GetSnapshotMeta(tiStore, uint64(snapTs))
	start := time.Now()
	dbInfos, err := meta.ListDatabases()
	if err != nil {
		log.Fatal("list databases failed", zap.Error(err))
	}

	databaseMap := make(map[common.SchemaID]*DatabaseInfo, len(dbInfos))
	tablesInKVSnap := make(map[common.TableID]bool)
	for _, dbInfo := range dbInfos {
		if filter.IsSysSchema(dbInfo.Name.O) {
			continue
		}
		databaseInfo := &DatabaseInfo{
			Name:          dbInfo.Name.O,
			Tables:        make([]common.TableID, 0),
			CreateVersion: snapTs,
			DeleteVersion: common.Ts(math.MaxUint64),
		}
		databaseMap[common.SchemaID(dbInfo.ID)] = databaseInfo

		batch := db.NewBatch()
		defer batch.Close()

		writeSchemaInfoToBatch(batch, snapTs, dbInfo)

		rawTables, err := meta.GetMetasByDBID(dbInfo.ID)
		if err != nil {
			log.Fatal("get tables failed", zap.Error(err))
		}
		for _, rawTable := range rawTables {
			if !isTableRawKey(rawTable.Field) {
				continue
			}
			tableID := writeTableInfoToBatch(batch, snapTs, dbInfo.ID, rawTable.Value)
			databaseInfo.Tables = append(databaseInfo.Tables, tableID)
			tablesInKVSnap[tableID] = true
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return nil, nil, upperBoundMeta{}, err
		}
	}

	writeGcTs(db, snapTs)
	upperBound := upperBoundMeta{
		FinishedDDLTs: 0,
		SchemaVersion: 0,
		ResolvedTs:    snapTs,
	}
	writeUpperBoundMeta(db, upperBound)

	log.Info("finish write schema snapshot",
		zap.Any("duration", time.Since(start).Seconds()))
	return databaseMap, tablesInKVSnap, upperBound, nil
}
