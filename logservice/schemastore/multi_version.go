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
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

type tableInfoItem struct {
	version uint64
	info    *common.TableInfo
}

type versionedTableInfoStore struct {
	mu sync.Mutex

	tableID int64

	// ordered by ts
	infos []*tableInfoItem

	deleteVersion uint64

	initialized bool

	pendingDDLs []PersistedDDLEvent

	// used to indicate whether the table info build is ready
	// must wait on it before reading table info from store
	readyToRead chan struct{}
}

func newEmptyVersionedTableInfoStore(tableID int64) *versionedTableInfoStore {
	return &versionedTableInfoStore{
		tableID:       tableID,
		infos:         make([]*tableInfoItem, 0),
		deleteVersion: math.MaxUint64,
		initialized:   false,
		pendingDDLs:   make([]PersistedDDLEvent, 0),
		readyToRead:   make(chan struct{}),
	}
}

func (v *versionedTableInfoStore) addInitialTableInfo(info *common.TableInfo) {
	v.mu.Lock()
	defer v.mu.Unlock()
	// assertEmpty(v.infos)
	// log.Info("addInitialTableInfo", zap.Int64("tableID", int64(v.tableID)), zap.Uint64("version", info.Version))
	v.infos = append(v.infos, &tableInfoItem{version: uint64(info.Version), info: info})
}

func (v *versionedTableInfoStore) getTableID() int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.tableID
}

func (v *versionedTableInfoStore) setTableInfoInitialized() {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, job := range v.pendingDDLs {
		// log.Info("apply pending ddl",
		// 	zap.Int64("tableID", int64(v.tableID)),
		// 	zap.String("query", job.Query),
		// 	zap.Uint64("finishedTS", job.BinlogInfo.FinishedTS),
		// 	zap.Any("infosLen", len(v.infos)))
		v.doApplyDDL(&job)
	}
	v.initialized = true
	close(v.readyToRead)
}

func (v *versionedTableInfoStore) waitTableInfoInitialized() {
	<-v.readyToRead
}

// return the table info with the largest version <= ts
func (v *versionedTableInfoStore) getTableInfo(ts uint64) (*common.TableInfo, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.initialized {
		log.Panic("should wait for table info initialized")
	}

	if ts >= v.deleteVersion {
		log.Error("table info deleted",
			zap.Any("ts", ts),
			zap.Any("tableID", v.tableID),
			zap.Any("infos", v.infos),
			zap.Any("deleteVersion", v.deleteVersion))
		return nil, fmt.Errorf("table info deleted %d", v.tableID)
	}

	target := sort.Search(len(v.infos), func(i int) bool {
		return v.infos[i].version > ts
	})
	if target == 0 {
		log.Error("no version found",
			zap.Any("ts", ts),
			zap.Any("tableID", v.tableID),
			zap.Any("infos", v.infos),
			zap.Any("deleteVersion", v.deleteVersion))
		return nil, errors.New("no version found")
	}
	return v.infos[target-1].info, nil
}

// only keep one item with the largest version <= gcTS, return whether the store should be totally removed
func (v *versionedTableInfoStore) gc(gcTs uint64) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.initialized {
		return false
	}
	if len(v.infos) == 0 {
		log.Fatal("no table info found", zap.Int64("tableID", v.tableID))
	}

	if gcTs >= v.deleteVersion {
		return true
	}

	target := sort.Search(len(v.infos), func(i int) bool {
		return v.infos[i].version > gcTs
	})
	if target == 0 {
		return false
	}

	v.infos = v.infos[target-1:]
	if len(v.infos) == 0 {
		log.Panic("should not happen")
	}
	return false
}

func assertEmpty(infos []*tableInfoItem, event *PersistedDDLEvent) {
	if len(infos) != 0 {
		log.Panic("shouldn't happen",
			zap.Any("infosLen", len(infos)),
			zap.Any("lastVersion", infos[len(infos)-1].version),
			zap.Any("lastTableInfoVersion", infos[len(infos)-1].info.Version),
			zap.String("query", event.Query),
			zap.Int64("tableID", event.CurrentTableID),
			zap.Uint64("finishedTs", event.FinishedTs),
			zap.Int64("schemaVersion", event.SchemaVersion))
	}
}

func assertNonEmpty(infos []*tableInfoItem, event *PersistedDDLEvent) {
	if len(infos) == 0 {
		log.Panic("shouldn't happen",
			zap.Any("infos", infos),
			zap.String("query", event.Query))
	}
}

func assertNonDeleted(v *versionedTableInfoStore) {
	if v.deleteVersion != uint64(math.MaxUint64) {
		log.Panic("shouldn't happen")
	}
}

func (v *versionedTableInfoStore) applyDDLFromPersistStorage(event *PersistedDDLEvent) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.initialized {
		log.Panic("should not happen")
	}

	v.doApplyDDL(event)
}

func (v *versionedTableInfoStore) applyDDL(event *PersistedDDLEvent) {
	v.mu.Lock()
	defer v.mu.Unlock()
	// delete table should not receive more ddl
	assertNonDeleted(v)

	if !v.initialized {
		// The usage of the parameter `event` may outlive the function call, so we copy it.
		v.pendingDDLs = append(v.pendingDDLs, *event)
		return
	}
	v.doApplyDDL(event)
}

// lock must be hold by the caller
func (v *versionedTableInfoStore) doApplyDDL(event *PersistedDDLEvent) {
	// TODO: add a unit test
	// TODO: whether need add schema version check
	if len(v.infos) != 0 && event.FinishedTs <= v.infos[len(v.infos)-1].version {
		log.Warn("already applied ddl, ignore it.",
			zap.Int64("tableID", v.tableID),
			zap.String("query", event.Query),
			zap.Uint64("finishedTS", event.FinishedTs),
			zap.Int("infosLen", len(v.infos)))
		return
	}
	appendTableInfo := func() {
		info := common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, event.TableInfo)
		v.infos = append(v.infos, &tableInfoItem{version: uint64(event.FinishedTs), info: info})
	}

	switch model.ActionType(event.Type) {
	case model.ActionCreateTable:
		if len(v.infos) == 1 {
			// table info may be in snapshot, can not filter redudant job in this case
			log.Warn("ignore create table job",
				zap.Int64("tableID", int64(v.tableID)),
				zap.String("query", event.Query),
				zap.Uint64("finishedTS", event.FinishedTs))
			break
		}
		assertEmpty(v.infos, event)
		appendTableInfo()
	case model.ActionDropSchema:
		// ignore
	case model.ActionDropTable:
		v.deleteVersion = uint64(event.FinishedTs)
	case model.ActionAddColumn,
		model.ActionDropColumn:
		assertNonEmpty(v.infos, event)
		appendTableInfo()
	case model.ActionTruncateTable:
		if isPartitionTable(event.TableInfo) {
			createTable := false
			for _, partition := range getAllPartitionIDs(event.TableInfo) {
				if v.tableID == partition {
					createTable = true
					break
				}
			}
			if createTable {
				log.Info("create table for truncate table")
				appendTableInfo()
			} else {
				v.deleteVersion = uint64(event.FinishedTs)
			}
		} else {
			if v.tableID == event.CurrentTableID {
				appendTableInfo()
			} else {
				if v.tableID != event.PrevTableID {
					log.Panic("should not happen")
				}
				v.deleteVersion = uint64(event.FinishedTs)
			}
		}
	case model.ActionRenameTable:
		assertNonEmpty(v.infos, event)
		appendTableInfo()
	case model.ActionAddTablePartition:
		newCreatedIDs := getCreatedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
		for _, partition := range newCreatedIDs {
			if v.tableID == partition {
				appendTableInfo()
				break
			}
		}
	case model.ActionDropTablePartition:
		droppedIDs := getDroppedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
		for _, partition := range droppedIDs {
			if v.tableID == partition {
				v.deleteVersion = uint64(event.FinishedTs)
				break
			}
		}
	case model.ActionCreateView:
		// create view is add to all table's ddl history, so it will be read when build store, just ignore it
	case model.ActionTruncateTablePartition:
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		dropped := false
		for _, partition := range droppedIDs {
			if v.tableID == partition {
				v.deleteVersion = uint64(event.FinishedTs)
				dropped = true
				break
			}
		}
		if !dropped {
			newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
			for _, partition := range newCreatedIDs {
				if v.tableID == partition {
					appendTableInfo()
					break
				}
			}
		}
	case model.ActionExchangeTablePartition:
		assertNonEmpty(v.infos, event)
		columnSchema := v.infos[len(v.infos)-1].info.ShadowCopyColumnSchema()
		// the previous normal table
		if v.tableID == event.PrevTableID {
			tableInfo := common.NewTableInfo(
				event.CurrentSchemaID,
				event.CurrentSchemaName,
				pmodel.NewCIStr(event.CurrentTableName).O,
				v.infos[len(v.infos)-1].info.TableName.TableID,
				v.infos[len(v.infos)-1].info.TableName.IsPartition,
				v.infos[len(v.infos)-1].info.Version,
				columnSchema,
			)
			v.infos = append(v.infos, &tableInfoItem{version: uint64(event.FinishedTs), info: tableInfo})
		} else {
			tableInfo := common.NewTableInfo(
				event.CurrentSchemaID,
				event.CurrentSchemaName,
				pmodel.NewCIStr(event.PrevTableName).O,
				v.infos[len(v.infos)-1].info.TableName.TableID,
				v.infos[len(v.infos)-1].info.TableName.IsPartition,
				v.infos[len(v.infos)-1].info.Version,
				columnSchema,
			)
			v.infos = append(v.infos, &tableInfoItem{version: uint64(event.FinishedTs), info: tableInfo})
		}
	case model.ActionCreateTables:
		assertEmpty(v.infos, event)
		for _, tableInfo := range event.MultipleTableInfos {
			if isPartitionTable(tableInfo) {
				for _, partitionID := range getAllPartitionIDs(tableInfo) {
					if v.tableID == partitionID {
						info := common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, tableInfo)
						v.infos = append(v.infos, &tableInfoItem{version: uint64(event.FinishedTs), info: info})
						break
					}
				}
			} else {
				if v.tableID == tableInfo.ID {
					info := common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, tableInfo)
					v.infos = append(v.infos, &tableInfoItem{version: uint64(event.FinishedTs), info: info})
					break
				}
			}
		}
	case model.ActionReorganizePartition:
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		dropped := false
		for _, partition := range droppedIDs {
			if v.tableID == partition {
				v.deleteVersion = uint64(event.FinishedTs)
				dropped = true
				break
			}
		}
		if !dropped {
			newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
			for _, partition := range newCreatedIDs {
				if v.tableID == partition {
					appendTableInfo()
					break
				}
			}
		}
	default:
		log.Panic("not supported ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}
}
