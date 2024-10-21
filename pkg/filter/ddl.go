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

package filter

import (
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
)

//TODO: clean this file

// ddlWhiteListMap is a map of all DDL types that can be applied to cdc's schema storage.
var ddlWhiteListMap = map[timodel.ActionType]bf.EventType{
	// schema related DDLs
	timodel.ActionCreateSchema:                  bf.CreateDatabase,
	timodel.ActionDropSchema:                    bf.DropDatabase,
	timodel.ActionModifySchemaCharsetAndCollate: bf.ModifySchemaCharsetAndCollate,

	// table related DDLs
	timodel.ActionCreateTable:                  bf.CreateTable,
	timodel.ActionCreateTables:                 bf.CreateTable,
	timodel.ActionDropTable:                    bf.DropTable,
	timodel.ActionTruncateTable:                bf.TruncateTable,
	timodel.ActionRenameTable:                  bf.RenameTable,
	timodel.ActionRenameTables:                 bf.RenameTable,
	timodel.ActionRecoverTable:                 bf.RecoverTable,
	timodel.ActionModifyTableComment:           bf.ModifyTableComment,
	timodel.ActionModifyTableCharsetAndCollate: bf.ModifyTableCharsetAndCollate,

	// view related DDLs
	timodel.ActionCreateView: bf.CreateView,
	timodel.ActionDropView:   bf.DropView,

	// partition related DDLs
	timodel.ActionAddTablePartition:      bf.AddTablePartition,
	timodel.ActionDropTablePartition:     bf.DropTablePartition,
	timodel.ActionTruncateTablePartition: bf.TruncateTablePartition,
	timodel.ActionExchangeTablePartition: bf.ExchangePartition,
	timodel.ActionReorganizePartition:    bf.ReorganizePartition,
	timodel.ActionAlterTablePartitioning: bf.AlterTablePartitioning,
	timodel.ActionRemovePartitioning:     bf.RemovePartitioning,

	// column related DDLs
	timodel.ActionAddColumn:       bf.AddColumn,
	timodel.ActionDropColumn:      bf.DropColumn,
	timodel.ActionModifyColumn:    bf.ModifyColumn,
	timodel.ActionSetDefaultValue: bf.SetDefaultValue,

	// index related DDLs
	timodel.ActionRebaseAutoID:         bf.RebaseAutoID,
	timodel.ActionAddPrimaryKey:        bf.AddPrimaryKey,
	timodel.ActionDropPrimaryKey:       bf.DropPrimaryKey,
	timodel.ActionAddIndex:             bf.CreateIndex,
	timodel.ActionDropIndex:            bf.DropIndex,
	timodel.ActionRenameIndex:          bf.RenameIndex,
	timodel.ActionAlterIndexVisibility: bf.AlterIndexVisibility,

	// TTL related DDLs
	timodel.ActionAlterTTLInfo:   bf.AlterTTLInfo,
	timodel.ActionAlterTTLRemove: bf.AlterTTLRemove,

	// difficult to classify DDLs
	timodel.ActionMultiSchemaChange: bf.MultiSchemaChange,

	// deprecated DDLs,see https://github.com/pingcap/tidb/pull/35862.
	// DDL types below are deprecated in TiDB v6.2.0, but we still keep them here
	// In case that some users will use TiCDC to replicate data from TiDB v6.1.x.
	timodel.ActionAddColumns:  bf.AddColumn,
	timodel.ActionDropColumns: bf.DropColumn,
}

// singleTableDDLs should only affect one table.
var singleTableDDLs = map[timodel.ActionType]struct{}{
	// table related DDLs
	timodel.ActionTruncateTable:                {},
	timodel.ActionRenameTable:                  {},
	timodel.ActionModifyTableComment:           {},
	timodel.ActionModifyTableCharsetAndCollate: {},

	// view related DDLs
	timodel.ActionCreateView: {},
	timodel.ActionDropView:   {},

	// column related DDLs
	timodel.ActionAddColumn:       {},
	timodel.ActionDropColumn:      {},
	timodel.ActionModifyColumn:    {},
	timodel.ActionSetDefaultValue: {},

	// index related DDLs
	timodel.ActionRebaseAutoID:         {},
	timodel.ActionAddPrimaryKey:        {},
	timodel.ActionDropPrimaryKey:       {},
	timodel.ActionAddIndex:             {},
	timodel.ActionDropIndex:            {},
	timodel.ActionRenameIndex:          {},
	timodel.ActionAlterIndexVisibility: {},

	// TTL related DDLs
	timodel.ActionAlterTTLInfo:   {},
	timodel.ActionAlterTTLRemove: {},

	timodel.ActionAddColumns:        {},
	timodel.ActionDropColumns:       {},
	timodel.ActionMultiSchemaChange: {},

	// Not supported yet
	// timodel.ActionShardRowID,
	// timodel.ActionAddForeignKey, timodel.ActionDropForeignKey,
	// timodel.ActionLockTable, timodel.ActionUnlockTable,
	// timodel.ActionSetTiFlashReplica,
	// timodel.ActionModifyTableAutoIdCache, timodel.ActionRebaseAutoRandomBase,
	// timodel.ActionAddCheckConstraint, timodel.ActionDropCheckConstraint, timodel.ActionAlterCheckConstraint,
	// timodel.ActionDropIndexes,
	// timodel.ActionAlterTableAttributes,
	// timodel.ActionAlterCacheTable, timodel.ActionAlterNoCacheTable,
	// timodel.ActionRepairTable,
	// timodel.ActionCreatePlacementPolicy, timodel.ActionAlterPlacementPolicy,
	// timodel.ActionDropPlacementPolicy,
	// timodel.ActionRecoverSchema,
}

// multiTableDDLs affect multiple tables.
var multiTableDDLs = map[timodel.ActionType]struct{}{
	timodel.ActionRenameTables: {},
	// partition related DDLs
	timodel.ActionAddTablePartition:      {},
	timodel.ActionDropTablePartition:     {},
	timodel.ActionTruncateTablePartition: {},
	timodel.ActionExchangeTablePartition: {},
	timodel.ActionReorganizePartition:    {},
	timodel.ActionAlterTablePartitioning: {},
	timodel.ActionRemovePartitioning:     {},
}

// globalTableDDLs must be sent to table trigger dispatcher.
var globalTableDDLs = map[timodel.ActionType]struct{}{
	timodel.ActionCreateSchema:                  {},
	timodel.ActionDropSchema:                    {},
	timodel.ActionModifySchemaCharsetAndCollate: {},
	timodel.ActionCreateTable:                   {},
	timodel.ActionDropTable:                     {},
	timodel.ActionCreateTables:                  {},
	timodel.ActionRecoverTable:                  {},
}

func ShouldBlock(action timodel.ActionType) bool {
	if _, ok := singleTableDDLs[action]; ok {
		return false
	}
	if _, ok := multiTableDDLs[action]; ok {
		return true
	}
	switch action {
	case timodel.ActionCreateSchema, timodel.ActionCreateTables,
		timodel.ActionCreateTable, timodel.ActionRecoverTable:
		// not block since there are no affected dispatchers.
		return false
	default:
		return true
	}
}
