package schemastore

import (
	"github.com/pingcap/tidb/pkg/parser/model"
)

//go:generate msgp

// TODO: use msgp.Raw to do version management
type PersistedDDLEvent struct {
	ID   int64 `msg:"id"`
	Type byte  `msg:"type"`

	// TODO: add more detailed comments about following fields
	// SchemaID means different for different job types:
	// - ExchangeTablePartition: db id of non-partitioned table
	SchemaID int64 `msg:"schema_id"`
	// TableID means different for different job types:
	// - ExchangeTablePartition: non-partitioned table id
	// For truncate table, it it the table id of the newly created table
	TableID    int64  `msg:"table_id"`
	SchemaName string `msg:"schema_name"`
	TableName  string `msg:"table_name"`

	PrevSchemaID   int64  `msg:"prev_schema_id"`
	PrevTableID    int64  `msg:"prev_table_id"`
	PrevSchemaName string `msg:"prev_schema_name"`
	PrevTableName  string `msg:"prev_table_name"`

	Query         string           `msg:"query"`
	SchemaVersion int64            `msg:"schema_version"`
	DBInfo        *model.DBInfo    `msg:"-"`
	TableInfo     *model.TableInfo `msg:"-"`
	// TODO: use a custom struct to store the table info?
	TableInfoValue []byte `msg:"table_info_value"`
	FinishedTs     uint64 `msg:"finished_ts"`
	// TODO: do we need the following two fields?
	BDRRole        string `msg:"bdr_role"`
	CDCWriteSource uint64 `msg:"cdc_write_source"`
}

func buildPersistedDDLEventFromJob(job *model.Job) PersistedDDLEvent {
	return PersistedDDLEvent{
		ID:             job.ID,
		Type:           byte(job.Type),
		SchemaID:       job.SchemaID,
		TableID:        job.TableID,
		Query:          job.Query,
		SchemaVersion:  job.BinlogInfo.SchemaVersion,
		DBInfo:         job.BinlogInfo.DBInfo,
		TableInfo:      job.BinlogInfo.TableInfo,
		FinishedTs:     job.BinlogInfo.FinishedTS,
		BDRRole:        job.BDRRole,
		CDCWriteSource: job.CDCWriteSource,
	}
}

// TODO: use msgp.Raw to do version management
type PersistedTableInfoEntry struct {
	SchemaID       int64  `msg:"schema_id"`
	SchemaName     string `msg:"schema_name"`
	TableInfoValue []byte `msg:"table_info_value"`
}

type UpperBoundMeta struct {
	FinishedDDLTs uint64 `msg:"finished_ddl_ts"`
	SchemaVersion int64  `msg:"schema_version"`
	ResolvedTs    uint64 `msg:"resolved_ts"`
}

//msgp:ignore BasicDatabaseInfo
type BasicDatabaseInfo struct {
	Name   string
	Tables map[int64]bool
}

//msgp:ignore BasicTableInfo
type BasicTableInfo struct {
	SchemaID int64
	Name     string
	InKVSnap bool
}

//msgp:ignore DDLJobWithCommitTs
type DDLJobWithCommitTs struct {
	Job *model.Job
	// the commitTs of the rawKVEntry which contains the DDL job
	CommitTs uint64
}
