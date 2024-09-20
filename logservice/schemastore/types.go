package schemastore

import (
	"github.com/pingcap/tidb/pkg/parser/model"
)

//go:generate msgp

// TODO: use msgp
// TODO: use msgp.Raw to do version management
type PersistedDDLEvent struct {
	ID   int64 `json:"id"`
	Type byte  `json:"type"`

	// TODO: add more detailed comments about following fields
	// SchemaID means different for different job types:
	// - ExchangeTablePartition: db id of non-partitioned table
	SchemaID int64 `json:"schema_id"`
	// TableID means different for different job types:
	// - ExchangeTablePartition: non-partitioned table id
	// For truncate table, it it the table id of the newly created table
	TableID    int64  `json:"table_id"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`

	PrevSchemaID   int64  `json:"prev_schema_id"`
	PrevTableID    int64  `json:"prev_table_id"`
	PrevSchemaName string `json:"prev_schema_name"`
	PrevTableName  string `json:"prev_table_name"`

	Query         string           `json:"query"`
	SchemaVersion int64            `json:"schema_version"`
	DBInfo        *model.DBInfo    `json:"-"`
	TableInfo     *model.TableInfo `json:"table_info"`
	FinishedTs    uint64           `json:"finished_ts"`
	BDRRole       string           `json:"bdr_role"`
	// CDCWriteSource indicates the source of CDC write.
	CDCWriteSource uint64 `json:"cdc_write_source"`
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

// msgp:ignore DDLJobWithCommitTs
type DDLJobWithCommitTs struct {
	Job *model.Job
	// the commitTs of the rawKVEntry which contains the DDL job
	CommitTs uint64
}
