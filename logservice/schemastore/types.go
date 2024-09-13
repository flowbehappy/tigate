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
	// SchemaID means different for different job types:
	// - ExchangeTablePartition: db id of non-partitioned table
	SchemaID int64 `json:"schema_id"`
	// TableID means different for different job types:
	// - ExchangeTablePartition: non-partitioned table id
	TableID       int64            `json:"table_id"`
	SchemaName    string           `json:"schema_name"`
	TableName     string           `json:"table_name"`
	Query         string           `json:"query"`
	SchemaVersion int64            `json:"schema_version"`
	DBInfo        *model.DBInfo    `json:"db_info"`
	TableInfo     *model.TableInfo `json:"table_info"`
	FinishedTs    uint64           `json:"finished_ts"`
	BDRRole       string           `json:"bdr_role"`
	// CDCWriteSource indicates the source of CDC write.
	CDCWriteSource uint64 `json:"cdc_write_source"`

	// TODO: add pre table name
}

func buildPersistedDDLEvent(job *model.Job) PersistedDDLEvent {
	return PersistedDDLEvent{
		ID:       job.ID,
		Type:     byte(job.Type),
		SchemaID: job.SchemaID,
		TableID:  job.TableID,
		// TODO: fill in schema name and table name
		SchemaName:     job.SchemaName,
		TableName:      job.TableName,
		Query:          job.Query,
		SchemaVersion:  job.BinlogInfo.SchemaVersion,
		DBInfo:         job.BinlogInfo.DBInfo,
		TableInfo:      job.BinlogInfo.TableInfo,
		FinishedTs:     job.BinlogInfo.FinishedTS,
		BDRRole:        job.BDRRole,
		CDCWriteSource: job.CDCWriteSource,
	}
}

//msgp:ignore DatabaseInfo
type DatabaseInfo struct {
	Name          string
	Tables        map[int64]bool
	CreateVersion uint64
	DeleteVersion uint64
}

func (d *DatabaseInfo) needGc(gcTs uint64) bool { return d.DeleteVersion < gcTs }

//msgp:ignore SchemaIDWithVersion
type SchemaIDWithVersion struct {
	SchemaID      int64
	CreateVersion uint64
}

//msgp:ignore TableNameWithVersion
type TableNameWithVersion struct {
	Name          string
	CreateVersion uint64
}

//msgp:ignore VersionedTableBasicInfo
type VersionedTableBasicInfo struct {
	SchemaIDs     []SchemaIDWithVersion
	Names         []TableNameWithVersion
	CreateVersion uint64
}
