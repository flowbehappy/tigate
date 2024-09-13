package schemastore

import (
	"github.com/flowbehappy/tigate/pkg/common"
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
	Query         string           `json:"query"`
	SchemaVersion int64            `json:"schema_version"`
	DBInfo        *model.DBInfo    `json:"-"`
	TableInfo     *model.TableInfo `json:"table_info"`
	FinishedTs    uint64           `json:"finished_ts"`
	BDRRole       string           `json:"bdr_role"`
	// CDCWriteSource indicates the source of CDC write.
	CDCWriteSource uint64 `json:"cdc_write_source"`

	// only used for drop schema
	TablesInSchema map[int64]bool `json:"-"`

	BlockedTables     *common.InfluencedTables `json:"blocked_tables"`
	NeedDroppedTables *common.InfluencedTables `json:"need_dropped_tables"`
	NeedAddedTables   []common.Table           `json:"need_added_tables"`

	TableNameChange *common.TableNameChange `json:"table_name_change"`
}

func buildPersistedDDLEvent(job *model.Job) PersistedDDLEvent {
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
