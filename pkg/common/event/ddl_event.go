package event

import (
	"encoding/json"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

type DDLEvent struct {
	DispatcherID common.DispatcherID `json:"dispatcher_id"`
	Type         byte                `json:"type"`
	// SchemaID means different for different job types:
	// - ExchangeTablePartition: db id of non-partitioned table
	SchemaID int64 `json:"schema_id"`
	// TableID means different for different job types:
	// - ExchangeTablePartition: non-partitioned table id
	TableID    int64            `json:"table_id"`
	SchemaName string           `json:"schema_name"`
	TableName  string           `json:"table_name"`
	Query      string           `json:"query"`
	TableInfo  *model.TableInfo `json:"table_info"`
	FinishedTs uint64           `json:"finished_ts"`
	// The seq of the event. It is set by event service.
	Seq uint64 `json:"seq"`
	// TODO: just here for compile, may be changed later
	MultipleTableInfos []*common.TableInfo `json:"multiple_table_infos"`

	BlockedTables     *InfluencedTables `json:"blocked_tables"`
	UpdatedSchemas    []SchemaIDChange  `json:"updated_schemas"`
	NeedDroppedTables *InfluencedTables `json:"need_dropped_tables"`
	NeedAddedTables   []Table           `json:"need_added_tables"`

	// DDLs which may change table name:
	//   Create Table
	//   Create Tables
	//   Drop Table
	//   Rename Table
	//   Rename Tables
	//   Drop Schema
	//   Recover Table
	TableNameChange *TableNameChange `json:"table_name_change"`

	TiDBOnly bool `json:"tidb_only"`
	// 用于在event flush 后执行，后续兼容不同下游的时候要看是不是要拆下去
	PostTxnFlushed []func() `msg:"-"`
}

func (d *DDLEvent) GetType() int {
	return TypeDDLEvent
}

func (d *DDLEvent) GetDispatcherID() common.DispatcherID {
	return d.DispatcherID
}

func (d *DDLEvent) GetStartTs() common.Ts {
	return 0
}

func (d *DDLEvent) GetCommitTs() common.Ts {
	return d.FinishedTs
}

func (d *DDLEvent) PostFlush() {
	for _, f := range d.PostTxnFlushed {
		f()
	}
}

func (d *DDLEvent) AddPostFlushFunc(f func()) {
	d.PostTxnFlushed = append(d.PostTxnFlushed, f)
}

func (d *DDLEvent) PushFrontFlushFunc(f func()) {
	d.PostTxnFlushed = append([]func(){f}, d.PostTxnFlushed...)
}

func (e *DDLEvent) GetBlockedTables() *InfluencedTables {
	return e.BlockedTables
}

func (e *DDLEvent) GetNeedDroppedTables() *InfluencedTables {
	return e.NeedDroppedTables
}

func (e *DDLEvent) GetNeedAddedTables() []Table {
	return e.NeedAddedTables
}

func (e *DDLEvent) GetUpdatedSchemas() []SchemaIDChange {
	return e.UpdatedSchemas
}

func (e *DDLEvent) GetDDLQuery() string {
	if e == nil {
		log.Error("DDLEvent is nil, should not happened in production env", zap.Any("event", e))
		return ""
	}
	return e.Query
}

func (e *DDLEvent) GetDDLSchemaName() string {
	if e == nil {
		return "" // 要报错的
	}
	return e.SchemaName
}

func (e *DDLEvent) GetDDLType() model.ActionType {
	return model.ActionType(e.Type)
}

func (t DDLEvent) Marshal() ([]byte, error) {
	// TODO: optimize it
	return json.Marshal(t)
}

func (t *DDLEvent) Unmarshal(data []byte) error {
	return json.Unmarshal(data, t)
}

// TODO: fix it
func (t *DDLEvent) GetSize() int64 {
	return 0
}

type SchemaTableName struct {
	SchemaName string
	TableName  string
}

// TableChange will record each ddl change of the table name.
// Each TableChange is related to a ddl event
type TableNameChange struct {
	AddName          []SchemaTableName
	DropName         []SchemaTableName
	DropDatabaseName string
}
