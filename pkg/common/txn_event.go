package common

import (
	"unsafe"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	timodel "github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// TODO: 想一想这个到底要哪些
type DDLEvent struct {
	Job *model.Job `json:"ddl_job"`
	// commitTS of the rawKV
	CommitTS Ts `json:"commit_ts"`
}

// TxnEvent represents all events in the current txn
// It could be a DDL event, or multiple DML events, but can't be both.
// TODO: field 改成小写？
type TxnEvent struct {
	// ClusterID is the ID of the tidb cluster this event belongs to.
	ClusterID uint64
	// Span of this event belongs to.
	Span *TableSpan

	DDLEvent       *DDLEvent
	Rows           []*RowChangedEvent
	StartTs        uint64
	CommitTs       uint64
	PostTxnFlushed func() // 用于在event flush 后执行，后续兼容不同下游的时候要看是不是要拆下去
}

func (e *TxnEvent) MemoryCost() int {
	// TODO: 目前只考虑 dml 情况
	size := int(unsafe.Sizeof(e.PostTxnFlushed))
	size += int(unsafe.Sizeof(e.StartTs)) + int(unsafe.Sizeof(e.CommitTs))
	for _, row := range e.Rows {
		size += row.ApproximateBytes()
	}
	return size
}

func (e *TxnEvent) IsDMLEvent() bool {
	return len(e.Rows) > 0
}

func (e *TxnEvent) IsDDLEvent() bool {
	return e.DDLEvent != nil
}

func (e *TxnEvent) IsCrossTableDDL() bool {
	ddlType := e.GetDDLType()
	return ddlType == model.ActionCreateSchema || ddlType == model.ActionDropSchema ||
		ddlType == model.ActionDropTable ||
		ddlType == model.ActionTruncateTable || ddlType == model.ActionRenameTable ||
		ddlType == model.ActionAddTablePartition || ddlType == model.ActionDropTablePartition ||
		ddlType == model.ActionTruncateTablePartition || ddlType == model.ActionRecoverTable ||
		ddlType == model.ActionRepairTable || ddlType == model.ActionExchangeTablePartition ||
		ddlType == model.ActionRemovePartitioning || ddlType == model.ActionRenameTables ||
		ddlType == model.ActionCreateTables || ddlType == model.ActionReorganizePartition ||
		ddlType == model.ActionFlashbackCluster || ddlType == model.ActionMultiSchemaChange
}

func (e *TxnEvent) IsSyncPointEvent() bool {
	// TODO
	return false
}

func (e *TxnEvent) GetDDLQuery() string {
	if e.DDLEvent == nil {
		return "" // 要报错的
	}
	return e.DDLEvent.Job.Query
}

func (e *TxnEvent) GetDDLSchemaName() string {
	if e.DDLEvent == nil {
		return "" // 要报错的
	}
	return e.DDLEvent.Job.SchemaName
}

func (e *TxnEvent) GetDDLType() model.ActionType {
	return e.DDLEvent.Job.Type
}

func (e *TxnEvent) GetRows() []*RowChangedEvent {
	return e.Rows
}

// // ColumnData represents a column value in row changed event
// type ColumnData struct {
// 	// ColumnID may be just a mock id, because we don't store it in redo log.
// 	// So after restore from redo log, we need to give every a column a mock id.
// 	// The only guarantee is that the column id is unique in a RowChangedEvent
// 	ColumnID int64
// 	Value    interface{}

// 	// ApproximateBytes is approximate bytes consumed by the column.
// 	ApproximateBytes int
// }

type RowChangedEvent struct {
	PhysicalTableID int64

	// NOTICE: We probably store the logical ID inside TableInfo's TableName,
	// not the physical ID.
	// For normal table, there is only one ID, which is the physical ID.
	// AKA TIDB_TABLE_ID.
	// For partitioned table, there are two kinds of ID:
	// 1. TIDB_PARTITION_ID is the physical ID of the partition.
	// 2. TIDB_TABLE_ID is the logical ID of the table.
	// In general, we always use the physical ID to represent a table, but we
	// record the logical ID from the DDL event(job.BinlogInfo.TableInfo).
	// So be careful when using the TableInfo.
	TableInfo *TableInfo

	Columns    []*Column
	PreColumns []*Column

	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs uint64
}

// GetTableID returns the table ID of the event.
func (r *RowChangedEvent) GetTableID() int64 {
	return r.PhysicalTableID
}

// Column represents a column value and its schema info
type Column struct {
	Name      string         `msg:"name"`
	Type      byte           `msg:"type"`
	Charset   string         `msg:"charset"`
	Collation string         `msg:"collation"`
	Flag      ColumnFlagType `msg:"-"`
	Value     interface{}    `msg:"-"`
	Default   interface{}    `msg:"-"`

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int `msg:"-"`
}

// GetColumns returns the columns of the event
func (r *RowChangedEvent) GetColumns() []*Column {
	return r.Columns
}

// GetPreColumns returns the pre columns of the event
func (r *RowChangedEvent) GetPreColumns() []*Column {
	return r.PreColumns
}

func (r *RowChangedEvent) ApproximateBytes() int {
	const sizeOfRowEvent = int(unsafe.Sizeof(*r))
	const sizeOfTable = int(unsafe.Sizeof(*r.TableInfo))
	const sizeOfInt = int(unsafe.Sizeof(int(0)))
	size := sizeOfRowEvent + sizeOfTable + 2*sizeOfInt

	// Size of cols
	for i := range r.Columns {
		size += r.Columns[i].ApproximateBytes
	}
	// Size of pre cols
	for i := range r.PreColumns {
		if r.PreColumns[i] != nil {
			size += r.PreColumns[i].ApproximateBytes
		}
	}
	return size
}

func ColumnDatas2Columns(cols []*timodel.ColumnData, tableInfo *TableInfo) []*Column {
	if cols == nil {
		return nil
	}
	columns := make([]*Column, len(cols))
	for i, colData := range cols {
		if colData == nil {
			log.Warn("meet nil column data, should not happened in production env",
				zap.Any("cols", cols),
				zap.Any("tableInfo", tableInfo))
			continue
		}
		columns[i] = columnData2Column(colData, tableInfo)
	}
	return columns
}

func columnData2Column(col *timodel.ColumnData, tableInfo *TableInfo) *Column {
	colID := col.ColumnID
	offset, ok := tableInfo.columnsOffset[colID]
	if !ok {
		log.Warn("invalid column id",
			zap.Int64("columnID", colID),
			zap.Any("tableInfo", tableInfo))
	}
	colInfo := tableInfo.Columns[offset]
	return &Column{
		Name:      colInfo.Name.O,
		Type:      colInfo.GetType(),
		Charset:   colInfo.GetCharset(),
		Collation: colInfo.GetCollate(),
		Flag:      *tableInfo.ColumnsFlag[colID],
		Value:     col.Value,
		Default:   GetColumnDefaultValue(colInfo),
	}
}
