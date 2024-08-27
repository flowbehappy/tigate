package common

import (
	"fmt"
	"unsafe"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	timodel "github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

//go:generate msgp

// TODO: 想一想这个到底要哪些
//
//msgp:ignore DDLEvent
type DDLEvent struct {
	Job *model.Job `json:"ddl_job"`
	// commitTS of the rawKV
	CommitTS Ts `json:"commit_ts"`

	// Just for test now
	BlockedTableSpan     []*heartbeatpb.TableSpan `json:"blocked_table_span"`
	NeedDroppedTableSpan []*heartbeatpb.TableSpan `json:"need_dropped_table_span"`
	NeedAddedTableSpan   []*heartbeatpb.TableSpan `json:"need_added_table_span"`
}

// TxnEvent represents all events in the current txn
// It could be a DDL event, or multiple DML events, but can't be both.
// TODO: field 改成小写？
type TxnEvent struct {
	// ClusterID is the ID of the tidb cluster this event belongs to.
	ClusterID uint64 `msg:"cluster-id"`

	// TODO: fix
	DispatcherID DispatcherID `msg:"dispatcher-id"`

	// Span of this event belongs to.
	Span *TableSpan `msg:"-"`

	DDLEvent   *DDLEvent          `msg:"-"` // FIXME
	Rows       []*RowChangedEvent `msg:"rows"`
	ResolvedTs uint64             `msg:"resolved-ts"`
	StartTs    uint64             `msg:"start-ts"`
	CommitTs   uint64             `msg:"commit-ts"`

	// 用于在event flush 后执行，后续兼容不同下游的时候要看是不是要拆下去
	PostTxnFlushed []func() `msg:"-"`
}

func (w TxnEvent) GetType() int {
	return TypeTxnEvent
}

func (w *TxnEvent) GetDispatcherID() DispatcherID {
	return w.DispatcherID
}

func (w *TxnEvent) GetDDLEvent() *DDLEvent {
	return w.DDLEvent
}

func (w *TxnEvent) String() string {
	return fmt.Sprintf("TxnEvent{StartTs: %d, CommitTs: %d, Rows: %d, DDLEvent: %v}", w.StartTs, w.CommitTs, len(w.Rows), w.DDLEvent)
}

func (w *TxnEvent) Marshal() ([]byte, error) {
	return w.MarshalMsg(nil)
}

func (w *TxnEvent) Unmarshal(data []byte) error {
	_, err := w.UnmarshalMsg(data)
	return err
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

func (e *TxnEvent) IsSingleTableDDL() bool {
	ddlType := e.GetDDLType()
	return ddlType == model.ActionAddColumn || ddlType == model.ActionDropColumn || ddlType == model.ActionModifyColumn || ddlType == model.ActionAddIndex || ddlType == model.ActionDropIndex || ddlType == model.ActionModifyTableComment || ddlType == model.ActionRebaseAutoID || ddlType == model.ActionSetDefaultValue || ddlType == model.ActionShardRowID || ddlType == model.ActionModifyTableCharsetAndCollate || ddlType == model.ActionCreateView || ddlType == model.ActionDropView || ddlType == model.ActionAddForeignKey || ddlType == model.ActionDropForeignKey || ddlType == model.ActionRenameIndex || ddlType == model.ActionLockTable || ddlType == model.ActionUnlockTable || ddlType == model.ActionSetTiFlashReplica || ddlType == model.ActionAddPrimaryKey || ddlType == model.ActionDropPrimaryKey || ddlType == model.ActionAddColumns || ddlType == model.ActionDropColumns || ddlType == model.ActionModifyTableAutoIdCache || ddlType == model.ActionRebaseAutoRandomBase || ddlType == model.ActionAlterIndexVisibility || ddlType == model.ActionAddCheckConstraint || ddlType == model.ActionDropCheckConstraint || ddlType == model.ActionAlterCheckConstraint || ddlType == model.ActionDropIndexes || ddlType == model.ActionAlterTableAttributes || ddlType == model.ActionAlterCacheTable || ddlType == model.ActionAlterNoCacheTable || ddlType == model.ActionMultiSchemaChange || ddlType == model.ActionAlterTTLInfo || ddlType == model.ActionAlterTTLRemove || ddlType == model.ActionRepairTable || ddlType == model.ActionFlashbackCluster || ddlType == model.ActionCreatePlacementPolicy || ddlType == model.ActionAlterPlacementPolicy || ddlType == model.ActionDropPlacementPolicy || ddlType == model.ActionCreateResourceGroup || ddlType == model.ActionAlterResourceGroup || ddlType == model.ActionDropResourceGroup || ddlType == model.ActionCreateSchema
}

func (e *TxnEvent) GetBlockedTableSpan() []*heartbeatpb.TableSpan {
	return e.DDLEvent.BlockedTableSpan
}

func (e *TxnEvent) GetNeedDroppedTableSpan() []*heartbeatpb.TableSpan {
	return e.DDLEvent.NeedDroppedTableSpan
}

func (e *TxnEvent) GetNeedAddedTableSpan() []*heartbeatpb.TableSpan {
	return e.DDLEvent.NeedAddedTableSpan
}

func (e *TxnEvent) IsSyncPointEvent() bool {
	// TODO
	return false
}

func (e *TxnEvent) GetDDLQuery() string {
	if e.DDLEvent == nil {
		log.Error("DDLEvent is nil, should not happened in production env", zap.Any("event", e))
		return ""
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

// ColumnData represents a column value in row changed event
//
//msgp:ignore ColumnData
type ColumnData struct {
	// ColumnID may be just a mock id, because we don't store it in redo log.
	// So after restore from redo log, we need to give every a column a mock id.
	// The only guarantee is that the column id is unique in a RowChangedEvent
	ColumnID int64
	Value    interface{}

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int
}

// TODO: remove it
//
//msgp:ignore RowChangedEventData
type RowChangedEventData struct {
	StartTs  uint64
	CommitTs uint64

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

	Columns    []*ColumnData
	PreColumns []*ColumnData

	// ApproximateDataSize is the approximate size of protobuf binary
	// representation of this event.
	ApproximateDataSize int64

	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs uint64
}

type RowChangedEvent struct {
	PhysicalTableID int64

	StartTs  uint64
	CommitTs uint64

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
	TableInfo *TableInfo `msg:"-"`

	Columns    []*Column `msg:"columns"`
	PreColumns []*Column `msg:"pre-columns"`

	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs uint64 `msg:"replicating-ts"`
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
	Flag      ColumnFlagType `msg:"flag"`   // FIXME
	Value     interface{}    `msg:"column"` // FIXME: this is incorrect in some cases
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
