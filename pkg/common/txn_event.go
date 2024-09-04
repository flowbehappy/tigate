package common

import (
	"fmt"
	"unsafe"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	timodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/integrity"
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
	BlockedTables     *InfluencedTables `json:"blocked_tables"`
	NeedDroppedTables *InfluencedTables `json:"need_dropped_tables"`
	NeedAddedTables   []Table           `json:"need_added_tables"`
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
	Span *heartbeatpb.TableSpan `msg:"-"`

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
	return ddlType == model.ActionAddColumn || ddlType == model.ActionDropColumn || ddlType == model.ActionModifyColumn ||
		ddlType == model.ActionAddIndex || ddlType == model.ActionDropIndex || ddlType == model.ActionModifyTableComment ||
		ddlType == model.ActionRebaseAutoID || ddlType == model.ActionSetDefaultValue || ddlType == model.ActionShardRowID ||
		ddlType == model.ActionModifyTableCharsetAndCollate || ddlType == model.ActionCreateView || ddlType == model.ActionDropView ||
		ddlType == model.ActionAddForeignKey || ddlType == model.ActionDropForeignKey || ddlType == model.ActionRenameIndex ||
		ddlType == model.ActionLockTable || ddlType == model.ActionUnlockTable || ddlType == model.ActionSetTiFlashReplica ||
		ddlType == model.ActionAddPrimaryKey || ddlType == model.ActionDropPrimaryKey || ddlType == model.ActionAddColumns ||
		ddlType == model.ActionDropColumns || ddlType == model.ActionModifyTableAutoIdCache || ddlType == model.ActionRebaseAutoRandomBase ||
		ddlType == model.ActionAlterIndexVisibility || ddlType == model.ActionAddCheckConstraint || ddlType == model.ActionDropCheckConstraint ||
		ddlType == model.ActionAlterCheckConstraint || ddlType == model.ActionDropIndexes || ddlType == model.ActionAlterTableAttributes ||
		ddlType == model.ActionAlterCacheTable || ddlType == model.ActionAlterNoCacheTable || ddlType == model.ActionMultiSchemaChange ||
		ddlType == model.ActionAlterTTLInfo || ddlType == model.ActionAlterTTLRemove || ddlType == model.ActionRepairTable ||
		ddlType == model.ActionCreatePlacementPolicy || ddlType == model.ActionAlterPlacementPolicy || ddlType == model.ActionRecoverTable ||
		ddlType == model.ActionDropPlacementPolicy || ddlType == model.ActionCreateSchema || ddlType == model.ActionRecoverSchema ||
		ddlType == model.ActionCreateTables || ddlType == model.ActionRenameTable || ddlType == model.ActionTruncateTable || ddlType == model.ActionCreateTable

}

type InfluenceType int

const (
	All InfluenceType = iota // influence all tables
	DB                       // influence all tables in the same database
	Normal
)

func (t InfluenceType) toPB() heartbeatpb.InfluenceType {
	switch t {
	case All:
		return heartbeatpb.InfluenceType_All
	case DB:
		return heartbeatpb.InfluenceType_DB
	case Normal:
		return heartbeatpb.InfluenceType_Normal
	default:
		log.Error("unknown influence type")
	}
	return heartbeatpb.InfluenceType_Normal
}

type InfluencedTables struct {
	InfluenceType InfluenceType
	TableIDs      []int64
	SchemaID      int64
}

func (i *InfluencedTables) ToPB() *heartbeatpb.InfluencedTables {
	if i == nil {
		return nil
	}
	return &heartbeatpb.InfluencedTables{
		InfluenceType: i.InfluenceType.toPB(),
		TableIDs:      i.TableIDs,
		SchemaID:      i.SchemaID,
	}
}

func ToTablesPB(tables []Table) []*heartbeatpb.Table {
	res := make([]*heartbeatpb.Table, len(tables))
	for i, t := range tables {
		res[i] = &heartbeatpb.Table{
			SchemaID: t.SchemaID,
			TableID:  t.TableID,
		}
	}
	return res
}

type Table struct {
	SchemaID int64
	TableID  int64
}

func (e *TxnEvent) GetBlockedTables() *InfluencedTables {
	return e.DDLEvent.BlockedTables
}

func (e *TxnEvent) GetNeedDroppedTables() *InfluencedTables {
	return e.DDLEvent.NeedDroppedTables
}

func (e *TxnEvent) GetNeedAddedTables() []Table {
	return e.DDLEvent.NeedAddedTables
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

	// Checksum for the event, only not nil if the upstream TiDB enable the row level checksum
	// and TiCDC set the integrity check level to the correctness.
	Checksum *integrity.Checksum
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

// IsDelete returns true if the row is a delete event
func (r *RowChangedEvent) IsDelete() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) == 0
}

// IsInsert returns true if the row is an insert event
func (r *RowChangedEvent) IsInsert() bool {
	return len(r.PreColumns) == 0 && len(r.Columns) != 0
}

// IsUpdate returns true if the row is an update event
func (r *RowChangedEvent) IsUpdate() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) != 0
}

// HandleKeyColInfos returns the column(s) and colInfo(s) corresponding to the handle key(s)
func (r *RowChangedEvent) HandleKeyColInfos() ([]*Column, []rowcodec.ColInfo) {
	pkeyCols := make([]*Column, 0)
	pkeyColInfos := make([]rowcodec.ColInfo, 0)

	var cols []*Column
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	tableInfo := r.TableInfo
	colInfos := tableInfo.GetColInfosForRowChangedEvent()
	for i, col := range cols {
		if col != nil && col.Flag.IsHandleKey() {
			pkeyCols = append(pkeyCols, col)
			pkeyColInfos = append(pkeyColInfos, colInfos[i])
		}
	}

	// It is okay not to have handle keys, so the empty array is an acceptable result
	return pkeyCols, pkeyColInfos
}

// PrimaryKeyColumnNames return all primary key's name
func (r *RowChangedEvent) PrimaryKeyColumnNames() []string {
	var result []string

	var cols []*Column
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	result = make([]string, 0)
	for _, col := range cols {
		if col != nil && col.Flag.IsPrimaryKey() {
			result = append(result, col.Name)
		}
	}
	return result
}

// GetHandleAndUniqueIndexOffsets4Test is used to get the offsets of handle columns and other unique index columns in test
func GetHandleAndUniqueIndexOffsets4Test(cols []*Column) [][]int {
	result := make([][]int, 0)
	handleColumns := make([]int, 0)
	for i, col := range cols {
		if col.Flag.IsHandleKey() {
			handleColumns = append(handleColumns, i)
		} else if col.Flag.IsUniqueKey() {
			// When there is a unique key which is not handle key,
			// we cannot get the accurate index info for this key.
			// So just be aggressive to make each unique column a unique index
			// to make sure there is no write conflict when syncing data in tests.
			result = append(result, []int{i})
		}
	}
	if len(handleColumns) != 0 {
		result = append(result, handleColumns)
	}
	return result
}

type MQRowEvent struct {
	Key      timodel.TopicPartitionKey
	RowEvent RowEvent
}

type RowEvent struct {
	Event    *RowChangedEvent
	Callback func()
}
