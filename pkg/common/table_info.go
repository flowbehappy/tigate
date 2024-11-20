package common

import (
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	datumTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap"
)

// ColumnFlagType is for encapsulating the flag operations for different flags.
type ColumnFlagType util.Flag

func (c *ColumnFlagType) Msgsize() int {
	return 8
}

func (c ColumnFlagType) MarshalMsg(b []byte) ([]byte, error) {
	return msgp.AppendUint64(b, uint64(c)), nil
}

func (c *ColumnFlagType) UnmarshalMsg(b []byte) (rest []byte, err error) {
	var value uint64
	value, rest, err = msgp.ReadUint64Bytes(b)
	if err != nil {
		return nil, err
	}
	*c = ColumnFlagType(value)
	return rest, nil
}

func (c ColumnFlagType) EncodeMsg(en *msgp.Writer) error {
	return en.WriteUint64(uint64(c))
}

func (c *ColumnFlagType) DecodeMsg(dc *msgp.Reader) error {
	value, err := dc.ReadUint64()
	if err != nil {
		return err
	}
	*c = ColumnFlagType(value)
	return nil
}

const (
	// BinaryFlag means the column charset is binary
	BinaryFlag ColumnFlagType = 1 << ColumnFlagType(iota)
	// HandleKeyFlag means the column is selected as the handle key
	// The handleKey is chosen by the following rules in the order:
	// 1. if the table has primary key, it's the handle key.
	// 2. If the table has not null unique key, it's the handle key.
	// 3. If the table has no primary key and no not null unique key, it has no handleKey.
	HandleKeyFlag
	// GeneratedColumnFlag means the column is a generated column
	GeneratedColumnFlag
	// PrimaryKeyFlag means the column is primary key
	PrimaryKeyFlag
	// UniqueKeyFlag means the column is unique key
	UniqueKeyFlag
	// MultipleKeyFlag means the column is multiple key
	MultipleKeyFlag
	// NullableFlag means the column is nullable
	NullableFlag
	// UnsignedFlag means the column stores an unsigned integer
	UnsignedFlag
)

// SetIsBinary sets BinaryFlag
func (b *ColumnFlagType) SetIsBinary() {
	(*Flag)(b).Add(Flag(BinaryFlag))
}

// UnsetIsBinary unsets BinaryFlag
func (b *ColumnFlagType) UnsetIsBinary() {
	(*Flag)(b).Remove(Flag(BinaryFlag))
}

// IsBinary shows whether BinaryFlag is set
func (b *ColumnFlagType) IsBinary() bool {
	return (*Flag)(b).HasAll(Flag(BinaryFlag))
}

// SetIsHandleKey sets HandleKey
func (b *ColumnFlagType) SetIsHandleKey() {
	(*Flag)(b).Add(Flag(HandleKeyFlag))
}

// UnsetIsHandleKey unsets HandleKey
func (b *ColumnFlagType) UnsetIsHandleKey() {
	(*Flag)(b).Remove(Flag(HandleKeyFlag))
}

// IsHandleKey shows whether HandleKey is set
func (b *ColumnFlagType) IsHandleKey() bool {
	return (*Flag)(b).HasAll(Flag(HandleKeyFlag))
}

// SetIsGeneratedColumn sets GeneratedColumn
func (b *ColumnFlagType) SetIsGeneratedColumn() {
	(*Flag)(b).Add(Flag(GeneratedColumnFlag))
}

// UnsetIsGeneratedColumn unsets GeneratedColumn
func (b *ColumnFlagType) UnsetIsGeneratedColumn() {
	(*Flag)(b).Remove(Flag(GeneratedColumnFlag))
}

// IsGeneratedColumn shows whether GeneratedColumn is set
func (b *ColumnFlagType) IsGeneratedColumn() bool {
	return (*Flag)(b).HasAll(Flag(GeneratedColumnFlag))
}

// SetIsPrimaryKey sets PrimaryKeyFlag
func (b *ColumnFlagType) SetIsPrimaryKey() {
	(*Flag)(b).Add(Flag(PrimaryKeyFlag))
}

// UnsetIsPrimaryKey unsets PrimaryKeyFlag
func (b *ColumnFlagType) UnsetIsPrimaryKey() {
	(*Flag)(b).Remove(Flag(PrimaryKeyFlag))
}

// IsPrimaryKey shows whether PrimaryKeyFlag is set
func (b *ColumnFlagType) IsPrimaryKey() bool {
	return (*Flag)(b).HasAll(Flag(PrimaryKeyFlag))
}

// SetIsUniqueKey sets UniqueKeyFlag
func (b *ColumnFlagType) SetIsUniqueKey() {
	(*Flag)(b).Add(Flag(UniqueKeyFlag))
}

// UnsetIsUniqueKey unsets UniqueKeyFlag
func (b *ColumnFlagType) UnsetIsUniqueKey() {
	(*Flag)(b).Remove(Flag(UniqueKeyFlag))
}

// IsUniqueKey shows whether UniqueKeyFlag is set
func (b *ColumnFlagType) IsUniqueKey() bool {
	return (*Flag)(b).HasAll(Flag(UniqueKeyFlag))
}

// IsMultipleKey shows whether MultipleKeyFlag is set
func (b *ColumnFlagType) IsMultipleKey() bool {
	return (*Flag)(b).HasAll(Flag(MultipleKeyFlag))
}

// SetIsMultipleKey sets MultipleKeyFlag
func (b *ColumnFlagType) SetIsMultipleKey() {
	(*Flag)(b).Add(Flag(MultipleKeyFlag))
}

// UnsetIsMultipleKey unsets MultipleKeyFlag
func (b *ColumnFlagType) UnsetIsMultipleKey() {
	(*Flag)(b).Remove(Flag(MultipleKeyFlag))
}

// IsNullable shows whether NullableFlag is set
func (b *ColumnFlagType) IsNullable() bool {
	return (*Flag)(b).HasAll(Flag(NullableFlag))
}

// SetIsNullable sets NullableFlag
func (b *ColumnFlagType) SetIsNullable() {
	(*Flag)(b).Add(Flag(NullableFlag))
}

// UnsetIsNullable unsets NullableFlag
func (b *ColumnFlagType) UnsetIsNullable() {
	(*Flag)(b).Remove(Flag(NullableFlag))
}

// IsUnsigned shows whether UnsignedFlag is set
func (b *ColumnFlagType) IsUnsigned() bool {
	return (*Flag)(b).HasAll(Flag(UnsignedFlag))
}

// SetIsUnsigned sets UnsignedFlag
func (b *ColumnFlagType) SetIsUnsigned() {
	(*Flag)(b).Add(Flag(UnsignedFlag))
}

// UnsetIsUnsigned unsets UnsignedFlag
func (b *ColumnFlagType) UnsetIsUnsigned() {
	(*Flag)(b).Remove(Flag(UnsignedFlag))
}

// TableName represents name of a table, includes table name and schema name.
type TableName struct {
	Schema      string `toml:"db-name" msg:"db-name"`
	Table       string `toml:"tbl-name" msg:"tbl-name"`
	TableID     int64  `toml:"tbl-id" msg:"tbl-id"`
	IsPartition bool   `toml:"is-partition" msg:"is-partition"`
}

// String implements fmt.Stringer interface.
func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// QuoteSchema quotes a full table name
func QuoteSchema(schema string, table string) string {
	var builder strings.Builder
	builder.WriteString("`")
	builder.WriteString(EscapeName(schema))
	builder.WriteString("`.`")
	builder.WriteString(EscapeName(table))
	builder.WriteString("`")
	return builder.String()
}

// QuoteName wraps a name with "`"
func QuoteName(name string) string {
	return "`" + EscapeName(name) + "`"
}

// EscapeName replaces all "`" in name with double "`"
func EscapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

// QuoteString returns quoted full table name
func (t TableName) QuoteString() string {
	return QuoteSchema(t.Schema, t.Table)
}

// GetSchema returns schema name.
func (t *TableName) GetSchema() string {
	return t.Schema
}

// GetTable returns table name.
func (t *TableName) GetTable() string {
	return t.Table
}

// GetTableID returns table ID.
func (t *TableName) GetTableID() int64 {
	return t.TableID
}

const (
	// HandleIndexPKIsHandle represents that the handle index is the pk and the pk is the handle
	HandleIndexPKIsHandle = -1
	// HandleIndexTableIneligible represents that the table is ineligible
	HandleIndexTableIneligible = -2
)

const (
	preSQLInsert = iota
	preSQLReplace
	preSQLUpdate
	preSQLDelete
)

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	*model.TableInfo `json:"table_info"`
	SchemaID         int64 `json:"schema_id"`
	// NOTICE: We probably store the logical ID inside TableName,
	// not the physical ID.
	// For normal table, there is only one ID, which is the physical ID.
	// AKA TIDB_TABLE_ID.
	// For partitioned table, there are two kinds of ID:
	// 1. TIDB_PARTITION_ID is the physical ID of the partition.
	// 2. TIDB_TABLE_ID is the logical ID of the table.
	// In general, we always use the physical ID to represent a table, but we
	// record the logical ID from the DDL event(job.BinlogInfo.TableInfo).
	// So be careful when using the TableInfo.
	TableName TableName `json:"table_name"`

	ColumnSchema *ColumnSchema `json:"column_schema"`

	PreSQLs map[int]string `json:"pre_sqls"`
}

func (ti *TableInfo) GetColumnsOffset() map[int64]int {
	return ti.ColumnSchema.ColumnsOffset
}

func (ti *TableInfo) InitPreSQLs() {
	// TODO: find better way to hold the preSQLs
	if len(ti.Columns) == 0 {
		log.Warn("table has no columns, should be in test mode", zap.String("table", ti.TableName.String()))
		return
	}
	ti.PreSQLs = make(map[int]string)
	ti.PreSQLs[preSQLInsert] = ti.genPreSQLInsert(false, true)
	ti.PreSQLs[preSQLReplace] = ti.genPreSQLInsert(true, true)
	ti.PreSQLs[preSQLUpdate] = ti.genPreSQLUpdate()
}

func (ti *TableInfo) genPreSQLInsert(isReplace bool, needPlaceHolder bool) string {
	var builder strings.Builder

	if isReplace {
		builder.WriteString("REPLACE INTO ")
	} else {
		builder.WriteString("INSERT INTO ")
	}
	quoteTable := ti.TableName.QuoteString()
	builder.WriteString(quoteTable)
	builder.WriteString(" (")
	builder.WriteString(ti.getColumnList(false))
	builder.WriteString(") VALUES ")

	if needPlaceHolder {
		builder.WriteString("(")
		builder.WriteString(placeHolder(len(ti.Columns) - ti.ColumnSchema.VirtualColumnCount))
		builder.WriteString(")")
	}
	return builder.String()
}

func (ti *TableInfo) genPreSQLUpdate() string {
	var builder strings.Builder
	builder.WriteString("UPDATE ")
	builder.WriteString(ti.TableName.QuoteString())
	builder.WriteString(" SET ")
	builder.WriteString(ti.getColumnList(true))
	return builder.String()
}

// placeHolder returns a string with n placeholders separated by commas
// n must be greater or equal than 1, or the function will panic
func placeHolder(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 1)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}

func (ti *TableInfo) getColumnList(isUpdate bool) string {
	var b strings.Builder
	for i, col := range ti.Columns {
		if col == nil || ti.ColumnSchema.ColumnsFlag[col.ID].IsGeneratedColumn() {
			continue
		}
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(QuoteName(col.Name.O))
		if isUpdate {
			b.WriteString(" = ?")
		}
	}
	return b.String()
}

func (ti *TableInfo) GetPreInsertSQL() string {
	return ti.PreSQLs[preSQLInsert]
}

func (ti *TableInfo) GetPreReplaceSQL() string {
	return ti.PreSQLs[preSQLReplace]
}

func (ti *TableInfo) GetPreUpdateSQL() string {
	return ti.PreSQLs[preSQLUpdate]
}

// GetColumnInfo returns the column info by ID
func (ti *TableInfo) GetColumnInfo(colID int64) (info *model.ColumnInfo, exist bool) {
	colOffset, exist := ti.ColumnSchema.ColumnsOffset[colID]
	if !exist {
		return nil, false
	}
	return ti.Columns[colOffset], true
}

// ForceGetColumnInfo return the column info by ID
// Caller must ensure `colID` exists
func (ti *TableInfo) ForceGetColumnInfo(colID int64) *model.ColumnInfo {
	colInfo, ok := ti.GetColumnInfo(colID)
	if !ok {
		log.Panic("invalid column id", zap.Int64("columnID", colID))
	}
	return colInfo
}

// ForceGetColumnFlagType return the column flag type by ID
// Caller must ensure `colID` exists
func (ti *TableInfo) ForceGetColumnFlagType(colID int64) *ColumnFlagType {
	flag, ok := ti.ColumnSchema.ColumnsFlag[colID]
	if !ok {
		log.Panic("invalid column id", zap.Int64("columnID", colID))
	}
	return flag
}

// ForceGetColumnName return the column name by ID
// Caller must ensure `colID` exists
func (ti *TableInfo) ForceGetColumnName(colID int64) string {
	return ti.ForceGetColumnInfo(colID).Name.O
}

// ForceGetColumnIDByName return column ID by column name
// Caller must ensure `colID` exists
func (ti *TableInfo) ForceGetColumnIDByName(name string) int64 {
	colID, ok := ti.ColumnSchema.NameToColID[name]
	if !ok {
		log.Panic("invalid column name", zap.String("column", name))
	}
	return colID
}

// GetSchemaName returns the schema name of the table
func (ti *TableInfo) GetSchemaName() string {
	return ti.TableName.Schema
}

// GetTableName returns the table name of the table
func (ti *TableInfo) GetTableName() string {
	return ti.TableName.Table
}

// GetSchemaNamePtr returns the pointer to the schema name of the table
func (ti *TableInfo) GetSchemaNamePtr() *string {
	return &ti.TableName.Schema
}

// GetTableNamePtr returns the pointer to the table name of the table
func (ti *TableInfo) GetTableNamePtr() *string {
	return &ti.TableName.Table
}

// IsPartitionTable returns whether the table is partition table
func (ti *TableInfo) IsPartitionTable() bool {
	return ti.TableName.IsPartition
}

func (ti *TableInfo) String() string {
	return fmt.Sprintf("TableInfo, ID: %d, Name:%s, ColNum: %d, IdxNum: %d, PKIsHandle: %t",
		ti.ID, ti.TableName, len(ti.Columns), len(ti.Indices), ti.PKIsHandle)
}

// GetRowColInfos returns all column infos for rowcodec
func (ti *TableInfo) GetRowColInfos() ([]int64, map[int64]*datumTypes.FieldType, []rowcodec.ColInfo) {
	return ti.ColumnSchema.HandleColID, ti.ColumnSchema.RowColFieldTps, ti.ColumnSchema.RowColInfos
}

// GetFieldSlice returns the field types of all columns
func (ti *TableInfo) GetFieldSlice() []*datumTypes.FieldType {
	return ti.ColumnSchema.RowColFieldTpsSlice
}

// GetColInfosForRowChangedEvent return column infos for non-virtual columns
// The column order in the result is the same as the order in its corresponding RowChangedEvent
func (ti *TableInfo) GetColInfosForRowChangedEvent() []rowcodec.ColInfo {
	return *ti.ColumnSchema.RowColInfosWithoutVirtualCols
}

func (ti *TableInfo) GetColumnFlags() map[int64]*ColumnFlagType {
	return ti.ColumnSchema.ColumnsFlag
}

// IsColCDCVisible returns whether the col is visible for CDC
func IsColCDCVisible(col *model.ColumnInfo) bool {
	// this column is a virtual generated column
	if col.IsGenerated() && !col.GeneratedStored {
		return false
	}
	return true
}

// HasVirtualColumns returns whether the table has virtual columns
func (ti *TableInfo) HasVirtualColumns() bool {
	return ti.ColumnSchema.VirtualColumnCount > 0
}

// IsEligible returns whether the table is a eligible table
func (ti *TableInfo) IsEligible(forceReplicate bool) bool {
	// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
	// See https://github.com/pingcap/tiflow/issues/4559
	if ti.IsSequence() {
		return false
	}
	if forceReplicate {
		return true
	}
	if ti.IsView() {
		return true
	}
	return ti.ColumnSchema.HasUniqueColumn
}

// GetIndex return the corresponding index by the given name.
func (ti *TableInfo) GetIndex(name string) *model.IndexInfo {
	for _, index := range ti.Indices {
		if index != nil && index.Name.O == name {
			return index
		}
	}
	return nil
}

// IndexByName returns the index columns and offsets of the corresponding index by name
func (ti *TableInfo) IndexByName(name string) ([]string, []int, bool) {
	index := ti.GetIndex(name)
	if index == nil {
		return nil, nil, false
	}
	names := make([]string, 0, len(index.Columns))
	offset := make([]int, 0, len(index.Columns))
	for _, col := range index.Columns {
		names = append(names, col.Name.O)
		offset = append(offset, col.Offset)
	}
	return names, offset, true
}

// OffsetsByNames returns the column offsets of the corresponding columns by names
// If any column does not exist, return false
func (ti *TableInfo) OffsetsByNames(names []string) ([]int, bool) {
	// todo: optimize it
	columnOffsets := make(map[string]int, len(ti.Columns))
	for _, col := range ti.Columns {
		if col != nil {
			columnOffsets[col.Name.O] = col.Offset
		}
	}

	result := make([]int, 0, len(names))
	for _, col := range names {
		offset, ok := columnOffsets[col]
		if !ok {
			return nil, false
		}
		result = append(result, offset)
	}

	return result, true
}

// GetPrimaryKeyColumnNames returns the primary key column names
func (ti *TableInfo) GetPrimaryKeyColumnNames() []string {
	var result []string
	if ti.PKIsHandle {
		result = append(result, ti.GetPkColInfo().Name.O)
		return result
	}

	indexInfo := ti.GetPrimaryKey()
	if indexInfo != nil {
		for _, col := range indexInfo.Columns {
			result = append(result, col.Name.O)
		}
	}
	return result
}

// GetVersion returns the version of the table info
// It is the last TSO when the table schema was changed
func (ti *TableInfo) GetVersion() uint64 {
	return ti.TableInfo.UpdateTS
}

// WrapTableInfo creates a TableInfo from a model.TableInfo
func WrapTableInfo(schemaID int64, schemaName string, info *model.TableInfo) *TableInfo {
	// search column schema object
	sharedColumnSchemaStorage := GetSharedColumnSchemaStorage()
	columnSchema := sharedColumnSchemaStorage.GetOrSetColumnSchema(info)
	log.Info("hyy column schema count", zap.Any("count", sharedColumnSchemaStorage.Len()))

	ti := &TableInfo{
		TableInfo: info,
		SchemaID:  schemaID,
		TableName: TableName{
			Schema:      schemaName,
			Table:       info.Name.O,
			TableID:     info.ID,
			IsPartition: info.GetPartitionInfo() != nil,
		},
		ColumnSchema: columnSchema,
	}

	return ti
}

// GetColumnDefaultValue returns the default definition of a column.
func GetColumnDefaultValue(col *model.ColumnInfo) interface{} {
	defaultValue := col.GetDefaultValue()
	if defaultValue == nil {
		defaultValue = col.GetOriginDefaultValue()
	}
	defaultDatum := datumTypes.NewDatum(defaultValue)
	return defaultDatum.GetValue()
}
