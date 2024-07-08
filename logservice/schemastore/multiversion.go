package schemastore

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	datumTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
)

// dbInfo interface should be much simpler?
// dbInfo 会变吗？db id 不会被复用吧？所以不存在多版本？

type tableInfoItem struct {
	ts   Timestamp
	info *TableInfo
}

type versionedTableInfoStore struct {
	mu sync.Mutex

	startTS Timestamp

	dispatchers map[DispatcherID]Timestamp

	// ordered by ts
	infos []tableInfoItem

	deleteVersion Timestamp
}

func newEmptyVersionedTableInfoStore() *versionedTableInfoStore {
	return &versionedTableInfoStore{
		dispatchers:   make(map[DispatcherID]Timestamp),
		infos:         make([]tableInfoItem, 0),
		deleteVersion: math.MaxUint64,
	}
}

// TableInfo can from upstream snapshot or create table ddl
func newVersionedTableInfoStore(startTS Timestamp, info *TableInfo) *versionedTableInfoStore {
	store := newEmptyVersionedTableInfoStore()
	store.infos = append(store.infos, tableInfoItem{ts: startTS, info: info})
	return store
}

func (v *versionedTableInfoStore) getStartTS() Timestamp {
	v.mu.Lock()
	defer v.mu.Unlock()
	// this is not very accurate, it's large than the actual startTS
	return v.startTS
}

func (v *versionedTableInfoStore) getTableInfo(ts Timestamp) (*TableInfo, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// TODO: 检查边界条件
	left := findTableInfoIndex(v.infos, ts)
	if left == 0 {
		return nil, errors.New("no table info found")
	}
	return v.infos[left-1].info, nil
}

func (v *versionedTableInfoStore) registerDispatcher(dispatcherID DispatcherID, ts Timestamp) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.dispatchers[dispatcherID]; ok {
		log.Info("dispatcher already registered", zap.String("dispatcherID", string(dispatcherID)))
	}
	v.dispatchers[dispatcherID] = ts
	v.tryGC()
}

// may trigger gc, or totally removed?
func (v *versionedTableInfoStore) unregisterDispatcher(dispatcherID DispatcherID) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.dispatchers, dispatcherID)
	if len(v.dispatchers) == 0 {
		return true
	}
	v.tryGC()
	return false
}

// will trigger gc
func (v *versionedTableInfoStore) updateDispatcherCheckpointTS(dispatcherID DispatcherID, ts Timestamp) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.dispatchers[dispatcherID]; !ok {
		return errors.New("dispatcher not found")
	}
	v.dispatchers[dispatcherID] = ts
	v.tryGC()
	return nil
}

// TODO: find a better function name and param name
// 找到第一个大于 ts 的 index
func findTableInfoIndex(infos []tableInfoItem, ts Timestamp) int {
	left, right := 0, len(infos)
	for left < right {
		mid := left + (right-left)/2
		if infos[mid].ts <= ts {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// lock is held by caller
func (v *versionedTableInfoStore) tryGC() {
	var minTS Timestamp
	minTS = math.MaxUint64
	for _, ts := range v.dispatchers {
		if ts < minTS {
			minTS = ts
		}
	}
	// only keep one item in infos which have a ts smaller than minTS
	if len(v.infos) == 0 {
		log.Fatal("no table info found")
	}

	// TODO: 检查边界条件
	left := findTableInfoIndex(v.infos, minTS)
	if left == 0 {
		return
	}
	v.infos = v.infos[left-1:]
	v.startTS = minTS
}

func (v *versionedTableInfoStore) getAllRegisteredDispatchers() map[DispatcherID]Timestamp {
	v.mu.Lock()
	defer v.mu.Unlock()
	result := make(map[DispatcherID]Timestamp, len(v.dispatchers))
	for k, v := range v.dispatchers {
		result[k] = v
	}
	return result
}

func (v *versionedTableInfoStore) applyDDL(job *model.Job) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.infos) == 0 {
		log.Fatal("no table info found")
	}
	lastTableInfo := v.infos[len(v.infos)-1].info.Clone()

	switch job.Type {
	case model.ActionRenameTable:
		// TODO
	case model.ActionDropTable, model.ActionDropView:
		// TODO
	case model.ActionTruncateTable:
		// just delete this table?
	default:
	}
}

func (v *versionedTableInfoStore) checkAndCopyTailFrom(src *versionedTableInfoStore) {
	v.mu.Lock()
	defer v.mu.Unlock()
	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.infos) == 0 {
		return
	}
	if len(v.infos) == 0 {
		v.infos = append(v.infos, src.infos[len(src.infos)-1])
	}
	firstSrcTS := src.infos[0].ts
	foundFirstSrcTS := false
	startCheckIndex := 0
	// TODO: use binary search
	for i, item := range v.infos {
		if item.ts < firstSrcTS {
			continue
		} else if item.ts == firstSrcTS {
			foundFirstSrcTS = true
			startCheckIndex = i
			break
		} else {
			if !foundFirstSrcTS {
				log.Panic("not found")
			}
			if item.ts != src.infos[i-startCheckIndex].ts {
				log.Panic("not match")
			}
		}
	}
	copyStartIndex := len(src.infos) - (len(v.infos) - startCheckIndex)
	v.infos = append(v.infos, src.infos[copyStartIndex:]...)
}

// WrapTableInfo creates a TableInfo from a model.TableInfo
func WrapTableInfo(schemaID int64, schemaName string, version uint64, info *model.TableInfo) *TableInfo {
	ti := &TableInfo{
		TableInfo: info,
		SchemaID:  schemaID,
		TableName: TableName{
			Schema:      schemaName,
			Table:       info.Name.O,
			TableID:     info.ID,
			IsPartition: info.GetPartitionInfo() != nil,
		},
		hasUniqueColumn:  false,
		Version:          version,
		columnsOffset:    make(map[int64]int, len(info.Columns)),
		nameToColID:      make(map[string]int64, len(info.Columns)),
		RowColumnsOffset: make(map[int64]int, len(info.Columns)),
		ColumnsFlag:      make(map[int64]*ColumnFlagType, len(info.Columns)),
		handleColID:      []int64{-1},
		HandleIndexID:    HandleIndexTableIneligible,
		rowColInfos:      make([]rowcodec.ColInfo, len(info.Columns)),
		rowColFieldTps:   make(map[int64]*types.FieldType, len(info.Columns)),
	}

	rowColumnsCurrentOffset := 0

	ti.virtualColumnCount = 0
	for i, col := range ti.Columns {
		ti.columnsOffset[col.ID] = i
		pkIsHandle := false
		if IsColCDCVisible(col) {
			ti.nameToColID[col.Name.O] = col.ID
			ti.RowColumnsOffset[col.ID] = rowColumnsCurrentOffset
			rowColumnsCurrentOffset++
			pkIsHandle = (ti.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag())) || col.ID == model.ExtraHandleID
			if pkIsHandle {
				// pk is handle
				ti.handleColID = []int64{col.ID}
				ti.HandleIndexID = HandleIndexPKIsHandle
				ti.hasUniqueColumn = true
				ti.IndexColumnsOffset = append(ti.IndexColumnsOffset, []int{ti.RowColumnsOffset[col.ID]})
			} else if ti.IsCommonHandle {
				ti.HandleIndexID = HandleIndexPKIsHandle
				ti.handleColID = ti.handleColID[:0]
				pkIdx := tables.FindPrimaryIndex(info)
				for _, pkCol := range pkIdx.Columns {
					id := info.Columns[pkCol.Offset].ID
					ti.handleColID = append(ti.handleColID, id)
				}
			}
		} else {
			ti.virtualColumnCount += 1
		}
		ti.rowColInfos[i] = rowcodec.ColInfo{
			ID:            col.ID,
			IsPKHandle:    pkIsHandle,
			Ft:            col.FieldType.Clone(),
			VirtualGenCol: col.IsGenerated(),
		}
		ti.rowColFieldTps[col.ID] = ti.rowColInfos[i].Ft
	}

	for _, idx := range ti.Indices {
		if ti.IsIndexUnique(idx) {
			ti.hasUniqueColumn = true
		}
		if idx.Primary || idx.Unique {
			indexColOffset := make([]int, 0, len(idx.Columns))
			for _, idxCol := range idx.Columns {
				colInfo := ti.Columns[idxCol.Offset]
				if IsColCDCVisible(colInfo) {
					indexColOffset = append(indexColOffset, ti.RowColumnsOffset[colInfo.ID])
				}
			}
			if len(indexColOffset) > 0 {
				ti.IndexColumnsOffset = append(ti.IndexColumnsOffset, indexColOffset)
			}
		}
	}

	ti.initRowColInfosWithoutVirtualCols()
	ti.findHandleIndex()
	ti.initColumnsFlag()
	return ti
}

func (ti *TableInfo) initRowColInfosWithoutVirtualCols() {
	if ti.virtualColumnCount == 0 {
		ti.rowColInfosWithoutVirtualCols = &ti.rowColInfos
		return
	}
	colInfos := make([]rowcodec.ColInfo, 0, len(ti.rowColInfos)-ti.virtualColumnCount)
	for i, col := range ti.Columns {
		if IsColCDCVisible(col) {
			colInfos = append(colInfos, ti.rowColInfos[i])
		}
	}
	if len(colInfos) != len(ti.rowColInfos)-ti.virtualColumnCount {
		log.Panic("invalid rowColInfosWithoutVirtualCols",
			zap.Int("len(colInfos)", len(colInfos)),
			zap.Int("len(ti.rowColInfos)", len(ti.rowColInfos)),
			zap.Int("ti.virtualColumnCount", ti.virtualColumnCount))
	}
	ti.rowColInfosWithoutVirtualCols = &colInfos
}

func (ti *TableInfo) findHandleIndex() {
	if ti.HandleIndexID == HandleIndexPKIsHandle {
		// pk is handle
		return
	}
	handleIndexOffset := -1
	for i, idx := range ti.Indices {
		if !ti.IsIndexUnique(idx) {
			continue
		}
		if idx.Primary {
			handleIndexOffset = i
			break
		}
		if handleIndexOffset < 0 {
			handleIndexOffset = i
		} else {
			if len(ti.Indices[handleIndexOffset].Columns) > len(ti.Indices[i].Columns) ||
				(len(ti.Indices[handleIndexOffset].Columns) == len(ti.Indices[i].Columns) &&
					ti.Indices[handleIndexOffset].ID > ti.Indices[i].ID) {
				handleIndexOffset = i
			}
		}
	}
	if handleIndexOffset >= 0 {
		ti.HandleIndexID = ti.Indices[handleIndexOffset].ID
	}
}

func (ti *TableInfo) initColumnsFlag() {
	for _, colInfo := range ti.Columns {
		var flag ColumnFlagType
		if colInfo.GetCharset() == "binary" {
			flag.SetIsBinary()
		}
		if colInfo.IsGenerated() {
			flag.SetIsGeneratedColumn()
		}
		if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			flag.SetIsPrimaryKey()
			if ti.HandleIndexID == HandleIndexPKIsHandle {
				flag.SetIsHandleKey()
			}
		}
		if mysql.HasUniKeyFlag(colInfo.GetFlag()) {
			flag.SetIsUniqueKey()
		}
		if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
			flag.SetIsNullable()
		}
		if mysql.HasMultipleKeyFlag(colInfo.GetFlag()) {
			flag.SetIsMultipleKey()
		}
		if mysql.HasUnsignedFlag(colInfo.GetFlag()) {
			flag.SetIsUnsigned()
		}
		ti.ColumnsFlag[colInfo.ID] = &flag
	}

	// In TiDB, just as in MySQL, only the first column of an index can be marked as "multiple key" or "unique key",
	// and only the first column of a unique index may be marked as "unique key".
	// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html.
	// Yet if an index has multiple columns, we would like to easily determine that all those columns are indexed,
	// which is crucial for the completeness of the information we pass to the downstream.
	// Therefore, instead of using the MySQL standard,
	// we made our own decision to mark all columns in an index with the appropriate flag(s).
	for _, idxInfo := range ti.Indices {
		for _, idxCol := range idxInfo.Columns {
			colInfo := ti.Columns[idxCol.Offset]
			flag := ti.ColumnsFlag[colInfo.ID]
			if idxInfo.Primary {
				flag.SetIsPrimaryKey()
			} else if idxInfo.Unique {
				flag.SetIsUniqueKey()
			}
			if len(idxInfo.Columns) > 1 {
				flag.SetIsMultipleKey()
			}
			if idxInfo.ID == ti.HandleIndexID && ti.HandleIndexID >= 0 {
				flag.SetIsHandleKey()
			}
			ti.ColumnsFlag[colInfo.ID] = flag
		}
	}
}

// GetColumnInfo returns the column info by ID
func (ti *TableInfo) GetColumnInfo(colID int64) (info *model.ColumnInfo, exist bool) {
	colOffset, exist := ti.columnsOffset[colID]
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
	flag, ok := ti.ColumnsFlag[colID]
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
	colID, ok := ti.nameToColID[name]
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
	return fmt.Sprintf("TableInfo, ID: %d, Name:%s, ColNum: %d, IdxNum: %d, PKIsHandle: %t", ti.ID, ti.TableName, len(ti.Columns), len(ti.Indices), ti.PKIsHandle)
}

// GetRowColInfos returns all column infos for rowcodec
func (ti *TableInfo) GetRowColInfos() ([]int64, map[int64]*types.FieldType, []rowcodec.ColInfo) {
	return ti.handleColID, ti.rowColFieldTps, ti.rowColInfos
}

// GetColInfosForRowChangedEvent return column infos for non-virtual columns
// The column order in the result is the same as the order in its corresponding RowChangedEvent
func (ti *TableInfo) GetColInfosForRowChangedEvent() []rowcodec.ColInfo {
	return *ti.rowColInfosWithoutVirtualCols
}

// IsColCDCVisible returns whether the col is visible for CDC
func IsColCDCVisible(col *model.ColumnInfo) bool {
	// this column is a virtual generated column
	if col.IsGenerated() && !col.GeneratedStored {
		return false
	}
	return true
}

// HasUniqueColumn returns whether the table has a unique column
func (ti *TableInfo) HasUniqueColumn() bool {
	return ti.hasUniqueColumn
}

// HasVirtualColumns returns whether the table has virtual columns
func (ti *TableInfo) HasVirtualColumns() bool {
	return ti.virtualColumnCount > 0
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
	return ti.HasUniqueColumn()
}

// IsIndexUnique returns whether the index is unique
func (ti *TableInfo) IsIndexUnique(indexInfo *model.IndexInfo) bool {
	if indexInfo.Primary {
		return true
	}
	if indexInfo.Unique {
		for _, col := range indexInfo.Columns {
			colInfo := ti.Columns[col.Offset]
			if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
				return false
			}
			// this column is a virtual generated column
			if colInfo.IsGenerated() && !colInfo.GeneratedStored {
				return false
			}
		}
		return true
	}
	return false
}

// Clone clones the TableInfo
func (ti *TableInfo) Clone() *TableInfo {
	return WrapTableInfo(ti.SchemaID, ti.TableName.Schema, ti.Version, ti.TableInfo.Clone())
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

// GetColumnDefaultValue returns the default definition of a column.
func GetColumnDefaultValue(col *model.ColumnInfo) interface{} {
	defaultValue := col.GetDefaultValue()
	if defaultValue == nil {
		defaultValue = col.GetOriginDefaultValue()
	}
	defaultDatum := datumTypes.NewDatum(defaultValue)
	return defaultDatum.GetValue()
}
