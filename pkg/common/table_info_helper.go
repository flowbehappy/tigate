package common

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	datumTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
)

// hash representation for ColumnSchema of TableInfo
// Considering sha-256 output is 32 bytes, we use 4 uint64 to represent the hash value.
type Digest struct {
	a uint64
	b uint64
	c uint64
	d uint64
}

func ToDigest(b []byte) Digest {
	return Digest{
		a: binary.BigEndian.Uint64(b[0:8]),
		b: binary.BigEndian.Uint64(b[8:16]),
		c: binary.BigEndian.Uint64(b[16:24]),
		d: binary.BigEndian.Uint64(b[24:32]),
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func hashTableInfo(tableInfo *model.TableInfo) Digest {
	sha256Hasher := sha256.New()
	buf := make([]byte, 8)

	// col info
	sha256Hasher.Write([]byte("colInfo"))
	binary.BigEndian.PutUint64(buf, uint64(len(tableInfo.Columns)))
	sha256Hasher.Write(buf)
	for _, col := range tableInfo.Columns {
		// column ID
		binary.BigEndian.PutUint64(buf, uint64(col.ID))
		sha256Hasher.Write(buf)
		// column name
		sha256Hasher.Write([]byte(col.Name.O))
		// column type
		columnType := col.FieldType
		sha256Hasher.Write([]byte{columnType.GetType()})
		binary.BigEndian.PutUint64(buf, uint64(columnType.GetFlag()))
		sha256Hasher.Write(buf)
		binary.BigEndian.PutUint64(buf, uint64(columnType.GetFlen()))
		sha256Hasher.Write(buf)
		binary.BigEndian.PutUint64(buf, uint64(columnType.GetDecimal()))
		sha256Hasher.Write(buf)
		sha256Hasher.Write([]byte(columnType.GetCharset()))
		sha256Hasher.Write([]byte(columnType.GetCollate()))
		elems := columnType.GetElems()
		binary.BigEndian.PutUint64(buf, uint64(len(elems)))
		sha256Hasher.Write(buf)
		for idx, elem := range elems {
			sha256Hasher.Write([]byte(elem))
			binaryLit := columnType.GetElemIsBinaryLit(idx)
			binary.BigEndian.PutUint64(buf, uint64(boolToInt(binaryLit)))
			sha256Hasher.Write(buf)
		}
		binary.BigEndian.PutUint64(buf, uint64(boolToInt(columnType.IsArray())))
		sha256Hasher.Write(buf)
	}
	// idx info
	sha256Hasher.Write([]byte("idxInfo"))
	binary.BigEndian.PutUint64(buf, uint64(len(tableInfo.Indices)))
	for _, idx := range tableInfo.Indices {
		// ID
		binary.BigEndian.PutUint64(buf, uint64(idx.ID))
		sha256Hasher.Write(buf)
		// columns offset
		binary.BigEndian.PutUint64(buf, uint64(len(idx.Columns)))
		sha256Hasher.Write(buf)
		for _, col := range idx.Columns {
			binary.BigEndian.PutUint64(buf, uint64(col.Offset))
			sha256Hasher.Write(buf)
		}
		// unique
		binary.BigEndian.PutUint64(buf, uint64(boolToInt(idx.Unique)))
		sha256Hasher.Write(buf)
		// primary
		binary.BigEndian.PutUint64(buf, uint64(boolToInt(idx.Primary)))
		sha256Hasher.Write(buf)
	}
	hash := sha256Hasher.Sum(nil)
	return ToDigest(hash)
}

var once sync.Once
var storage *SharedColumnSchemaStorage

func GetSharedColumnSchemaStorage() *SharedColumnSchemaStorage {
	once.Do(func() {
		storage = &SharedColumnSchemaStorage{
			m: make(map[Digest][]ColumnSchemaWithTableInfo),
		}
	})
	return storage
}

type ColumnSchemaWithTableInfo struct {
	*ColumnSchema
	*model.TableInfo
}

type SharedColumnSchemaStorage struct {
	// For the table have the same column schema, we will use the same ColumnSchema object to reduce memory usage.
	// we use a map to store the ColumnSchema object(in ColumnSchemaWithTableInfo),
	// the key is the hash value of the Column Info of the table info.
	// We use SHA-256 to calculate the hash value to reduce the collision probability.
	// However, there may still have some collisions in some cases,
	// so we use a list to store the ColumnSchemaWithTableInfo object with the same hash value.
	// ColumnSchemaWithTableInfo contains the ColumnSchema and TableInfo,
	// we can compare the TableInfo to check whether the column schema is the same.
	// If not the same, we will create a new ColumnSchema object and append it to the list.
	m     map[Digest][]ColumnSchemaWithTableInfo
	mutex sync.Mutex
}

// compare the item calculated in hashTableInfo
func compareSchemaInfoOfTableInfo(originTableInfo *model.TableInfo, newTableInfo *model.TableInfo) bool {
	if len(originTableInfo.Columns) != len(newTableInfo.Columns) {
		return false
	}

	for i, col := range originTableInfo.Columns {
		if col.Name.O != newTableInfo.Columns[i].Name.O {
			return false
		}
		if !col.FieldType.Equal(&newTableInfo.Columns[i].FieldType) {
			return false
		}
		if col.ID != newTableInfo.Columns[i].ID {
			return false
		}
	}

	if len(originTableInfo.Indices) != len(newTableInfo.Indices) {
		return false
	}

	for i, idx := range originTableInfo.Indices {
		if idx.ID != newTableInfo.Indices[i].ID {
			return false
		}
		if len(idx.Columns) != len(newTableInfo.Indices[i].Columns) {
			return false
		}
		for j, col := range idx.Columns {
			if col.Offset != newTableInfo.Indices[i].Columns[j].Offset {
				return false
			}
		}
		if idx.Unique != newTableInfo.Indices[i].Unique {
			return false
		}
		if idx.Primary != newTableInfo.Indices[i].Primary {
			return false
		}
	}
	return true
}

func (s *SharedColumnSchemaStorage) IncColumnSchemaCount(tableInfo *TableInfo)

func (s *SharedColumnSchemaStorage) GetOrSetColumnSchema(tableInfo *model.TableInfo) *ColumnSchema {
	digest := hashTableInfo(tableInfo)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	colSchemas, ok := s.m[digest]
	if !ok {
		// generate Column Schema
		columnSchema := NewColumnSchema(tableInfo)
		s.m[digest] = make([]ColumnSchemaWithTableInfo, 1)
		s.m[digest][0] = ColumnSchemaWithTableInfo{columnSchema, tableInfo}
		return columnSchema
	} else {
		for _, colSchemaWithTableInfo := range colSchemas {
			// compare tableInfo to check whether the column schema is the same
			if compareSchemaInfoOfTableInfo(colSchemaWithTableInfo.TableInfo, tableInfo) {
				return colSchemaWithTableInfo.ColumnSchema
			}
		}
		// not found the same column info, create a new one
		columnSchema := NewColumnSchema(tableInfo)
		s.m[digest] = append(s.m[digest], ColumnSchemaWithTableInfo{columnSchema, tableInfo})
		return columnSchema
	}
}

// ColumnSchema is used to store the column schema information of tableInfo.
// ColumnSchema is shared across multiple tableInfos with the same schema, in order to reduce memory usage.
type ColumnSchema struct {
	// These fields are copied from model.TableInfo.
	// Version means the version of the table info.
	Version uint16 `json:"version"`
	// Columns are listed in the order in which they appear in the schema
	Columns []*model.ColumnInfo `json:"cols"`
	Indices []*model.IndexInfo  `json:"index_info"`
	// PKIsHandle is true when primary key is a single integer column.
	PKIsHandle bool `json:"pk_is_handle"`
	// IsCommonHandle is true when clustered index feature is
	// enabled and the primary key is not a single integer column.
	IsCommonHandle bool `json:"is_common_handle"`
	// UpdateTS is used to record the timestamp of updating the table's schema information.
	// These changing schema operations don't include 'truncate table' and 'rename table'.
	UpdateTS uint64 `json:"update_timestamp"`

	// rest fields are generated
	// ColumnID -> offset in model.TableInfo.Columns
	ColumnsOffset map[int64]int `json:"columns_offset"`
	// Column name -> ColumnID
	NameToColID map[string]int64 `json:"name_to_col_id"`

	HasUniqueColumn bool `json:"has_unique_column"`

	// ColumnID -> offset in RowChangedEvents.Columns.
	RowColumnsOffset map[int64]int `json:"row_columns_offset"`

	ColumnsFlag map[int64]*ColumnFlagType `json:"columns_flag"`

	// the mounter will choose this index to output delete events
	// special value:
	// HandleIndexPKIsHandle(-1) : pk is handle
	// HandleIndexTableIneligible(-2) : the table is not eligible
	HandleIndexID int64 `json:"handle_index_id"`

	// IndexColumnsOffset store the offset of the columns in row changed events for
	// unique index and primary key
	// The reason why we need this is that the Indexes in TableInfo
	// will not contain the PK if it is create in statement like:
	// create table t (a int primary key, b int unique key);
	// Every element in first dimension is a index, and the second dimension is the columns offset
	// for example:
	// table has 3 columns: a, b, c
	// pk: a
	// index1: a, b
	// index2: a, c
	// indexColumnsOffset: [[0], [0, 1], [0, 2]]
	IndexColumnsOffset [][]int `json:"index_columns_offset"`

	// The following 3 fields, should only be used to decode datum from the raw value bytes, do not abuse those field.
	// RowColInfos extend the model.ColumnInfo with some extra information
	// it's the same length and order with the model.TableInfo.Columns
	RowColInfos    []rowcodec.ColInfo              `json:"row_col_infos"`
	RowColFieldTps map[int64]*datumTypes.FieldType `json:"row_col_field_tps"`
	// only for new row format decoder
	HandleColID []int64 `json:"handle_col_id"`
	// RowColFieldTpsSlice is used to decode chunk ∂ raw value bytes
	RowColFieldTpsSlice []*datumTypes.FieldType `json:"row_col_field_tps_slice"`

	// number of virtual columns
	VirtualColumnCount int `json:"virtual_column_count"`
	// RowColInfosWithoutVirtualCols is the same as rowColInfos, but without virtual columns
	RowColInfosWithoutVirtualCols *[]rowcodec.ColInfo `json:"row_col_infos_without_virtual_cols"`
}

func NewColumnSchema(tableInfo *model.TableInfo) *ColumnSchema {
	colSchema := &ColumnSchema{
		Version:          tableInfo.Version,
		Columns:          tableInfo.Columns,
		Indices:          tableInfo.Indices,
		PKIsHandle:       tableInfo.PKIsHandle,
		IsCommonHandle:   tableInfo.IsCommonHandle,
		UpdateTS:         tableInfo.UpdateTS,
		HasUniqueColumn:  false,
		ColumnsOffset:    make(map[int64]int, len(tableInfo.Columns)),
		NameToColID:      make(map[string]int64, len(tableInfo.Columns)),
		RowColumnsOffset: make(map[int64]int, len(tableInfo.Columns)),
		ColumnsFlag:      make(map[int64]*ColumnFlagType, len(tableInfo.Columns)),
		HandleColID:      []int64{-1},
		HandleIndexID:    HandleIndexTableIneligible,
		RowColInfos:      make([]rowcodec.ColInfo, len(tableInfo.Columns)),
		RowColFieldTps:   make(map[int64]*datumTypes.FieldType, len(tableInfo.Columns)),
	}

	rowColumnsCurrentOffset := 0

	colSchema.VirtualColumnCount = 0
	for i, col := range colSchema.Columns {
		colSchema.ColumnsOffset[col.ID] = i
		pkIsHandle := false
		if IsColCDCVisible(col) {
			colSchema.NameToColID[col.Name.O] = col.ID
			colSchema.RowColumnsOffset[col.ID] = rowColumnsCurrentOffset
			rowColumnsCurrentOffset++
			pkIsHandle = (tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag())) || col.ID == model.ExtraHandleID
			if pkIsHandle {
				// pk is handle
				colSchema.HandleColID = []int64{col.ID}
				colSchema.HandleIndexID = HandleIndexPKIsHandle
				colSchema.HasUniqueColumn = true
				colSchema.IndexColumnsOffset = append(colSchema.IndexColumnsOffset, []int{colSchema.RowColumnsOffset[col.ID]})
			} else if tableInfo.IsCommonHandle {
				colSchema.HandleIndexID = HandleIndexPKIsHandle
				colSchema.HandleColID = colSchema.HandleColID[:0]
				pkIdx := tables.FindPrimaryIndex(tableInfo)
				for _, pkCol := range pkIdx.Columns {
					id := tableInfo.Columns[pkCol.Offset].ID
					colSchema.HandleColID = append(colSchema.HandleColID, id)
				}
			}
		} else {
			colSchema.VirtualColumnCount += 1
		}
		colSchema.RowColInfos[i] = rowcodec.ColInfo{
			ID:            col.ID,
			IsPKHandle:    pkIsHandle,
			Ft:            col.FieldType.Clone(),
			VirtualGenCol: col.IsGenerated(),
		}
		colSchema.RowColFieldTps[col.ID] = colSchema.RowColInfos[i].Ft
		colSchema.RowColFieldTpsSlice = append(colSchema.RowColFieldTpsSlice, colSchema.RowColInfos[i].Ft)
	}

	for _, idx := range colSchema.Indices {
		if IsIndexUniqueAndNotNull(tableInfo, idx) {
			colSchema.HasUniqueColumn = true
		}
		if idx.Primary || idx.Unique {
			indexColOffset := make([]int, 0, len(idx.Columns))
			for _, idxCol := range idx.Columns {
				colInfo := tableInfo.Columns[idxCol.Offset]
				if IsColCDCVisible(colInfo) {
					indexColOffset = append(indexColOffset, colSchema.RowColumnsOffset[colInfo.ID])
				}
			}
			if len(indexColOffset) > 0 {
				colSchema.IndexColumnsOffset = append(colSchema.IndexColumnsOffset, indexColOffset)
			}
		}
	}
	colSchema.initRowColInfosWithoutVirtualCols()
	colSchema.findHandleIndex(tableInfo)
	colSchema.initColumnsFlag()

	return colSchema
}

// GetPkColInfo gets the ColumnInfo of pk if exists.
// Make sure PkIsHandle checked before call this method.
func (s *ColumnSchema) GetPkColInfo() *model.ColumnInfo {
	for _, colInfo := range s.Columns {
		if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			return colInfo
		}
	}
	return nil
}

// Cols returns the columns of the table in public state.
func (s *ColumnSchema) Cols() []*model.ColumnInfo {
	publicColumns := make([]*model.ColumnInfo, len(s.Columns))
	maxOffset := -1
	for _, col := range s.Columns {
		if col.State != model.StatePublic {
			continue
		}
		publicColumns[col.Offset] = col
		if maxOffset < col.Offset {
			maxOffset = col.Offset
		}
	}
	return publicColumns[0 : maxOffset+1]
}

// GetPrimaryKey extract the primary key in a table and return `IndexInfo`
// The returned primary key could be explicit or implicit.
// If there is no explicit primary key in table,
// the first UNIQUE INDEX on NOT NULL columns will be the implicit primary key.
// For more information about implicit primary key, see
// https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html
func (s *ColumnSchema) GetPrimaryKey() *model.IndexInfo {
	var implicitPK *model.IndexInfo

	for _, key := range s.Indices {
		if key.Primary {
			// table has explicit primary key
			return key
		}
		// The case index without any columns should never happen, but still do a check here
		if len(key.Columns) == 0 {
			continue
		}
		// find the first unique key with NOT NULL columns
		if implicitPK == nil && key.Unique {
			// ensure all columns in unique key have NOT NULL flag
			allColNotNull := true
			skip := false
			for _, idxCol := range key.Columns {
				col := model.FindColumnInfo(s.Cols(), idxCol.Name.L)
				// This index has a column in DeleteOnly state,
				// or it is expression index (it defined on a hidden column),
				// it can not be implicit PK, go to next index iterator
				if col == nil || col.Hidden {
					skip = true
					break
				}
				if !mysql.HasNotNullFlag(col.GetFlag()) {
					allColNotNull = false
					break
				}
			}
			if skip {
				continue
			}
			if allColNotNull {
				implicitPK = key
			}
		}
	}
	return implicitPK
}

func (s *ColumnSchema) initRowColInfosWithoutVirtualCols() {
	if s.VirtualColumnCount == 0 {
		s.RowColInfosWithoutVirtualCols = &s.RowColInfos
		return
	}
	colInfos := make([]rowcodec.ColInfo, 0, len(s.RowColInfos)-s.VirtualColumnCount)
	for i, col := range s.Columns {
		if IsColCDCVisible(col) {
			colInfos = append(colInfos, s.RowColInfos[i])
		}
	}
	if len(colInfos) != len(s.RowColInfos)-s.VirtualColumnCount {
		log.Panic("invalid rowColInfosWithoutVirtualCols",
			zap.Int("len(colInfos)", len(colInfos)),
			zap.Int("len(ti.rowColInfos)", len(s.RowColInfos)),
			zap.Int("ti.virtualColumnCount", s.VirtualColumnCount))
	}
	s.RowColInfosWithoutVirtualCols = &colInfos
}

func (s *ColumnSchema) findHandleIndex(tableInfo *model.TableInfo) {
	if s.HandleIndexID == HandleIndexPKIsHandle {
		// pk is handle
		return
	}
	handleIndexOffset := -1
	for i, idx := range s.Indices {
		if !IsIndexUniqueAndNotNull(tableInfo, idx) {
			continue
		}
		if idx.Primary {
			handleIndexOffset = i
			break
		}
		if handleIndexOffset < 0 {
			handleIndexOffset = i
		} else {
			if len(s.Indices[handleIndexOffset].Columns) > len(s.Indices[i].Columns) ||
				(len(s.Indices[handleIndexOffset].Columns) == len(s.Indices[i].Columns) &&
					s.Indices[handleIndexOffset].ID > s.Indices[i].ID) {
				handleIndexOffset = i
			}
		}
	}
	if handleIndexOffset >= 0 {
		log.Info("find handle index", zap.String("table", tableInfo.Name.O), zap.String("index", s.Indices[handleIndexOffset].Name.O))
		s.HandleIndexID = s.Indices[handleIndexOffset].ID
	}
}

func (s *ColumnSchema) initColumnsFlag() {
	for _, colInfo := range s.Columns {
		var flag ColumnFlagType
		if colInfo.GetCharset() == "binary" {
			flag.SetIsBinary()
		}
		if colInfo.IsGenerated() {
			flag.SetIsGeneratedColumn()
		}
		if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			flag.SetIsPrimaryKey()
			if s.HandleIndexID == HandleIndexPKIsHandle {
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
		s.ColumnsFlag[colInfo.ID] = &flag
	}

	// In TiDB, just as in MySQL, only the first column of an index can be marked as "multiple key" or "unique key",
	// and only the first column of a unique index may be marked as "unique key".
	// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html.
	// Yet if an index has multiple columns, we would like to easily determine that all those columns are indexed,
	// which is crucial for the completeness of the information we pass to the downstream.
	// Therefore, instead of using the MySQL standard,
	// we made our own decision to mark all columns in an index with the appropriate flag(s).
	for _, idxInfo := range s.Indices {
		for _, idxCol := range idxInfo.Columns {
			colInfo := s.Columns[idxCol.Offset]
			flag := s.ColumnsFlag[colInfo.ID]
			if idxInfo.Primary {
				flag.SetIsPrimaryKey()
			} else if idxInfo.Unique {
				flag.SetIsUniqueKey()
			}
			if len(idxInfo.Columns) > 1 {
				flag.SetIsMultipleKey()
			}
			if idxInfo.ID == s.HandleIndexID && s.HandleIndexID >= 0 {
				flag.SetIsHandleKey()
			}
			s.ColumnsFlag[colInfo.ID] = flag
		}
	}
}

// IsIndexUnique returns whether the index is unique and all columns are not null
func IsIndexUniqueAndNotNull(tableInfo *model.TableInfo, indexInfo *model.IndexInfo) bool {
	if indexInfo.Primary {
		return true
	}
	if indexInfo.Unique {
		for _, col := range indexInfo.Columns {
			colInfo := tableInfo.Columns[col.Offset]
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

// TryGetCommonPkColumnIds get the IDs of primary key column if the table has common handle.
func TryGetCommonPkColumnIds(columnSchema *ColumnSchema) []int64 {
	if !columnSchema.IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(columnSchema)
	pkColIDs := make([]int64, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkColIDs = append(pkColIDs, columnSchema.Columns[idxCol.Offset].ID)
	}
	return pkColIDs
}

// FindPrimaryIndex uses to find primary index in tableInfo.
func FindPrimaryIndex(columnSchema *ColumnSchema) *model.IndexInfo {
	var pkIdx *model.IndexInfo
	for _, idx := range columnSchema.Indices {
		if idx.Primary {
			pkIdx = idx
			break
		}
	}
	return pkIdx
}

// PrimaryPrefixColumnIDs get prefix column ids in primary key.
func PrimaryPrefixColumnIDs(columnSchema *ColumnSchema) (prefixCols []int64) {
	for _, idx := range columnSchema.Indices {
		if !idx.Primary {
			continue
		}
		for _, col := range idx.Columns {
			if col.Length > 0 && columnSchema.Columns[col.Offset].GetFlen() > col.Length {
				prefixCols = append(prefixCols, columnSchema.Columns[col.Offset].ID)
			}
		}
	}
	return
}
