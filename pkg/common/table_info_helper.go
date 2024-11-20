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
	// use ok to represent the digest is valid or not
	// if valid, means the ColumnSchema with digest use the columnSchema in SharedColumnSchemaStorage
	// otherwise, verse versa.
	ok bool
}

func ToDigest(b []byte) Digest {
	return Digest{
		a:  binary.BigEndian.Uint64(b[0:8]),
		b:  binary.BigEndian.Uint64(b[8:16]),
		c:  binary.BigEndian.Uint64(b[16:24]),
		d:  binary.BigEndian.Uint64(b[24:32]),
		ok: true,
	}
}

func (d Digest) Clear() {
	d.ok = false
}

func (d Digest) valid() bool {
	return d.ok
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
	count int // reference count
}

func NewColumnSchemaWithTableInfo(columnSchema *ColumnSchema, tableInfo *model.TableInfo) *ColumnSchemaWithTableInfo {
	return &ColumnSchemaWithTableInfo{
		ColumnSchema: columnSchema,
		TableInfo:    tableInfo,
		count:        1,
	}
}

type SharedColumnSchemaStorage struct {
	// For the table have the same column schema, we will use the same ColumnSchema object to reduce memory usage.
	// we use a map to store the ColumnSchema object(in ColumnSchemaWithTableInfo),
	// the key is the hash value of the Column Info of the table info.
	// We use SHA-256 to calculate the hash value to reduce the collision probability.
	// However, there may still have some collisions in some cases,
	// so we use a list to store the ColumnSchemaWithTableInfo object with the same hash value.
	// ColumnSchemaWithTableInfo contains the ColumnSchema and TableInfo, and a reference count.
	// we can compare the TableInfo to check whether the column schema is the same.
	// If not the same, we will create a new ColumnSchema object and append it to the list.
	// The reference count is used to check whether the ColumnSchema object can be released.
	// If the reference count is 0, we can release the ColumnSchema object.
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

func (s *SharedColumnSchemaStorage) GetOrSetColumnSchema(tableInfo *model.TableInfo) *ColumnSchema {
	digest := hashTableInfo(tableInfo)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	colSchemas, ok := s.m[digest]
	if !ok {
		// generate Column Schema
		columnSchema := NewColumnSchema(tableInfo, digest)
		s.m[digest] = make([]ColumnSchemaWithTableInfo, 1)
		s.m[digest][0] = *NewColumnSchemaWithTableInfo(columnSchema, tableInfo)
		return columnSchema
	} else {
		for idx, colSchemaWithTableInfo := range colSchemas {
			// compare tableInfo to check whether the column schema is the same
			if compareSchemaInfoOfTableInfo(colSchemaWithTableInfo.TableInfo, tableInfo) {
				s.m[digest][idx].count++
				return colSchemaWithTableInfo.ColumnSchema
			}
		}
		// not found the same column info, create a new one
		columnSchema := NewColumnSchema(tableInfo, digest)
		s.m[digest] = append(s.m[digest], *NewColumnSchemaWithTableInfo(columnSchema, tableInfo))
		return columnSchema
	}
}

func (s *SharedColumnSchemaStorage) Len() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	count := 0
	for _, colSchemas := range s.m {
		count += len(colSchemas)
	}
	return count
}

// we call this function when each TableInfo with valid digest is released.
// we decrease the reference count of the ColumnSchema object,
// if the reference count is 0, we can release the ColumnSchema object.
// the release of TableInfo will happens in the following scenarios:
//  1. when the ddlEvent sent to event collector by event service, if they are not in the same node, mc will Marshal the ddlEvent to bytes and send to other node.
//     Thus, after Marshal, this ddlEvent is released, the same as the TableInfo.
//  2. when the dispatcher receive the next ddlEvent, it will catch the new tableInfo, and release the old one. Thus the old tableInfo is released.
//  3. when the ddlEvent flushed successfully, the TableInfo is released.
//     However, the tableInfo is shared with dispatcher, and dispatcher always release later, so we don't need to deal here.
//  4. versionedTableInfo gc will release some tableInfo.
func (s *SharedColumnSchemaStorage) TryReleaseColumnSchema(columnSchema *ColumnSchema) {
	if !columnSchema.Digest.valid() {
		return
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	colSchemas, ok := s.m[columnSchema.Digest]
	if !ok {
		log.Warn("try release column schema failed, column schema not found", zap.Any("columnSchema", columnSchema))
		return
	}
	for idx, colSchemaWithTableInfo := range colSchemas {
		if colSchemaWithTableInfo.ColumnSchema == columnSchema {
			s.m[columnSchema.Digest][idx].count--
			if s.m[columnSchema.Digest][idx].count == 0 {
				// release the ColumnSchema object
				SharedColumnSchemaCountGauge.Dec()
				s.m[columnSchema.Digest] = append(s.m[columnSchema.Digest][:idx], s.m[columnSchema.Digest][idx+1:]...)
				if len(s.m[columnSchema.Digest]) == 0 {
					delete(s.m, columnSchema.Digest)
				}
			}
		}
	}
}

// ColumnSchema is used to store the column schema information of tableInfo.
// ColumnSchema is shared across multiple tableInfos with the same schema, in order to reduce memory usage.
type ColumnSchema struct {
	// digest of the table info
	Digest Digest `json:"digest"`
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

func NewColumnSchema(tableInfo *model.TableInfo, digest Digest) *ColumnSchema {
	log.Info("create new column schema", zap.Any("tableInfo", tableInfo))
	colSchema := &ColumnSchema{
		Digest:           digest,
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
	for i, col := range tableInfo.Columns {
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

	for _, idx := range tableInfo.Indices {
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
	colSchema.initRowColInfosWithoutVirtualCols(tableInfo)
	colSchema.findHandleIndex(tableInfo)
	colSchema.initColumnsFlag(tableInfo)

	SharedColumnSchemaCountGauge.Inc()
	return colSchema
}

func (s *ColumnSchema) initRowColInfosWithoutVirtualCols(tableInfo *model.TableInfo) {
	if s.VirtualColumnCount == 0 {
		s.RowColInfosWithoutVirtualCols = &s.RowColInfos
		return
	}
	colInfos := make([]rowcodec.ColInfo, 0, len(s.RowColInfos)-s.VirtualColumnCount)
	for i, col := range tableInfo.Columns {
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
	for i, idx := range tableInfo.Indices {
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
			if len(tableInfo.Indices[handleIndexOffset].Columns) > len(tableInfo.Indices[i].Columns) ||
				(len(tableInfo.Indices[handleIndexOffset].Columns) == len(tableInfo.Indices[i].Columns) &&
					tableInfo.Indices[handleIndexOffset].ID > tableInfo.Indices[i].ID) {
				handleIndexOffset = i
			}
		}
	}
	if handleIndexOffset >= 0 {
		log.Info("find handle index", zap.String("table", tableInfo.Name.O), zap.String("index", tableInfo.Indices[handleIndexOffset].Name.O))
		s.HandleIndexID = tableInfo.Indices[handleIndexOffset].ID
	}
}

func (s *ColumnSchema) initColumnsFlag(tableInfo *model.TableInfo) {
	for _, colInfo := range tableInfo.Columns {
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
	for _, idxInfo := range tableInfo.Indices {
		for _, idxCol := range idxInfo.Columns {
			colInfo := tableInfo.Columns[idxCol.Offset]
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
