package common

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	datumTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
)

// hash representation for columnSchema of TableInfo
// Considering sha-256 output is 32 bytes, we use 4 uint64 to represent the hash value.
type Digest struct {
	a uint64
	b uint64
	c uint64
	d uint64
	// use ok to represent the digest is valid or not
	// if valid, means the columnSchema with digest use the columnSchema in SharedColumnSchemaStorage
	// otherwise, verse versa.
	ok bool
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
			m: make(map[Digest][]ColumnSchemaWithCount),
		}
	})
	return storage
}

type ColumnSchemaWithCount struct {
	*columnSchema
	count int // reference count
}

func NewColumnSchemaWithCount(columnSchema *columnSchema) *ColumnSchemaWithCount {
	return &ColumnSchemaWithCount{
		columnSchema: columnSchema,
		count:        1,
	}
}

type SharedColumnSchemaStorage struct {
	// For the table have the same column schema, we will use the same columnSchema object to reduce memory usage.
	// we use a map to store the columnSchema object(in ColumnSchemaWithTableInfo),
	// the key is the hash value of the Column Info of the table info.
	// We use SHA-256 to calculate the hash value to reduce the collision probability.
	// However, there may still have some collisions in some cases,
	// so we use a list to store the ColumnSchemaWithCount object with the same hash value.
	// ColumnSchemaWithCount contains the columnSchema and a reference count.
	// The reference count is used to check whether the columnSchema object can be released.
	// If the reference count is 0, we can release the columnSchema object.
	m     map[Digest][]ColumnSchemaWithCount
	mutex sync.Mutex
}

func (s *columnSchema) sameColumnsAndIndices(columns []*model.ColumnInfo, indices []*model.IndexInfo) bool {
	if len(s.Columns) != len(columns) {
		return false
	}

	for i, col := range s.Columns {
		if col.Name.O != columns[i].Name.O {
			return false
		}
		if !col.FieldType.Equal(&columns[i].FieldType) {
			return false
		}
		if col.ID != columns[i].ID {
			return false
		}
	}

	if len(s.Indices) != len(indices) {
		return false
	}

	for i, idx := range s.Indices {
		if idx.ID != indices[i].ID {
			return false
		}
		if len(idx.Columns) != len(indices[i].Columns) {
			return false
		}
		for j, col := range idx.Columns {
			if col.Offset != indices[i].Columns[j].Offset {
				return false
			}
		}
		if idx.Unique != indices[i].Unique {
			return false
		}
		if idx.Primary != indices[i].Primary {
			return false
		}
	}
	return true
}

func (s *columnSchema) SameWithTableInfo(tableInfo *model.TableInfo) bool {
	return s.sameColumnsAndIndices(tableInfo.Columns, tableInfo.Indices)
}

// compare the item calculated in hashTableInfo
func (s *columnSchema) Equal(columnSchema *columnSchema) bool {
	return s.sameColumnsAndIndices(columnSchema.Columns, columnSchema.Indices)
}

func (s *SharedColumnSchemaStorage) incColumnSchemaCount(columnSchema *columnSchema) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	colSchemas, ok := s.m[columnSchema.Digest]
	if !ok {
		log.Error("inc column schema count failed, column schema not found", zap.Any("columnSchema", columnSchema))
	}
	for idx, colSchemaWithCount := range colSchemas {
		if colSchemaWithCount.columnSchema.Equal(columnSchema) {
			s.m[columnSchema.Digest][idx].count++
			return
		}
	}
	if !ok {
		log.Error("inc column schema count failed, column schema not found", zap.Any("columnSchema", columnSchema))
	}
}

// we only should get ColumnSchema By GetOrSetColumnSchema.
// For the object which get columnSchema by this function, we need to set finalizer to ask
// when the object is released, we should call tryReleaseColumnSchema to decrease the reference count of the columnSchema object
// to ensure the gc for column schema.
//
//	eg. runtime.SetFinalizer(ti, func(ti *TableInfo) {
//	    	GetSharedColumnSchemaStorage().tryReleaseColumnSchema(ti.ColumnSchema)
//	   })
func (s *SharedColumnSchemaStorage) GetOrSetColumnSchema(tableInfo *model.TableInfo) *columnSchema {
	digest := hashTableInfo(tableInfo)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	colSchemas, ok := s.m[digest]
	if !ok {
		// generate Column Schema
		columnSchema := newColumnSchema(tableInfo, digest)
		s.m[digest] = make([]ColumnSchemaWithCount, 1)
		s.m[digest][0] = *NewColumnSchemaWithCount(columnSchema)
		return columnSchema
	} else {
		for idx, colSchemaWithCount := range colSchemas {
			// compare tableInfo to check whether the column schema is the same
			if colSchemaWithCount.columnSchema.SameWithTableInfo(tableInfo) {
				s.m[digest][idx].count++
				log.Info("hyy into get or set column schema, find the column schema", zap.Any("count", colSchemaWithCount.count))
				return colSchemaWithCount.columnSchema
			}
		}
		// not found the same column info, create a new one
		columnSchema := newColumnSchema(tableInfo, digest)
		s.m[digest] = append(s.m[digest], *NewColumnSchemaWithCount(columnSchema))
		return columnSchema
	}
}

// This function is used after unmarshal, we need to get shared column schema by the unmarshal result to avoid inused-object.
func (s *SharedColumnSchemaStorage) getOrSetColumnSchemaByColumnSchema(columnSchema *columnSchema) *columnSchema {
	digest := columnSchema.Digest
	s.mutex.Lock()
	defer s.mutex.Unlock()
	colSchemas, ok := s.m[digest]
	if !ok {
		s.m[digest] = make([]ColumnSchemaWithCount, 1)
		s.m[digest][0] = *NewColumnSchemaWithCount(columnSchema)
		return columnSchema
	} else {
		for idx, colSchemaWithCount := range colSchemas {
			// compare tableInfo to check whether the column schema is the same
			if colSchemaWithCount.columnSchema.Equal(columnSchema) {
				s.m[digest][idx].count++
				return colSchemaWithCount.columnSchema
			}
		}
		// not found the same column info, add a new one
		s.m[digest] = append(s.m[digest], *NewColumnSchemaWithCount(columnSchema))
		return columnSchema
	}
}

// we call this function when each TableInfo is released,
// we decrease the reference count of the columnSchema object,
// if the reference count is 0, we can release the columnSchema object.
// the release of TableInfo will happens in the following scenarios:
//  1. when the ddlEvent sent to event collector by event service, if they are not in the same node, mc will Marshal the ddlEvent to bytes and send to other node.
//     Thus, after Marshal, this ddlEvent is released, the same as the TableInfo.
//  2. when the dispatcher receive the next ddlEvent, it will catch the new tableInfo, and release the old one. Thus the old tableInfo is released.
//  3. when the ddlEvent flushed successfully, the TableInfo is released.
//     However, the tableInfo is shared with dispatcher, and dispatcher always release later, so we don't need to deal here.
//  4. versionedTableInfo gc will release some tableInfo.
func (s *SharedColumnSchemaStorage) tryReleaseColumnSchema(columnSchema *columnSchema) {
	log.Info("hyy into try release column schema")
	s.mutex.Lock()
	defer s.mutex.Unlock()
	colSchemas, ok := s.m[columnSchema.Digest]
	if !ok {
		log.Warn("try release column schema failed, column schema not found", zap.Any("columnSchema", columnSchema))
		return
	}
	for idx, colSchemaWithCount := range colSchemas {
		if colSchemaWithCount.columnSchema == columnSchema {
			log.Info("hyy into try release column schema, find the column schema", zap.Any("count", colSchemaWithCount.count))
			s.m[columnSchema.Digest][idx].count--
			if s.m[columnSchema.Digest][idx].count == 0 {
				// release the columnSchema object
				SharedColumnSchemaCountGauge.Dec()
				s.m[columnSchema.Digest] = append(s.m[columnSchema.Digest][:idx], s.m[columnSchema.Digest][idx+1:]...)
				if len(s.m[columnSchema.Digest]) == 0 {
					delete(s.m, columnSchema.Digest)
				}
			}
		}
	}
}

// columnSchema is used to store the column schema information of tableInfo.
// columnSchema is shared across multiple tableInfos with the same schema, in order to reduce memory usage.
// we make columnSchema as a private struct, in order to avoid other method to directly create a columnSchema object.
// we only want user to get columnSchema by GetOrSetColumnSchema or Clone method.
type columnSchema struct {
	// digest of the table info
	Digest Digest `json:"digest"`

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

func (s *columnSchema) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// If you want to copy columnSchema(shaddow copy), you should use Clone method.
// This function will increase the reference count of the columnSchema object.
func (s *columnSchema) Clone() *columnSchema {
	GetSharedColumnSchemaStorage().incColumnSchemaCount(s)
	return s
}

// TODO: we can optimize the method, to first unmarshal part of columnSchema to ensure whether there have a shared column schema.
func unmarshalJsonToColumnSchema(data []byte) (*columnSchema, error) {
	var colSchema columnSchema
	err := json.Unmarshal(data, &colSchema)
	if err != nil {
		return nil, err
	}

	sharedColumnSchema := GetSharedColumnSchemaStorage().getOrSetColumnSchemaByColumnSchema(&colSchema)
	return sharedColumnSchema, nil
}

// make newColumnSchema as a private method, in order to avoid other method to directly create a columnSchema object.
// we only want user to get columnSchema by GetOrSetColumnSchema or Clone method.
func newColumnSchema(tableInfo *model.TableInfo, digest Digest) *columnSchema {
	colSchema := &columnSchema{
		Digest:           digest,
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
		if colSchema.IsIndexUniqueAndNotNull(idx) {
			colSchema.HasUniqueColumn = true
		}
		if idx.Primary || idx.Unique {
			indexColOffset := make([]int, 0, len(idx.Columns))
			for _, idxCol := range idx.Columns {
				colInfo := colSchema.Columns[idxCol.Offset]
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
	colSchema.findHandleIndex(tableInfo.Name.O)
	colSchema.initColumnsFlag()

	SharedColumnSchemaCountGauge.Inc()
	return colSchema
}

// GetPkColInfo gets the ColumnInfo of pk if exists.
// Make sure PkIsHandle checked before call this method.
func (s *columnSchema) GetPkColInfo() *model.ColumnInfo {
	for _, colInfo := range s.Columns {
		if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			return colInfo
		}
	}
	return nil
}

// Cols returns the columns of the table in public state.
func (s *columnSchema) Cols() []*model.ColumnInfo {
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
func (s *columnSchema) GetPrimaryKey() *model.IndexInfo {
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

func (s *columnSchema) initRowColInfosWithoutVirtualCols() {
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

func (s *columnSchema) findHandleIndex(tableName string) {
	if s.HandleIndexID == HandleIndexPKIsHandle {
		// pk is handle
		return
	}
	handleIndexOffset := -1
	for i, idx := range s.Indices {
		if !s.IsIndexUniqueAndNotNull(idx) {
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
		log.Info("find handle index", zap.String("table", tableName), zap.String("index", s.Indices[handleIndexOffset].Name.O))
		s.HandleIndexID = s.Indices[handleIndexOffset].ID
	}
}

func (s *columnSchema) initColumnsFlag() {
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
func (s *columnSchema) IsIndexUniqueAndNotNull(indexInfo *model.IndexInfo) bool {
	if indexInfo.Primary {
		return true
	}
	if indexInfo.Unique {
		for _, col := range indexInfo.Columns {
			colInfo := s.Columns[col.Offset]
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
func TryGetCommonPkColumnIds(tableInfo *TableInfo) []int64 {
	if !tableInfo.columnSchema.IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tableInfo.columnSchema)
	pkColIDs := make([]int64, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkColIDs = append(pkColIDs, tableInfo.columnSchema.Columns[idxCol.Offset].ID)
	}
	return pkColIDs
}

// FindPrimaryIndex uses to find primary index in columnSchema.
func FindPrimaryIndex(columnSchema *columnSchema) *model.IndexInfo {
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
func PrimaryPrefixColumnIDs(tableInfo *TableInfo) (prefixCols []int64) {
	for _, idx := range tableInfo.columnSchema.Indices {
		if !idx.Primary {
			continue
		}
		for _, col := range idx.Columns {
			if col.Length > 0 && tableInfo.columnSchema.Columns[col.Offset].GetFlen() > col.Length {
				prefixCols = append(prefixCols, tableInfo.columnSchema.Columns[col.Offset].ID)
			}
		}
	}
	return
}
