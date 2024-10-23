package event

import (
	"encoding/binary"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const (
	// defaultRowCount is the start row count of a transaction.
	defaultRowCount = 1
	// DMLEventVersion is the version of the DMLEvent struct.
	DMLEventVersion = 0
)

// DMLEvent represent a batch of DMLs of a whole or partial of a transaction.
type DMLEvent struct {
	// Version is the version of the DMLEvent struct.
	Version         byte                `json:"version"`
	DispatcherID    common.DispatcherID `json:"dispatcher_id"`
	PhysicalTableID int64               `json:"physical_table_id"`
	StartTs         uint64              `json:"start_ts"`
	CommitTs        uint64              `json:"commit_ts"`
	// The seq of the event. It is set by event service.
	Seq uint64 `json:"seq"`
	// Length is the number of rows in the transaction.
	Length int32 `json:"length"`
	// RowTypes is the types of every row in the transaction.
	// len(RowTypes) == Length
	// ApproximateSize is the approximate size of all rows in the transaction.
	ApproximateSize int64     `json:"approximate_size"`
	RowTypes        []RowType `json:"row_types"`
	// Rows is the rows of the transaction.
	Rows *chunk.Chunk `json:"rows"`
	// RawRows is the raw bytes of the rows.
	// When the DMLEvent is received from a remote eventService, the Rows is nil.
	// All the data is stored in RawRows.
	// The receiver needs to call DecodeRawRows function to decode the RawRows into Rows.
	RawRows []byte `json:"raw_rows"`

	// TableInfo is the table info of the transaction.
	// If the DMLEvent is send from a remote eventService, the TableInfo is nil.
	TableInfo *common.TableInfo `json:"table_info"`
	// The following fields are set and used by dispatcher.
	ReplicatingTs uint64 `json:"replicating_ts"`
	// PostTxnFlushed is the functions to be executed after the transaction is flushed.
	// It is set and used by dispatcher.
	PostTxnFlushed []func() `json:"-"`
	// offset is the offset of the current row in the transaction.
	// It is internal field, not exported. So it doesn't need to be marshalled.
	offset int `json:"-"`
}

func NewDMLEvent(
	dispatcherID common.DispatcherID,
	tableID int64,
	startTs,
	commitTs uint64,
	tableInfo *common.TableInfo) *DMLEvent {
	// FIXME: check if chk isFull in the future
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), defaultRowCount)
	return &DMLEvent{
		Version:         DMLEventVersion,
		DispatcherID:    dispatcherID,
		PhysicalTableID: tableID,
		StartTs:         startTs,
		CommitTs:        commitTs,
		TableInfo:       tableInfo,
		Rows:            chk,
		RowTypes:        make([]RowType, 0, 1),
	}
}

func (t *DMLEvent) AppendRow(raw *common.RawKVEntry,
	decode func(
		rawKv *common.RawKVEntry,
		tableInfo *common.TableInfo, chk *chunk.Chunk) (int, error),
) error {
	RowType := RowTypeInsert
	if raw.OpType == common.OpTypeDelete {
		RowType = RowTypeDelete
	}
	if len(raw.Value) != 0 && len(raw.OldValue) != 0 {
		RowType = RowTypeUpdate
	}
	count, err := decode(raw, t.TableInfo, t.Rows)
	if err != nil {
		return err
	}
	if count == 1 {
		t.RowTypes = append(t.RowTypes, RowType)
	} else if count == 2 {
		t.RowTypes = append(t.RowTypes, RowType, RowType)
	}
	t.Length += 1
	t.ApproximateSize += int64(len(raw.Key) + len(raw.Value) + len(raw.OldValue))
	return nil
}

func (t *DMLEvent) GetType() int {
	return TypeDMLEvent
}

func (t *DMLEvent) GetDispatcherID() common.DispatcherID {
	return t.DispatcherID
}

func (t *DMLEvent) GetCommitTs() common.Ts {
	return common.Ts(t.CommitTs)
}

func (t *DMLEvent) GetStartTs() common.Ts {
	return common.Ts(t.StartTs)
}

func (t *DMLEvent) PostFlush() {
	for _, f := range t.PostTxnFlushed {
		f()
	}
}

func (t *DMLEvent) GetSeq() uint64 {
	return t.Seq
}

func (t *DMLEvent) PushFrontFlushFunc(f func()) {
	t.PostTxnFlushed = append([]func(){f}, t.PostTxnFlushed...)
}

func (t *DMLEvent) AddPostFlushFunc(f func()) {
	t.PostTxnFlushed = append(t.PostTxnFlushed, f)
}

func (t *DMLEvent) GetNextRow() (RowChange, bool) {
	if t.offset >= len(t.RowTypes) {
		return RowChange{}, false
	}
	rowType := t.RowTypes[t.offset]
	switch rowType {
	case RowTypeInsert:
		row := RowChange{
			Row:     t.Rows.GetRow(t.offset),
			RowType: rowType,
		}
		t.offset++
		return row, true
	case RowTypeDelete:
		row := RowChange{
			PreRow:  t.Rows.GetRow(t.offset),
			RowType: rowType,
		}
		t.offset++
		return row, true
	case RowTypeUpdate:
		row := RowChange{
			PreRow:  t.Rows.GetRow(t.offset),
			Row:     t.Rows.GetRow(t.offset + 1),
			RowType: rowType,
		}
		t.offset += 2
		return row, true
	default:
		log.Panic("TEvent.GetNextRow: invalid row type")
	}
	return RowChange{}, false
}

// Len returns the number of row change events in the transaction.
// Note: An update event is counted as 1 row.
func (t *DMLEvent) Len() int32 {
	return t.Length
}

func (t DMLEvent) Marshal() ([]byte, error) {
	return t.encode()
}

// Unmarshal the DMLEvent from the given data.
// Please make sure the TableInfo of the DMLEvent is set before unmarshal.
func (t *DMLEvent) Unmarshal(data []byte) error {
	return t.decode(data)
}

func (t *DMLEvent) GetSize() int64 {
	return t.ApproximateSize
}

func (t *DMLEvent) encode() ([]byte, error) {
	if t.Version != 0 {
		log.Panic("DMLEvent: Only version 0 is supported right now", zap.Uint8("version", t.Version))
		return nil, nil
	}
	return t.encodeV0()
}

func (t *DMLEvent) encodeV0() ([]byte, error) {
	if t.Version != 0 {
		log.Panic("DMLEvent: invalid version, expect 0, got ", zap.Uint8("version", t.Version))
		return nil, nil
	}
	// Calculate the total size needed for the encoded data
	size := 1 + t.DispatcherID.GetSize() + 5*8 + 4 + int(t.Length)

	// Allocate a buffer with the calculated size
	buf := make([]byte, size)
	offset := 0

	// Encode all fields
	// Version
	buf[offset] = t.Version
	offset += 1

	// DispatcherID
	dispatcherIDBytes := t.DispatcherID.Marshal()
	copy(buf[offset:], dispatcherIDBytes)
	offset += len(dispatcherIDBytes)

	// PhysicalTableID
	binary.LittleEndian.PutUint64(buf[offset:], uint64(t.PhysicalTableID))
	offset += 8
	// StartTs
	binary.LittleEndian.PutUint64(buf[offset:], t.StartTs)
	offset += 8
	// CommitTs
	binary.LittleEndian.PutUint64(buf[offset:], t.CommitTs)
	offset += 8
	// Seq
	binary.LittleEndian.PutUint64(buf[offset:], t.Seq)
	offset += 8
	// Length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(t.Length))
	offset += 4
	// ApproximateSize
	binary.LittleEndian.PutUint64(buf[offset:], uint64(t.ApproximateSize))
	offset += 8
	// RowTypes
	for _, rowType := range t.RowTypes {
		buf[offset] = byte(rowType)
		offset++
	}

	encoder := chunk.NewCodec(t.TableInfo.GetFieldSlice())
	data := encoder.Encode(t.Rows)

	// Append the encoded data to the buffer
	result := append(buf, data...)

	return result, nil
}

func (t *DMLEvent) decode(data []byte) error {
	t.Version = data[0]
	if t.Version != 0 {
		log.Panic("DMLEvent: Only version 0 is supported right now", zap.Uint8("version", t.Version))
		return nil
	}
	return t.decodeV0(data)
}

func (t *DMLEvent) decodeV0(data []byte) error {
	if t.Version != 0 {
		log.Panic("DMLEvent: invalid version, expect 0, got ", zap.Uint8("version", t.Version))
		return nil
	}
	offset := 1
	t.DispatcherID.Unmarshal(data[offset:])
	offset += t.DispatcherID.GetSize()
	t.PhysicalTableID = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	t.StartTs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.CommitTs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.Seq = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.Length = int32(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	t.ApproximateSize = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	t.RowTypes = make([]RowType, t.Length)
	for i := 0; i < int(t.Length); i++ {
		t.RowTypes[i] = RowType(data[offset])
		offset++
	}
	t.RawRows = data[offset:]
	return nil
}

// AssembleRows assembles the Rows from the RawRows.
// It also sets the TableInfo and clears the RawRows.
func (t *DMLEvent) AssembleRows(tableInfo *common.TableInfo) error {
	if tableInfo == nil {
		log.Panic("DMLEvent: TableInfo is nil")
		return nil
	}
	if len(t.RawRows) == 0 {
		log.Panic("DMLEvent: RawRows is empty")
		return nil
	}
	decoder := chunk.NewCodec(tableInfo.GetFieldSlice())
	t.Rows, _ = decoder.Decode(t.RawRows)
	t.TableInfo = tableInfo
	t.RawRows = nil
	return nil
}

type RowChange struct {
	PreRow  chunk.Row
	Row     chunk.Row
	RowType RowType
}

type RowType byte

const (
	// RowTypeDelete represents a delete row.
	RowTypeDelete RowType = iota
	// RowTypeInsert represents a insert row.
	RowTypeInsert
	// RowTypeUpdate represents a update row.
	RowTypeUpdate
)

func RowTypeToString(rowType RowType) string {
	switch rowType {
	case RowTypeInsert:
		return "Insert"
	case RowTypeDelete:
		return "Delete"
	case RowTypeUpdate:
		return "Update"
	default:
		return "Unknown"
	}
}
