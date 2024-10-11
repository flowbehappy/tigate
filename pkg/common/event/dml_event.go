package event

import (
	"log"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// DMLEvent represent a batch of DMLs of a whole or partial of a transaction.
type DMLEvent struct {
	DispatcherID    common.DispatcherID `json:"dispatcher_id"`
	PhysicalTableID int64               `json:"physical_table_id"`
	StartTs         uint64              `json:"start_ts"`
	CommitTs        uint64              `json:"commit_ts"`

	// Offset is the offset of the current row in the transaction.
	Offset int `json:"offset"`
	len    int

	TableInfo       *common.TableInfo `json:"table_info"`
	Rows            *chunk.Chunk      `json:"rows"`
	RowTypes        []RowType         `json:"row_types"`
	ApproximateSize int64             `json:"approximate_size"`

	// The following fields are set and used by dispatcher.
	ReplicatingTs  uint64   `json:"replicating_ts"`
	PostTxnFlushed []func() `msg:"-"`
}

func NewDMLEvent(
	dispatcherID common.DispatcherID,
	tableID int64,
	startTs,
	commitTs uint64,
	tableInfo *common.TableInfo) *DMLEvent {
	// FIXME: check if chk isFull in the future
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), txnRowCount)
	return &DMLEvent{
		DispatcherID:    dispatcherID,
		PhysicalTableID: tableID,
		StartTs:         startTs,
		CommitTs:        commitTs,
		TableInfo:       tableInfo,
		Rows:            chk,
		RowTypes:        make([]RowType, 0, 1),
		Offset:          0,
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
	t.len += 1
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

func (t *DMLEvent) AddPostFlushFunc(f func()) {
	t.PostTxnFlushed = append(t.PostTxnFlushed, f)
}

func (t *DMLEvent) GetNextRow() (RowChange, bool) {
	if t.Offset >= len(t.RowTypes) {
		return RowChange{}, false
	}
	rowType := t.RowTypes[t.Offset]
	switch rowType {
	case RowTypeInsert:
		row := RowChange{
			Row:     t.Rows.GetRow(t.Offset),
			RowType: rowType,
		}
		t.Offset++
		return row, true
	case RowTypeDelete:
		row := RowChange{
			PreRow:  t.Rows.GetRow(t.Offset),
			RowType: rowType,
		}
		t.Offset++
		return row, true
	case RowTypeUpdate:
		row := RowChange{
			PreRow:  t.Rows.GetRow(t.Offset),
			Row:     t.Rows.GetRow(t.Offset + 1),
			RowType: rowType,
		}
		t.Offset += 2
		return row, true
	default:
		log.Panic("TEvent.GetNextRow: invalid row type")
	}
	return RowChange{}, false
}

// Len returns the number of row change events in the transaction.
// Note: An update event is counted as 1 row.
func (t *DMLEvent) Len() int {
	return t.len
}

func (t DMLEvent) Marshal() ([]byte, error) {
	// TODO
	log.Panic("TEvent.Marshal: not implemented")
	buf := make([]byte, 0)
	return buf, nil
}

func (t *DMLEvent) Unmarshal(data []byte) error {
	//TODO
	log.Panic("TEvent.Unmarshal: not implemented")
	return nil
}

func (t *DMLEvent) GetSize() int64 {
	return t.ApproximateSize
}

type RowChange struct {
	PreRow  chunk.Row
	Row     chunk.Row
	RowType RowType
}

type RowType int

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
