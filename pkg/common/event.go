// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

type Event interface {
	GetType() int
	GetDispatcherID() DispatcherID
	GetCommitTs() Ts
	GetStartTs() Ts
}

// FlushableEvent is an event that can be flushed to downstream by a dispatcher.
type FlushableEvent interface {
	Event
	PostFlush()
	AddPostFlushFunc(func())
}

const (
	txnRowCount = 2018
)

const (
	// TEvent is the event type of a transaction.
	TypeTEvent = iota
	// DDLEvent is the event type of a DDL.
	TypeDDLEvent
	// ResolvedEvent is the event type of a resolvedTs.
	TypeResolvedEvent
	// BatchResolvedTs is the event type of a batch resolvedTs.
	TypeBatchResolvedTs
	TypeTxnEvent
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

type BatchResolvedTs struct {
	Events []ResolvedEvent
}

func (b BatchResolvedTs) GetType() int {
	return TypeBatchResolvedTs
}

func (b BatchResolvedTs) GetDispatcherID() DispatcherID {
	// It's a fake dispatcherID.
	return NewDispatcherID()
}

func (b BatchResolvedTs) GetCommitTs() Ts {
	// It's a fake commitTs.
	return 0
}

func (b BatchResolvedTs) GetStartTs() Ts {
	// It's a fake startTs.
	return 0
}

func (b *BatchResolvedTs) Marshal() ([]byte, error) {
	buf := make([]byte, 0, len(b.Events)*24)
	for _, e := range b.Events {
		data, err := e.Marshal()
		if err != nil {
			return nil, err
		}
		buf = append(buf, data...)
	}
	return buf, nil
}

func (b *BatchResolvedTs) Unmarshal(data []byte) error {
	if len(data)%24 != 0 {
		log.Panic("BatchResolvedTs.Unmarshal: invalid data")
	}
	b.Events = make([]ResolvedEvent, 0, len(data)/24)
	for i := 0; i < len(data); i += 24 {
		var e ResolvedEvent
		if err := e.Unmarshal(data[i : i+24]); err != nil {
			return err
		}
		b.Events = append(b.Events, e)
	}
	return nil
}

// ResolvedEvent represents a resolvedTs event of a dispatcher.
type ResolvedEvent struct {
	DispatcherID DispatcherID
	ResolvedTs   Ts
}

func (e ResolvedEvent) GetType() int {
	return TypeResolvedEvent
}

func (e ResolvedEvent) GetDispatcherID() DispatcherID {
	return e.DispatcherID
}

func (e ResolvedEvent) GetCommitTs() Ts {
	return e.ResolvedTs
}

func (e ResolvedEvent) GetStartTs() Ts {
	return e.ResolvedTs
}

func (e ResolvedEvent) Marshal() ([]byte, error) {
	buf := e.DispatcherID.Marshal()
	buf = append(buf, make([]byte, 8)...)
	binary.LittleEndian.PutUint64(buf[16:24], e.ResolvedTs)
	return buf, nil
}

func (e *ResolvedEvent) Unmarshal(data []byte) error {
	if len(data) != 24 {
		log.Panic("ResolvedEvent.Unmarshal: invalid data")
	}
	e.DispatcherID.Unmarshal(data[:16])
	e.ResolvedTs = Ts(binary.LittleEndian.Uint64(data[16:24]))
	return nil
}

func (e ResolvedEvent) String() string {
	return fmt.Sprintf("ResolvedEvent{DispatcherID: %s, ResolvedTs: %d}", e.DispatcherID, e.ResolvedTs)
}

// TEvent represent a transaction, it contains the rows of the transaction.
// Note: The PreRows is the rows before the transaction, it is used to generate the update and delete SQL.
// The Rows is the rows after the transaction, it is used to generate the insert and update SQL.
type TEvent struct {
	DispatcherID    DispatcherID `json:"dispatcher_id"`
	PhysicalTableID uint64       `json:"physical_table_id"`
	StartTs         uint64       `json:"start_ts"`
	CommitTs        uint64       `json:"commit_ts"`

	TableInfo *TableInfo `json:"table_info"`

	Rows     *chunk.Chunk `json:"rows"`
	RowTypes []RowType    `json:"row_types"`
	// Offset is the offset of the current row in the transaction.
	Offset int `json:"offset"`
	len    int

	// The following fields are set and used by dispatcher.
	ReplicatingTs  uint64   `json:"replicating_ts"`
	PostTxnFlushed []func() `msg:"-"`
}

func NewTEvent(
	dispatcherID DispatcherID,
	tableID uint64,
	startTs,
	commitTs uint64,
	tableInfo *TableInfo) *TEvent {
	// FIXME: check if chk isFull in the future
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFileSlice(), txnRowCount)
	return &TEvent{
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

func (t *TEvent) AppendRow(raw *RawKVEntry,
	decode func(
		rawKv *RawKVEntry,
		tableInfo *TableInfo, chk *chunk.Chunk) (int, error),
) error {
	RowType := RowTypeInsert
	if raw.OpType == OpTypeDelete {
		RowType = RowTypeDelete
	}
	if len(raw.Value) != 0 && len(raw.OldValue) != 0 {
		RowType = RowTypeUpdate
	}
	count, err := decode(raw, t.TableInfo, t.Rows)
	if err != nil {
		return err
	}
	log.Info("fizz TEvent.AppendRow", zap.Int("count", count), zap.Any("rowType", RowTypeToString(RowType)))
	if count == 1 {
		t.RowTypes = append(t.RowTypes, RowType)
	} else if count == 2 {
		t.RowTypes = append(t.RowTypes, RowType, RowType)
	}
	t.len += 1
	return nil
}

func (t *TEvent) GetType() int {
	return TypeTEvent
}

func (t *TEvent) GetDispatcherID() DispatcherID {
	return t.DispatcherID
}

func (t *TEvent) GetCommitTs() Ts {
	return Ts(t.CommitTs)
}

func (t *TEvent) GetStartTs() Ts {
	return Ts(t.StartTs)
}

func (t *TEvent) PostFlush() {
	for _, f := range t.PostTxnFlushed {
		f()
	}
}

func (t *TEvent) AddPostFlushFunc(f func()) {
	t.PostTxnFlushed = append(t.PostTxnFlushed, f)
}

func (t *TEvent) GetNextRow() (*Row, bool) {
	if t.Offset >= len(t.RowTypes) {
		return &Row{}, false
	}
	rowType := t.RowTypes[t.Offset]
	switch rowType {
	case RowTypeInsert:
		row := &Row{
			Row:     t.Rows.GetRow(t.Offset),
			RowType: rowType,
		}
		t.Offset++
		return row, true
	case RowTypeDelete:
		row := &Row{
			PreRow:  t.Rows.GetRow(t.Offset),
			RowType: rowType,
		}
		t.Offset++
		return row, true
	case RowTypeUpdate:
		row := &Row{
			PreRow:  t.Rows.GetRow(t.Offset),
			Row:     t.Rows.GetRow(t.Offset + 1),
			RowType: rowType,
		}
		t.Offset += 2
		return row, true
	default:
		log.Panic("TEvent.GetNextRow: invalid row type")
	}
	return &Row{}, false
}

// Len returns the number of row change events in the transaction.
// Note: An update event is counted as 1 row.
func (t *TEvent) Len() int {
	return t.len
}

func (t TEvent) Marshal() ([]byte, error) {
	// TODO
	log.Panic("TEvent.Marshal: not implemented")
	buf := make([]byte, 0)
	return buf, nil
}

func (t *TEvent) Unmarshal(data []byte) error {
	//TODO
	log.Panic("TEvent.Unmarshal: not implemented")
	return nil
}

type Row struct {
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

// func (r *Row) ToSQL(safeMode bool) (string, []interface{}) {
// 	switch r.RowType {
// 	case RowTypeInsert:
// 		return r.ToInsert(safeMode)
// 	case RowTypeDelete:
// 		return r.ToDelete()
// 	case RowTypeUpdate:
// 		return nil, nil
// 	default:
// 		log.Panic("Row.ToSQL: invalid row type")
// 	}
// 	return nil, nil
// }

// func (r *Row) ToInsert(safeMode bool) (string, []interface{}) {
// 	if r.RowType != RowTypeInsert {
// 		log.Panic("Row.ToInsertSQL: invalid row type")
// 	}
// 	return "", nil
// }

// func (r *Row) ToDelete(safeMode bool) (string, []interface{}) {
// 	if r.RowType != RowTypeDelete {
// 		log.Panic("Row.ToDeleteSQL: invalid row type")
// 	}
// 	return "", nil
// }

// func (r *Row) ToUpdate(safeMode bool) (string, []interface{}) {
// 	if r.RowType != RowTypeUpdate {
// 		log.Panic("Row.ToUpdateSQL: invalid row type")
// 	}
// 	return "", nil
// }
