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
	"log"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

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

type Event interface {
	GetType() int
	GetDispatcherID() DispatcherID
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

type Row struct {
	PreRow chunk.Row
	Row    chunk.Row
	RowType
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
		RowTypes:        make([]RowType, 1),
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
	if count != 0 {
		t.RowTypes = append(t.RowTypes, RowType)
	}
	return nil
}

func (t TEvent) GetType() int {
	return TypeTEvent
}

func (t TEvent) GetDispatcherID() DispatcherID {
	return t.DispatcherID
}

func (t TEvent) GetNextRow() (Row, bool) {
	if t.Offset >= t.Rows.NumRows() {
		return Row{}, false
	}
	rowType := t.RowTypes[t.Offset]
	switch rowType {
	case RowTypeInsert:
		row := Row{
			Row:     t.Rows.GetRow(t.Offset),
			RowType: rowType,
		}
		t.Offset++
		return row, true
	case RowTypeDelete:
		row := Row{
			PreRow:  t.Rows.GetRow(t.Offset),
			RowType: rowType,
		}
		t.Offset++
		return row, true
	case RowTypeUpdate:
		row := Row{
			PreRow:  t.Rows.GetRow(t.Offset),
			Row:     t.Rows.GetRow(t.Offset + 1),
			RowType: rowType,
		}
		t.Offset += 2
		return row, true
	default:
		log.Panic("TEvent.GetNextRow: invalid row type")
	}
	return Row{}, false
}

// Len returns the number of row change events in the transaction.
// Note: An update event is counted as 1 row.
func (t TEvent) Len() int {
	return len(t.RowTypes)
}

func (t TEvent) Marshal() ([]byte, error) {
	// TODO
	log.Panic("TEvent.Marshal: not implemented")
	buf := make([]byte, 0)
	return buf, nil
}

func (t TEvent) Unmarshal(data []byte) error {
	//TODO
	log.Panic("TEvent.Unmarshal: not implemented")
	return nil
}

type DDLEventX struct {
	DispatcherID DispatcherID
	// commitTS of the rawKV
	CommitTS Ts
	Job      *model.Job
}

func (d DDLEventX) GetType() int {
	return TypeDDLEvent
}

func (d DDLEventX) GetDispatcherID() DispatcherID {
	return d.DispatcherID
}

func (d DDLEventX) Marshal() ([]byte, error) {
	// TODO
	log.Panic("DDLEvent.Marshal: not implemented")
	buf := make([]byte, 0)
	return buf, nil
}

func (d DDLEventX) Unmarshal(data []byte) error {
	//TODO
	log.Panic("DDLEvent.Unmarshal: not implemented")
	return nil
}

func (d DDLEventX) String() string {
	return fmt.Sprintf("DDLEvent{DispatcherID: %s, CommitTS: %d, Job: %v}", d.DispatcherID, d.CommitTS, d.Job)
}
