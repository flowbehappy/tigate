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
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/model"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

type Event interface {
	GetType() int
	GetDispatcherID() DispatcherID
	GetCommitTs() Ts
	GetStartTs() Ts
	// GetSize returns the approximate size of the event in bytes.
	// It's used for memory control and monitoring.
	GetSize() int64
}

// FlushableEvent is an event that can be flushed to downstream by a dispatcher.
type FlushableEvent interface {
	Event
	PostFlush()
	AddPostFlushFunc(func())
}

const (
	txnRowCount = 2
)

const (
	// TEvent is the event type of a transaction.
	TypeDMLEvent = iota
	// DDLEvent is the event type of a DDL.
	TypeDDLEvent
	// ResolvedEvent is the event type of a resolvedTs.
	TypeResolvedEvent
	// BatchResolvedTs is the event type of a batch resolvedTs.
	TypeBatchResolvedEvent
)

// fakeDispatcherID is a fake dispatcherID for batch resolvedTs.
var fakeDispatcherID = DispatcherID{low: 0, high: 0}

type BatchResolvedEvent struct {
	Events []ResolvedEvent
}

func (b BatchResolvedEvent) GetType() int {
	return TypeBatchResolvedEvent
}

func (b BatchResolvedEvent) GetDispatcherID() DispatcherID {
	// It's a fake dispatcherID.
	return fakeDispatcherID
}

func (b BatchResolvedEvent) GetCommitTs() Ts {
	// It's a fake commitTs.
	return 0
}

func (b BatchResolvedEvent) GetStartTs() Ts {
	// It's a fake startTs.
	return 0
}

func (b *BatchResolvedEvent) Marshal() ([]byte, error) {
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

func (b *BatchResolvedEvent) Unmarshal(data []byte) error {
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

// No one will use this method, just for implementing Event interface.
func (b *BatchResolvedEvent) GetSize() int64 {
	return 0
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

// No one will use this method, just for implementing Event interface.
func (e ResolvedEvent) GetSize() int64 {
	return 0
}

// DMLEvent represent a batch of DMLs of a whole or partial of a transaction.
type DMLEvent struct {
	DispatcherID    DispatcherID `json:"dispatcher_id"`
	PhysicalTableID int64        `json:"physical_table_id"`
	StartTs         uint64       `json:"start_ts"`
	CommitTs        uint64       `json:"commit_ts"`

	// Offset is the offset of the current row in the transaction.
	Offset int `json:"offset"`
	len    int

	TableInfo       *TableInfo   `json:"table_info"`
	Rows            *chunk.Chunk `json:"rows"`
	RowTypes        []RowType    `json:"row_types"`
	ApproximateSize int64        `json:"approximate_size"`

	// The following fields are set and used by dispatcher.
	ReplicatingTs  uint64   `json:"replicating_ts"`
	PostTxnFlushed []func() `msg:"-"`
}

func NewDMLEvent(
	dispatcherID DispatcherID,
	tableID int64,
	startTs,
	commitTs uint64,
	tableInfo *TableInfo) *DMLEvent {
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

func (t *DMLEvent) AppendRow(raw *RawKVEntry,
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
	if count == 1 {
		t.RowTypes = append(t.RowTypes, RowType)
	} else if count == 2 {
		t.RowTypes = append(t.RowTypes, RowType, RowType)
	}
	t.len += 1
	t.ApproximateSize += int64(len(raw.Key)) + int64(len(raw.Value)) + int64(len(raw.OldValue))
	return nil
}

func (t *DMLEvent) GetType() int {
	return TypeDMLEvent
}

func (t *DMLEvent) GetDispatcherID() DispatcherID {
	return t.DispatcherID
}

func (t *DMLEvent) GetCommitTs() Ts {
	return Ts(t.CommitTs)
}

func (t *DMLEvent) GetStartTs() Ts {
	return Ts(t.StartTs)
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

type SchemaTableName struct {
	SchemaName string
	TableName  string
}

// TableChange will record each ddl change of the table name.
// Each TableChange is related to a ddl event
type TableNameChange struct {
	AddName          []SchemaTableName
	DropName         []SchemaTableName
	DropDatabaseName string
}

type DDLEvent struct {
	DispatcherID DispatcherID `json:"dispatcher_id"`
	Type         byte         `json:"type"`
	// SchemaID means different for different job types:
	// - ExchangeTablePartition: db id of non-partitioned table
	SchemaID int64 `json:"schema_id"`
	// TableID means different for different job types:
	// - ExchangeTablePartition: non-partitioned table id
	TableID int64 `json:"table_id"`
	// TODO: need verify the meaning of SchemaName and TableName for different ddl
	SchemaName string           `json:"schema_name"`
	TableName  string           `json:"table_name"`
	Query      string           `json:"query"`
	TableInfo  *model.TableInfo `json:"table_info"`
	FinishedTs uint64           `json:"finished_ts"`

	// TODO: just here for compile, may be changed later
	MultipleTableInfos []*TableInfo `json:"multiple_table_infos"`

	BlockedTables     *InfluencedTables `json:"blocked_tables"`
	NeedDroppedTables *InfluencedTables `json:"need_dropped_tables"`
	NeedAddedTables   []Table           `json:"need_added_tables"`

	TiDBOnly bool `json:"tidb_only"`

	// only Create Table / Create Tables / Drop Table / Rename Table /
	// Rename Tables / Drop Schema / Recover Table will make the table name change
	TableNameChange *TableNameChange `json:"table_name_change"`
	// 用于在event flush 后执行，后续兼容不同下游的时候要看是不是要拆下去
	PostTxnFlushed []func() `msg:"-"`
}

func (d *DDLEvent) GetType() int {
	return TypeDDLEvent
}

func (d *DDLEvent) GetDispatcherID() DispatcherID {
	return d.DispatcherID
}

func (d *DDLEvent) GetStartTs() Ts {
	return 0
}

func (d *DDLEvent) GetCommitTs() Ts {
	return d.FinishedTs
}

func (d *DDLEvent) PostFlush() {
	for _, f := range d.PostTxnFlushed {
		f()
	}
}

func (d *DDLEvent) AddPostFlushFunc(f func()) {
	d.PostTxnFlushed = append(d.PostTxnFlushed, f)
}

func (e *DDLEvent) GetBlockedTables() *InfluencedTables {
	return e.BlockedTables
}

func (e *DDLEvent) GetNeedDroppedTables() *InfluencedTables {
	return e.NeedDroppedTables
}

func (e *DDLEvent) GetNeedAddedTables() []Table {
	return e.NeedAddedTables
}

func (e *DDLEvent) IsSyncPointEvent() bool {
	// TODO
	return false
}

func (e *DDLEvent) GetDDLQuery() string {
	if e == nil {
		log.Error("DDLEvent is nil, should not happened in production env", zap.Any("event", e))
		return ""
	}
	return e.Query
}

func (e *DDLEvent) GetDDLSchemaName() string {
	if e == nil {
		return "" // 要报错的
	}
	return e.SchemaName
}

func (e *DDLEvent) GetDDLType() model.ActionType {
	return model.ActionType(e.Type)
}

func (t DDLEvent) Marshal() ([]byte, error) {
	// TODO: optimize it
	return json.Marshal(t)
}

func (t *DDLEvent) Unmarshal(data []byte) error {
	return json.Unmarshal(data, t)
}

// TODO: fix it
func (t *DDLEvent) GetSize() int64 {
	return 0
}

type InfluenceType int

const (
	InfluenceTypeAll InfluenceType = iota // influence all tables
	InfluenceTypeDB                       // influence all tables in the same database
	InfluenceTypeNormal
)

func (t InfluenceType) toPB() heartbeatpb.InfluenceType {
	switch t {
	case InfluenceTypeAll:
		return heartbeatpb.InfluenceType_All
	case InfluenceTypeDB:
		return heartbeatpb.InfluenceType_DB
	case InfluenceTypeNormal:
		return heartbeatpb.InfluenceType_Normal
	default:
		log.Error("unknown influence type")
	}
	return heartbeatpb.InfluenceType_Normal
}

type InfluencedTables struct {
	InfluenceType InfluenceType
	// only exists when InfluenceType is InfluenceTypeNormal
	TableIDs []int64
	// only exists when InfluenceType is InfluenceTypeDB
	SchemaID int64
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
			TableID:  t.TableID,
			SchemaID: t.SchemaID,
		}
	}
	return res
}

type Table struct {
	SchemaID int64
	TableID  int64
}
