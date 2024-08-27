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

// TEvent represent a transaction, it contains the rows of the transaction.
// Note: The PreRows is the rows before the transaction, it is used to generate the update and delete SQL.
// The Rows is the rows after the transaction, it is used to generate the insert and update SQL.
type TEvent struct {
	DispatcherID    DispatcherID
	PhysicalTableID uint64
	StartTs         uint64
	CommitTs        uint64

	Rows    chunk.Chunk
	PreRows chunk.Chunk
}

func (t TEvent) GetType() int {
	return TypeTEvent
}

func (t TEvent) GetDispatcherID() DispatcherID {
	return t.DispatcherID
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

type DDLEvent struct {
	DispatcherID DispatcherID
	// commitTS of the rawKV
	CommitTS Ts
	Job      *model.Job
}

func (d DDLEvent) GetType() int {
	return TypeDDLEvent
}

func (d DDLEvent) GetDispatcherID() DispatcherID {
	return d.DispatcherID
}

func (d DDLEvent) Marshal() ([]byte, error) {
	// TODO
	log.Panic("DDLEvent.Marshal: not implemented")
	buf := make([]byte, 0)
	return buf, nil
}

func (d DDLEvent) Unmarshal(data []byte) error {
	//TODO
	log.Panic("DDLEvent.Unmarshal: not implemented")
	return nil
}

func (d DDLEvent) String() string {
	return fmt.Sprintf("DDLEvent{DispatcherID: %s, CommitTS: %d, Job: %v}", d.DispatcherID, d.CommitTS, d.Job)
}
