package event

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
)

type Event interface {
	GetType() int
	GetSeq() uint64
	GetDispatcherID() common.DispatcherID
	GetCommitTs() common.Ts
	GetStartTs() common.Ts
	// GetSize returns the approximate size of the event in bytes.
	// It's used for memory control and monitoring.
	GetSize() int64
	IsPaused() bool
}

// FlushableEvent is an event that can be flushed to downstream by a dispatcher.
type FlushableEvent interface {
	Event
	PostFlush()
	AddPostFlushFunc(func())
	PushFrontFlushFunc(f func())
	ClearPostFlushFunc()
}

// BlockEvent is an event that may be blocked the dispatcher.
// It could be a ddl event or a sync point event.
type BlockEvent interface {
	FlushableEvent
	GetBlockedTables() *InfluencedTables
	GetNeedDroppedTables() *InfluencedTables
	GetNeedAddedTables() []Table
	GetUpdatedSchemas() []SchemaIDChange
}

const (
	// TEvent is the event type of a transaction.
	TypeDMLEvent = iota
	// DDLEvent is the event type of a DDL.
	TypeDDLEvent
	// ResolvedEvent is the event type of a resolvedTs.
	TypeResolvedEvent
	// BatchResolvedTs is the event type of a batch resolvedTs.
	TypeBatchResolvedEvent
	// SyncPointEvent is the event type of a sync point.
	TypeSyncPointEvent
	// HandshakeEvent is the event type to indicate the start of a new event stream.
	TypeHandshakeEvent
	// TypeReadyEvent is the event type to indicate the event service is ready to send events.
	TypeReadyEvent
	// TypeNotReusableEvent is the event type to indicate the event service has no data for reuse.
	TypeNotReusableEvent
)

// fakeDispatcherID is a fake dispatcherID for batch resolvedTs.
var fakeDispatcherID = common.DispatcherID(common.NewGIDWithValue(0, 0))

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
	*SchemaTableName
}

type SchemaIDChange struct {
	TableID     int64
	OldSchemaID int64
	NewSchemaID int64
}

func ToSchemaIDChangePB(SchemaIDChange []SchemaIDChange) []*heartbeatpb.SchemaIDChange {
	if SchemaIDChange == nil {
		return nil
	}
	res := make([]*heartbeatpb.SchemaIDChange, len(SchemaIDChange))
	for i, c := range SchemaIDChange {
		res[i] = &heartbeatpb.SchemaIDChange{
			TableID:     c.TableID,
			OldSchemaID: c.OldSchemaID,
			NewSchemaID: c.NewSchemaID,
		}
	}
	return res
}

type EventSenderState byte

const (
	EventSenderStateNormal EventSenderState = iota
	EventSenderStatePaused
)

func (s EventSenderState) String() string {
	switch s {
	case EventSenderStateNormal:
		return "normal"
	case EventSenderStatePaused:
		return "paused"
	}
	return "unknown"
}

func (s EventSenderState) encode() []byte {
	return []byte{byte(s)}
}

func (s *EventSenderState) decode(data []byte) {
	if len(data) == 0 {
		return
	}
	*s = EventSenderState(data[0])
}

func (s EventSenderState) GetSize() int {
	return 1
}

func (s EventSenderState) IsPaused() bool {
	return s == EventSenderStatePaused
}
