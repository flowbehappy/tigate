package event

import (
	"encoding/json"

	"github.com/flowbehappy/tigate/pkg/common"
)

// Implement Event / FlushEvent / BlockEvent interface
type SyncPointEvent struct {
	DispatcherID   common.DispatcherID `json:"dispatcher_id"`
	CommitTs       uint64              `json:"commit_ts"`
	PostTxnFlushed []func()            `msg:"-"`
}

func (e *SyncPointEvent) GetType() int {
	return TypeSyncPointEvent
}

func (e *SyncPointEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

func (e *SyncPointEvent) GetCommitTs() common.Ts {
	return e.CommitTs
}

func (e *SyncPointEvent) GetStartTs() common.Ts {
	return e.CommitTs
}

func (e *SyncPointEvent) GetSize() int64 {
	return 0
}

func (e SyncPointEvent) Marshal() ([]byte, error) {
	// TODO: optimize it
	return json.Marshal(e)
}

func (e SyncPointEvent) Unmarshal(data []byte) error {
	// TODO: optimize it
	return json.Unmarshal(data, e)
}

func (e *SyncPointEvent) GetBlockedTables() *InfluencedTables {
	return &InfluencedTables{
		InfluenceType: InfluenceTypeAll,
	}
}

func (e *SyncPointEvent) GetNeedDroppedTables() *InfluencedTables {
	return nil
}

func (e *SyncPointEvent) GetNeedAddedTables() []Table {
	return nil
}

func (e *SyncPointEvent) GetUpdatedSchemas() []SchemaIDChange {
	return nil
}

func (e *SyncPointEvent) PostFlush() {
	for _, f := range e.PostTxnFlushed {
		f()
	}
}

func (e *SyncPointEvent) AddPostFlushFunc(f func()) {
	e.PostTxnFlushed = append(e.PostTxnFlushed, f)
}

func (e *SyncPointEvent) PushFrontFlushFunc(f func()) {
	e.PostTxnFlushed = append([]func(){f}, e.PostTxnFlushed...)
}
