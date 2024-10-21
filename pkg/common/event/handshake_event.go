package event

import (
	"encoding/json"

	"github.com/flowbehappy/tigate/pkg/common"
)

type HandshakeEvent struct {
	Version      int                 `json:"version"`
	DispatcherID common.DispatcherID `json:"dispatcher_id"`
	ResolvedTs   uint64              `json:"resolved_ts"`
	Seq          uint64              `json:"seq"`
}

// GetType returns the event type
func (e *HandshakeEvent) GetType() int {
	return TypeHandshakeEvent
}

// GeSeq return the sequence number of handshake event.
func (e *HandshakeEvent) GetSeq() uint64 {
	return e.Seq
}

// GetDispatcherID returns the dispatcher ID
func (e *HandshakeEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

// GetCommitTs returns the commit timestamp
func (e *HandshakeEvent) GetCommitTs() common.Ts {
	return e.ResolvedTs
}

// GetStartTs returns the start timestamp
func (e *HandshakeEvent) GetStartTs() common.Ts {
	return e.ResolvedTs
}

// GetSize returns the approximate size of the event in bytes
func (e *HandshakeEvent) GetSize() int64 {
	return int64(8 + 16)
}

func (e *HandshakeEvent) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *HandshakeEvent) Unmarshal(data []byte) error {
	return json.Unmarshal(data, e)
}
