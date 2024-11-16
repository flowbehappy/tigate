package event

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

const (
	ReadyEventVersion = 0
)

type ReadyEvent struct {
	Version      byte
	DispatcherID common.DispatcherID
}

func NewReadyEvent(dispatcherID common.DispatcherID) ReadyEvent {
	return ReadyEvent{
		Version:      ReadyEventVersion,
		DispatcherID: dispatcherID,
	}
}

// GetType returns the event type
func (e *ReadyEvent) GetType() int {
	return TypeReadyEvent
}

// GeSeq return the sequence number of handshake event.
func (e *ReadyEvent) GetSeq() uint64 {
	// not used
	return 0
}

// GetDispatcherID returns the dispatcher ID
func (e *ReadyEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

// GetCommitTs returns the commit timestamp
func (e *ReadyEvent) GetCommitTs() common.Ts {
	// not used
	return 0
}

// GetStartTs returns the start timestamp
func (e *ReadyEvent) GetStartTs() common.Ts {
	// not used
	return 0
}

// GetSize returns the approximate size of the event in bytes
func (e *ReadyEvent) GetSize() int64 {
	return int64(1 + e.DispatcherID.GetSize())
}

func (e *ReadyEvent) IsPaused() bool {
	// TODO: is this ok?
	return false
}

func (e ReadyEvent) Marshal() ([]byte, error) {
	return e.encode()
}

func (e *ReadyEvent) Unmarshal(data []byte) error {
	return e.decode(data)
}

func (e ReadyEvent) encode() ([]byte, error) {
	if e.Version != 0 {
		log.Panic("ReadyEvent: invalid version, expect 0, got ", zap.Uint8("version", e.Version))
	}
	return e.encodeV0()
}

func (e *ReadyEvent) decode(data []byte) error {
	version := data[0]
	if version != 0 {
		log.Panic("ReadyEvent: invalid version, expect 0, got ", zap.Uint8("version", version))
	}
	return e.decodeV0(data)
}

func (e ReadyEvent) encodeV0() ([]byte, error) {
	data := make([]byte, e.GetSize())
	offset := 0
	data[offset] = e.Version
	offset += 1
	copy(data[offset:], e.DispatcherID.Marshal())
	offset += e.DispatcherID.GetSize()
	return data, nil
}

func (e *ReadyEvent) decodeV0(data []byte) error {
	offset := 0
	e.Version = data[offset]
	offset += 1
	dispatcherIDData := data[offset:]
	return e.DispatcherID.Unmarshal(dispatcherIDData)
}
