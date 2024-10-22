package event

import (
	"encoding/binary"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	HandshakeEventVersion = 0
)

type HandshakeEvent struct {
	// Version is the version of the HandshakeEvent struct.
	Version      byte                `json:"version"`
	ResolvedTs   uint64              `json:"resolved_ts"`
	Seq          uint64              `json:"seq"`
	DispatcherID common.DispatcherID `json:"-"`
}

func NewHandshakeEvent(dispatcherID common.DispatcherID, resolvedTs common.Ts, seq uint64) *HandshakeEvent {
	return &HandshakeEvent{
		Version:      HandshakeEventVersion,
		ResolvedTs:   uint64(resolvedTs),
		Seq:          seq,
		DispatcherID: dispatcherID,
	}
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
	return int64(1 + 8 + 8 + 16)
}

func (e HandshakeEvent) Marshal() ([]byte, error) {
	return e.encode()
}

func (e *HandshakeEvent) Unmarshal(data []byte) error {
	return e.decode(data)
}

func (e HandshakeEvent) encode() ([]byte, error) {
	if e.Version != 0 {
		log.Panic("HandshakeEvent: invalid version, expect 0, got ", zap.Uint8("version", e.Version))
	}
	return e.encodeV0()
}

func (e *HandshakeEvent) decode(data []byte) error {
	version := data[0]
	if version != 0 {
		log.Panic("HandshakeEvent: invalid version, expect 0, got ", zap.Uint8("version", version))
	}
	return e.decodeV0(data)
}

func (e HandshakeEvent) encodeV0() ([]byte, error) {
	data := make([]byte, e.GetSize())
	offset := 0
	data[offset] = e.Version
	offset += 1
	binary.BigEndian.PutUint64(data[offset:], e.ResolvedTs)
	offset += 8
	binary.BigEndian.PutUint64(data[offset:], e.Seq)
	offset += 8
	copy(data[offset:], e.DispatcherID.Marshal())
	return data, nil
}

func (e *HandshakeEvent) decodeV0(data []byte) error {
	offset := 0
	e.Version = data[offset]
	offset += 1
	e.ResolvedTs = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	e.Seq = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	dispatcherIDData := data[offset:]
	return e.DispatcherID.Unmarshal(dispatcherIDData)
}
