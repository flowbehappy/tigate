package event

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

type BatchResolvedEvent struct {
	// Version is the version of the BatchResolvedEvent struct.
	Version byte
	Events  []ResolvedEvent
}

func (b BatchResolvedEvent) GetType() int {
	return TypeBatchResolvedEvent
}

func (b BatchResolvedEvent) GetDispatcherID() common.DispatcherID {
	// It's a fake dispatcherID.
	return fakeDispatcherID
}

func (b BatchResolvedEvent) GetCommitTs() common.Ts {
	// It's a fake commitTs.
	return 0
}

func (b BatchResolvedEvent) GetStartTs() common.Ts {
	// It's a fake startTs.
	return 0
}

func (b *BatchResolvedEvent) GetSeq() uint64 {
	// It's a fake seq.
	return 0
}

func (b *BatchResolvedEvent) Marshal() ([]byte, error) {
	if len(b.Events) == 0 {
		return nil, nil
	}
	firstEvent := b.Events[0]
	buf := make([]byte, 0, len(b.Events)*int(firstEvent.GetSize()))
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
	fakeEvent := ResolvedEvent{}
	eSize := int(fakeEvent.GetSize())
	b.Events = make([]ResolvedEvent, 0, len(data)/eSize)
	for i := 0; i < len(data); i += eSize {
		var e ResolvedEvent
		if err := e.Unmarshal(data[i : i+eSize]); err != nil {
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

// No one will use this method, just for implementing Event interface.
func (b *BatchResolvedEvent) IsPaused() bool {
	return false
}

const (
	ResolvedEventVersion = 0
)

// ResolvedEvent represents a resolvedTs event of a dispatcher.
type ResolvedEvent struct {
	DispatcherID common.DispatcherID
	ResolvedTs   common.Ts
	State        EventSenderState
	Version      byte
}

func NewResolvedEvent(
	resolvedTs common.Ts,
	dispatcherID common.DispatcherID) ResolvedEvent {
	return ResolvedEvent{
		DispatcherID: dispatcherID,
		ResolvedTs:   resolvedTs,
		State:        EventSenderStateNormal,
		Version:      ResolvedEventVersion,
	}
}

func (e ResolvedEvent) GetType() int {
	return TypeResolvedEvent
}

func (e ResolvedEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

func (e ResolvedEvent) GetCommitTs() common.Ts {
	return e.ResolvedTs
}

func (e ResolvedEvent) GetStartTs() common.Ts {
	return e.ResolvedTs
}

func (e ResolvedEvent) GetSeq() uint64 {
	return 0
}

func (e ResolvedEvent) Marshal() ([]byte, error) {
	return e.encode()
}

func (e *ResolvedEvent) Unmarshal(data []byte) error {
	return e.decode(data)
}

func (e ResolvedEvent) encode() ([]byte, error) {
	if e.Version != ResolvedEventVersion {
		log.Panic("ResolvedEvent: invalid version, expect 0, got ", zap.Uint8("version", e.Version))
	}
	return e.encodeV0()
}

func (e *ResolvedEvent) decode(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("ResolvedEvent.decode: empty data")
	}
	e.Version = data[0]
	if e.Version != ResolvedEventVersion {
		return fmt.Errorf("ResolvedEvent: invalid version, expect 0, got %d", e.Version)
	}
	return e.decodeV0(data)
}

func (e ResolvedEvent) encodeV0() ([]byte, error) {
	data := make([]byte, e.GetSize())
	offset := 0
	data[offset] = e.Version
	offset += 1
	binary.BigEndian.PutUint64(data[offset:], uint64(e.ResolvedTs))
	offset += 8
	copy(data[offset:], e.State.encode())
	offset += e.State.GetSize()
	copy(data[offset:], e.DispatcherID.Marshal())
	return data, nil
}

func (e *ResolvedEvent) decodeV0(data []byte) error {
	if len(data) != int(e.GetSize()) {
		return fmt.Errorf("ResolvedEvent.decodeV0: invalid data length, expected %d, got %d", e.GetSize(), len(data))
	}
	offset := 1 // Skip version byte
	e.ResolvedTs = common.Ts(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	e.State.decode(data[offset:])
	offset += e.State.GetSize()
	return e.DispatcherID.Unmarshal(data[offset:])
}

func (e ResolvedEvent) String() string {
	return fmt.Sprintf("ResolvedEvent{DispatcherID: %s, ResolvedTs: %d}", e.DispatcherID, e.ResolvedTs)
}

// Update GetSize method to reflect the new structure
func (e ResolvedEvent) GetSize() int64 {
	return int64(1 + 8 + e.State.GetSize() + e.DispatcherID.GetSize()) // Version(1) + ResolvedTs(8) + State(1) + DispatcherID(16)
}

func (e ResolvedEvent) IsPaused() bool {
	return e.State.IsPaused()
}
