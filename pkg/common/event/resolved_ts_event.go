package event

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/flowbehappy/tigate/pkg/common"
)

type BatchResolvedEvent struct {
	Events []ResolvedEvent
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
	DispatcherID common.DispatcherID
	ResolvedTs   common.Ts
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
	e.ResolvedTs = common.Ts(binary.LittleEndian.Uint64(data[16:24]))
	return nil
}

func (e ResolvedEvent) String() string {
	return fmt.Sprintf("ResolvedEvent{DispatcherID: %s, ResolvedTs: %d}", e.DispatcherID, e.ResolvedTs)
}

// No one will use this method, just for implementing Event interface.
func (e ResolvedEvent) GetSize() int64 {
	return 0
}
