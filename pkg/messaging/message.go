package messaging

import (
	"fmt"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/google/uuid"
)

type IOType int32

const (
	TypeInvalid IOType = iota
	TypeBytes
	TypeServerId
	TypeDMLEvent
	TypeDDLEvent
)

func (t IOType) String() string {
	switch t {
	case TypeBytes:
		return "Bytes"
	case TypeServerId:
		return "ServerId"
	case TypeDMLEvent:
		return "DMLEvent"
	case TypeDDLEvent:
		return "DDLEvent"
	default:
	}
	return "Unknown"
}

type Bytes []byte

type ServerId uuid.UUID

func (s ServerId) String() string { return uuid.UUID(s).String() }

func NewServerId() ServerId {
	return ServerId(uuid.New())
}

func (b *Bytes) encode(buf []byte) []byte    { return append(buf, (*b)...) }
func (s *ServerId) encode(buf []byte) []byte { return append(buf, (*s)[:]...) }

// Note that please never change the return slice directly,
// because the slice is a reference to the original data.
func (s *ServerId) slice() []byte { return (*s)[:] }

type DMLEvent struct {
	// TODO
}

func (d *DMLEvent) encode(buf []byte) []byte {
	// TODO
	return nil
}

type DDLEvent struct {
	// TODO
}

func (d *DDLEvent) encode(buf []byte) []byte {
	// TODO
	return nil
}

type IOTypeT interface {
	*Bytes | *ServerId | *DMLEvent | *DDLEvent

	encode(buf []byte) []byte
}

func CastTo[T IOTypeT](m interface{}) T {
	return m.(T)
}

// TargetMessage is a wrapper of message to be sent to a target server.
// It contains the source server id, the target server id, the message type and the message.
type TargetMessage struct {
	From     ServerId
	To       ServerId
	Epoch    uint64
	Sequence uint64
	Topic    string
	Type     IOType
	Message  interface{}
}

// NewTargetMessage creates a new TargetMessage to be sent to a target server.
func NewTargetMessage(To ServerId, Topic string, Type IOType, Message interface{}) *TargetMessage {
	return &TargetMessage{
		To:      To,
		Type:    Type,
		Topic:   Topic,
		Message: Message,
	}
}

func (m *TargetMessage) encode(buf []byte) []byte {
	switch m.Type {
	case TypeBytes:
		m := m.Message.(Bytes)
		return encodeIOType(&m, buf)
	case TypeServerId:
	case TypeDMLEvent:
	case TypeDDLEvent:
	default:
	}
	log.Panic("Unimplemented IOType", zap.Stringer("Type", m.Type))
	return nil
}

func (m *TargetMessage) decode(data []byte) {
	m.Message = decodeIOType(m.Type, data)
}

func (m *TargetMessage) String() string {
	return fmt.Sprintf("From: %s, To: %s, Type: %s, Message: %v", m.From.String(), m.To.String(), m.Type, m.Message)
}

func encodeIOType[T IOTypeT](data T, buf []byte) []byte {
	return (data).encode(buf)
}

func decodeIOType(mtype IOType, data []byte) interface{} {
	switch mtype {
	case TypeBytes:
		return Bytes(data)
	case TypeServerId:
		if len(data) != 16 {
			log.Panic("Invalid data len, data len is expected 16", zap.Int("len", len(data)), zap.String("data", fmt.Sprintf("%v", data)))
		}
		uid, err := uuid.FromBytes(data)
		if err != nil {
			return nil
		}
		return ServerId(uid)
	case TypeDMLEvent:
	case TypeDDLEvent:
	default:
	}
	log.Panic("Unimplemented IOType", zap.Stringer("Type", mtype))
	return nil
}
