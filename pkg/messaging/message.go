package messaging

import (
	"encoding/json"
	"fmt"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/apperror"
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
	TypeHeartBeatRequest
	TypeHeartBeatResponse
	TypeScheduleDispatcherRequest
	TypeBootstrapMaintainerRequest
	TypeCoordinatorBootstrapRequest
	TypeDispatchMaintainerRequest
	TypeMaintainerHeartbeatRequest
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
	case TypeHeartBeatRequest:
		return "HeartBeatRequest"
	case TypeHeartBeatResponse:
		return "HeartBeatResponse"
	case TypeScheduleDispatcherRequest:
		return "ScheduleDispatcherRequest"
	case TypeBootstrapMaintainerRequest:
		return "BootstrapMaintainerRequest"
	case TypeCoordinatorBootstrapRequest:
		return "CoordinatorBootstrapRequest"
	case TypeDispatchMaintainerRequest:
		return "DispatchMaintainerRequest"
	case TypeMaintainerHeartbeatRequest:
		return "MaintainerHeartbeatRequest"
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

func (b *Bytes) encode(buf []byte) []byte { return append(buf, (*b)...) }
func (b *Bytes) decode(data []byte) error {
	*b = data
	return nil
}

func (s *ServerId) encode(buf []byte) []byte { return append(buf, (*s)[:]...) }
func (s *ServerId) decode(data []byte) error {
	if len(data) != 16 {
		log.Panic("Invalid data len, data len is expected 16", zap.Int("len", len(data)), zap.String("data", fmt.Sprintf("%v", data)))
		return apperror.AppError{Type: apperror.ErrorTypeDecodeData, Reason: fmt.Sprintf("Invalid data len, data len is expected 16")}
	}
	uid, err := uuid.FromBytes(data)
	if err != nil {
		return err
	}
	*s = ServerId(uid)
	return nil
}

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

func (d *DMLEvent) decode(data []byte) error {
	return nil
}

type DDLEvent struct {
	// TODO
}

func (d *DDLEvent) encode(buf []byte) []byte {
	// TODO
	return nil
}

func (d *DDLEvent) decode(data []byte) error {
	return nil
}

type HeartBeatRequest struct {
	*heartbeatpb.HeartBeatRequest
}

func (h *HeartBeatRequest) encode(buf []byte) []byte {
	data, err := h.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatRequest", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (h *HeartBeatRequest) decode(data []byte) error {
	return h.Unmarshal(data)
}

type HeartBeatResponse struct {
	*heartbeatpb.HeartBeatResponse
}

func (h *HeartBeatResponse) encode(buf []byte) []byte {
	data, err := h.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (h *HeartBeatResponse) decode(data []byte) error {
	return h.Unmarshal(data)
}

type ScheduleDispatcherRequest struct {
	*heartbeatpb.ScheduleDispatcherRequest
}

func (s *ScheduleDispatcherRequest) encode(buf []byte) []byte {
	data, err := s.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (s *ScheduleDispatcherRequest) decode(data []byte) error {
	return s.Unmarshal(data)
}

type IOTypeT interface {
	encode(buf []byte) []byte
	decode(data []byte) error
}

func encodeIOType[T IOTypeT](data T, buf []byte) []byte {
	return (data).encode(buf)
}

func decodeIOType(ioType IOType, value []byte) interface{} {
	var m interface{}
	CastTo(m, ioType).decode(value)
	return m
}
func CastTo(m interface{}, ioType IOType) IOTypeT {
	switch ioType {
	case TypeBytes:
		return m.(*Bytes)
	case TypeServerId:
		return m.(*ServerId)
	case TypeDMLEvent:
		return m.(*DMLEvent)
	case TypeDDLEvent:
		return m.(*DDLEvent)
	case TypeHeartBeatRequest:
		return m.(*HeartBeatRequest)
	case TypeHeartBeatResponse:
		return m.(*HeartBeatResponse)
	case TypeScheduleDispatcherRequest:
		return m.(*ScheduleDispatcherRequest)
	case TypeBootstrapMaintainerRequest:
		return m.(*MaintainerBootstrapRequest)
	case TypeCoordinatorBootstrapRequest:
		return m.(*CoordinatorBootstrapRequest)
	case TypeDispatchMaintainerRequest:
		return m.(*MaintainerBootstrapRequest)
	case TypeMaintainerHeartbeatRequest:
		return m.(*MaintainerHeartbeat)
	default:
		log.Panic("Unimplemented IOType", zap.Stringer("Type", ioType))
		return nil
	}
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
	return encodeIOType(CastTo(m.Message, m.Type), buf)
}

func (m *TargetMessage) decode(value []byte) {
	m.Message = decodeIOType(m.Type, value)
}

func (m *TargetMessage) String() string {
	return fmt.Sprintf("From: %s, To: %s, Type: %s, Message: %v", m.From.String(), m.To.String(), m.Type, m.Message)
}

type MaintainerBootstrapRequest struct {
	*heartbeatpb.MaintainerBootstrapRequest
}

func (m *MaintainerBootstrapRequest) encode(buf []byte) []byte {
	data, err := json.Marshal(m)
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (m *MaintainerBootstrapRequest) decode(data []byte) error {
	return json.Unmarshal(data, &m)
}

type MaintainerHeartbeat struct {
	*heartbeatpb.MaintainerHeartbeat
}

func (m *MaintainerHeartbeat) encode(buf []byte) []byte {
	data, err := m.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (m *MaintainerHeartbeat) decode(data []byte) error {
	return m.Unmarshal(data)
}

type CoordinatorBootstrapRequest struct {
	*heartbeatpb.CoordinatorBootstrapRequest
}

func (m *CoordinatorBootstrapRequest) encode(buf []byte) []byte {
	data, err := m.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (m *CoordinatorBootstrapRequest) decode(data []byte) error {
	return m.Unmarshal(data)
}

type DispatchMaintainerRequest struct {
	*heartbeatpb.CoordinatorBootstrapRequest
}

func (m *DispatchMaintainerRequest) encode(buf []byte) []byte {
	data, err := m.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (m *DispatchMaintainerRequest) decode(data []byte) error {
	return m.Unmarshal(data)
}
