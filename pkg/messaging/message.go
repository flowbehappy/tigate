package messaging

import (
	"fmt"

	"github.com/flowbehappy/tigate/eventpb"
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
	TypeEventFeed
	TypeRegisterDispatcherRequest
	TypeCoordinatorBootstrapRequest
	TypeCoordinatorBootstrapResponse
	TypeDispatchMaintainerRequest
	TypeMaintainerHeartbeatRequest
	TypeMaintainerBootstrapResponse
	TypeMaintainerBootstrapRequest
	TypeMessageError
	TypeMessageHandShake
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
	case TypeCoordinatorBootstrapRequest:
		return "CoordinatorBootstrapRequest"
	case TypeDispatchMaintainerRequest:
		return "DispatchMaintainerRequest"
	case TypeMaintainerHeartbeatRequest:
		return "MaintainerHeartbeatRequest"
	case TypeCoordinatorBootstrapResponse:
		return "CoordinatorBootstrapResponse"
	case TypeEventFeed:
		return "EventFeed"
	case TypeRegisterDispatcherRequest:
		return "RegisterDispatcherRequest"
	case TypeMaintainerBootstrapRequest:
		return "BootstrapMaintainerRequest"
	case TypeMaintainerBootstrapResponse:
		return "MaintainerBootstrapResponse"
	case TypeMessageError:
		return "MessageError"
	default:
	}
	return "Unknown"
}

type Bytes []byte

type ServerId string

func (s ServerId) String() string {
	return string(s)
}

func NewServerId() ServerId {
	return ServerId(uuid.New().String())
}

func (b *Bytes) encode(buf []byte) []byte { return append(buf, (*b)...) }
func (b *Bytes) decode(data []byte) error {
	*b = data
	return nil
}

func (s ServerId) encode(buf []byte) []byte { return append(buf, []byte(s)...) }
func (s ServerId) decode(data []byte) error {
	if len(data) != 16 {
		log.Panic("Invalid data len, data len is expected 16", zap.Int("len", len(data)), zap.String("data", fmt.Sprintf("%v", data)))
		return apperror.AppError{Type: apperror.ErrorTypeDecodeData, Reason: fmt.Sprintf("Invalid data len, data len is expected 16")}
	}
	uid := string(data)
	s = ServerId(uid)
	return nil
}

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

type EventFeed struct {
	*eventpb.EventFeed
}

func (f *EventFeed) encode(buf []byte) []byte {
	data, err := f.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (f *EventFeed) decode(data []byte) error {
	return f.Unmarshal(data)
}

type RegisterDispatcherRequest struct {
	*eventpb.RegisterDispatcherRequest
}

func (r *RegisterDispatcherRequest) encode(buf []byte) []byte {
	data, err := r.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (r *RegisterDispatcherRequest) decode(data []byte) error {
	return r.Unmarshal(data)
}

type IOTypeT interface {
	encode(buf []byte) []byte
	decode(data []byte) error
}

func encodeIOType[T IOTypeT](data T, buf []byte) []byte {
	return (data).encode(buf)
}

func decodeIOType(ioType IOType, value []byte) (IOTypeT, error) {
	var m IOTypeT
	switch ioType {
	case TypeBytes:
		m = &Bytes{}
	case TypeServerId:
		return ServerId(value), nil
	case TypeDMLEvent:
		m = &DMLEvent{}
	case TypeDDLEvent:
		m = &DDLEvent{}
	case TypeHeartBeatRequest:
		m = &HeartBeatRequest{}
	case TypeHeartBeatResponse:
		m = &HeartBeatResponse{}
	case TypeScheduleDispatcherRequest:
		m = &ScheduleDispatcherRequest{}
	case TypeCoordinatorBootstrapRequest:
		m = &CoordinatorBootstrapRequest{}
	case TypeDispatchMaintainerRequest:
		m = &DispatchMaintainerRequest{}
	case TypeMaintainerHeartbeatRequest:
		m = &MaintainerHeartbeat{}
	case TypeCoordinatorBootstrapResponse:
		m = &CoordinatorBootstrapResponse{}
	case TypeEventFeed:
		m = &EventFeed{}
	case TypeRegisterDispatcherRequest:
		m = &RegisterDispatcherRequest{}
	case TypeMaintainerBootstrapResponse:
		m = &MaintainerBootstrapResponse{}
	case TypeMaintainerBootstrapRequest:
		m = &MaintainerBootstrapRequest{}
	case TypeMessageError:
		m = &MessageError{AppError: &apperror.AppError{}}
	default:
		log.Panic("Unimplemented IOType", zap.Stringer("Type", ioType))
	}
	err := m.decode(value)
	return m, err
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
	Message  IOTypeT
}

// NewTargetMessage creates a new TargetMessage to be sent to a target server.
func NewTargetMessage(To ServerId, Topic string, Type IOType, Message IOTypeT) *TargetMessage {
	return &TargetMessage{
		To:      To,
		Type:    Type,
		Topic:   Topic,
		Message: Message,
	}
}

func (m *TargetMessage) encode(buf []byte) []byte {
	return encodeIOType(m.Message, buf)
}

func (m *TargetMessage) String() string {
	return fmt.Sprintf("From: %s, To: %s, Type: %s, Message: %v", m.From, m.To, m.Type, m.Message)
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
	m.MaintainerHeartbeat = &heartbeatpb.MaintainerHeartbeat{}
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
	m.CoordinatorBootstrapRequest = &heartbeatpb.CoordinatorBootstrapRequest{}
	return m.Unmarshal(data)
}

type DispatchMaintainerRequest struct {
	*heartbeatpb.DispatchMaintainerRequest
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
	m.DispatchMaintainerRequest = &heartbeatpb.DispatchMaintainerRequest{}
	return m.Unmarshal(data)
}

type CoordinatorBootstrapResponse struct {
	*heartbeatpb.CoordinatorBootstrapResponse
}

func (m *CoordinatorBootstrapResponse) encode(buf []byte) []byte {
	data, err := m.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (m *CoordinatorBootstrapResponse) decode(data []byte) error {
	m.CoordinatorBootstrapResponse = &heartbeatpb.CoordinatorBootstrapResponse{}
	return m.Unmarshal(data)
}

type MaintainerBootstrapRequest struct {
	*heartbeatpb.MaintainerBootstrapRequest
}

func (m *MaintainerBootstrapRequest) encode(buf []byte) []byte {
	data, err := m.Marshal()
	if err != nil {
		log.Panic("Failed to encode HeartBeatResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (m *MaintainerBootstrapRequest) decode(data []byte) error {
	m.MaintainerBootstrapRequest = &heartbeatpb.MaintainerBootstrapRequest{}
	return m.Unmarshal(data)
}

type MaintainerBootstrapResponse struct {
	*heartbeatpb.MaintainerBootstrapResponse
}

func (m *MaintainerBootstrapResponse) encode(buf []byte) []byte {
	data, err := m.Marshal()
	if err != nil {
		log.Panic("Failed to encode MaintainerBootstrapResponse", zap.Error(err))
		return buf
	}
	buf = append(buf, data...)
	return buf
}

func (m *MaintainerBootstrapResponse) decode(data []byte) error {
	m.MaintainerBootstrapResponse = &heartbeatpb.MaintainerBootstrapResponse{}
	return m.Unmarshal(data)
}

type MessageError struct {
	*apperror.AppError
}

func NewMessageError(err *apperror.AppError) *MessageError {
	return &MessageError{AppError: err}
}

func (m *MessageError) encode(buf []byte) []byte {
	buf = append(buf, byte(m.Type))
	buf = append(buf, []byte(m.Reason)...)
	return buf
}

func (m *MessageError) decode(data []byte) error {
	m.Type = apperror.ErrorType(data[0])
	m.Reason = string(data[1:])
	return nil
}
