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

func (b *Bytes) Marshal() ([]byte, error) { return *b, nil }
func (b *Bytes) Unmarshal(data []byte) error {
	*b = data
	return nil
}

func (s ServerId) Marshal() ([]byte, error) { return []byte(s), nil }
func (s ServerId) Unmarshal(data []byte) error {
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

func (d *DMLEvent) Marshal() ([]byte, error) {
	// TODO
	return nil, nil
}

func (d *DMLEvent) Unmarshal(data []byte) error {
	return nil
}

type DDLEvent struct {
	// TODO
}

func (d *DDLEvent) Marshal() ([]byte, error) {
	// TODO
	return nil, nil
}

func (d *DDLEvent) Unmarshal(data []byte) error {
	return nil
}

type IOTypeT interface {
	Unmarshal(data []byte) error
	Marshal() (data []byte, err error)
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
		m = &heartbeatpb.HeartBeatRequest{}
	case TypeHeartBeatResponse:
		m = &heartbeatpb.HeartBeatResponse{}
	case TypeScheduleDispatcherRequest:
		m = &heartbeatpb.ScheduleDispatcherRequest{}
	case TypeCoordinatorBootstrapRequest:
		m = &heartbeatpb.CoordinatorBootstrapRequest{}
	case TypeDispatchMaintainerRequest:
		m = &heartbeatpb.DispatchMaintainerRequest{}
	case TypeMaintainerHeartbeatRequest:
		m = &heartbeatpb.MaintainerHeartbeat{}
	case TypeCoordinatorBootstrapResponse:
		m = &heartbeatpb.CoordinatorBootstrapResponse{}
	case TypeEventFeed:
		m = &eventpb.EventFeed{}
	case TypeRegisterDispatcherRequest:
		m = &eventpb.RegisterDispatcherRequest{}
	case TypeMaintainerBootstrapResponse:
		m = &heartbeatpb.MaintainerBootstrapResponse{}
	case TypeMaintainerBootstrapRequest:
		m = &heartbeatpb.MaintainerBootstrapRequest{}
	case TypeMessageError:
		m = &MessageError{AppError: &apperror.AppError{}}
	default:
		log.Panic("Unimplemented IOType", zap.Stringer("Type", ioType))
	}
	err := m.Unmarshal(value)
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
func NewTargetMessage(To ServerId, Topic string, Message IOTypeT) *TargetMessage {
	var ioType IOType
	switch Message.(type) {
	case *Bytes:
		ioType = TypeBytes
	case *ServerId:
		ioType = TypeServerId
	case *DMLEvent:
		ioType = TypeDMLEvent
	case *DDLEvent:
		ioType = TypeDDLEvent
	case *heartbeatpb.HeartBeatRequest:
		ioType = TypeHeartBeatRequest
	case *heartbeatpb.ScheduleDispatcherRequest:
		ioType = TypeScheduleDispatcherRequest
	case *heartbeatpb.MaintainerBootstrapRequest:
		ioType = TypeMaintainerBootstrapRequest
	case *heartbeatpb.DispatchMaintainerRequest:
		ioType = TypeDispatchMaintainerRequest
	case *heartbeatpb.CoordinatorBootstrapRequest:
		ioType = TypeCoordinatorBootstrapRequest
	case *heartbeatpb.HeartBeatResponse:
		ioType = TypeHeartBeatResponse
	case *MessageError:
		ioType = TypeMessageError
	case *eventpb.EventFeed:
		ioType = TypeEventFeed
	case *heartbeatpb.MaintainerHeartbeat:
		ioType = TypeMaintainerHeartbeatRequest
	case *heartbeatpb.CoordinatorBootstrapResponse:
		ioType = TypeCoordinatorBootstrapResponse
	case *eventpb.RegisterDispatcherRequest:
		ioType = TypeRegisterDispatcherRequest
	case *heartbeatpb.MaintainerBootstrapResponse:
		ioType = TypeMaintainerBootstrapResponse
	default:
		panic("unknown io type")
	}

	return &TargetMessage{
		To:      To,
		Type:    ioType,
		Topic:   Topic,
		Message: Message,
	}
}

func (m *TargetMessage) String() string {
	return fmt.Sprintf("From: %s, To: %s, Type: %s, Message: %v", m.From, m.To, m.Type, m.Message)
}

type MessageError struct {
	*apperror.AppError
}

func NewMessageError(err *apperror.AppError) *MessageError {
	return &MessageError{AppError: err}
}

func (m *MessageError) Marshal() ([]byte, error) {
	buf := make([]byte, 58)
	buf = append(buf, byte(m.Type))
	buf = append(buf, []byte(m.Reason)...)
	return buf, nil
}

func (m *MessageError) Unmarshal(data []byte) error {
	m.Type = apperror.ErrorType(data[0])
	m.Reason = string(data[1:])
	return nil
}
