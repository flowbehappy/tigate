package messaging

import (
	"encoding/binary"
	"fmt"

	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/common"
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
	TypeTxnEvent
	TypeWatermark
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
	case TypeTxnEvent:
		return "TxnEvent"
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
	case TypeMessageHandShake:
		return "MessageHandShake"
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
	// todo: s the serverId is not set
	uid := string(data)
	s = ServerId(uid)
	return nil
}

type DMLEvent struct {
	E *common.TxnEvent
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
	E *common.TxnEvent
	// TODO
}

func (d *DDLEvent) Marshal() ([]byte, error) {
	// TODO
	return nil, nil
}

func (d *DDLEvent) Unmarshal(data []byte) error {
	return nil
}

type RegisterDispatcherRequest struct {
	*eventpb.RegisterDispatcherRequest
}

func (r RegisterDispatcherRequest) Marshal() ([]byte, error) {
	return r.RegisterDispatcherRequest.Marshal()
}

func (r RegisterDispatcherRequest) Unmarshal(data []byte) error {
	return r.RegisterDispatcherRequest.Unmarshal(data)
}

func (r RegisterDispatcherRequest) GetID() string {
	return r.DispatcherId
}

func (r RegisterDispatcherRequest) GetClusterID() uint64 {
	return 0
}

func (r RegisterDispatcherRequest) GetTopic() common.TopicType {
	return EventFeedTopic
}

func (r RegisterDispatcherRequest) GetServerID() string {
	return r.ServerId
}

func (r RegisterDispatcherRequest) GetTableSpan() *common.TableSpan {
	return &common.TableSpan{TableSpan: r.TableSpan}
}

func (r RegisterDispatcherRequest) GetStartTs() uint64 {
	return r.StartTs
}

func (r RegisterDispatcherRequest) IsRegister() bool {
	return !r.Remove
}

type Watermark struct {
	Span *common.TableSpan
	Ts   uint64
}

func (w *Watermark) Marshal() ([]byte, error) {
	res := make([]byte, 8)
	binary.LittleEndian.PutUint64(res, w.Ts)
	spanBytes, err := w.Span.Marshal()
	if err != nil {
		return nil, err
	}
	res = append(res, spanBytes...)
	return res, nil
}

func (w *Watermark) Unmarshal(data []byte) error {
	w.Ts = binary.LittleEndian.Uint64(data[:8])
	w.Span = &common.TableSpan{}
	return w.Span.Unmarshal(data[8:])
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
		m = &RegisterDispatcherRequest{}
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
	Epoch    common.EpochType
	Sequence uint64
	Topic    common.TopicType
	Type     IOType
	Message  IOTypeT
}

// NewTargetMessage creates a new TargetMessage to be sent to a target server.
func NewTargetMessage(To ServerId, Topic common.TopicType, Message IOTypeT) *TargetMessage {
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
	case *common.TxnEvent:
		ioType = TypeTxnEvent
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
	case *RegisterDispatcherRequest:
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
