package messaging

import (
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
	TypeTxnEvent
	TypeHeartBeatRequest
	TypeHeartBeatResponse
	TypeScheduleDispatcherRequest
	TypeRegisterDispatcherRequest

	TypeCoordinatorBootstrapRequest
	TypeCoordinatorBootstrapResponse
	TypeAddMaintainerRequest
	TypeRemoveMaintainerRequest
	TypeMaintainerHeartbeatRequest
	TypeMaintainerBootstrapRequest
	TypeMaintainerBootstrapResponse
	TypeMaintainerCloseRequest
	TypeMaintainerCloseResponse

	TypeMessageError
	TypeMessageHandShake
)

func (t IOType) String() string {
	switch t {
	case TypeBytes:
		return "Bytes"
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
	case TypeAddMaintainerRequest:
		return "AddMaintainerRequest"
	case TypeRemoveMaintainerRequest:
		return "RemoveMaintainerRequest"
	case TypeMaintainerHeartbeatRequest:
		return "MaintainerHeartbeatRequest"
	case TypeCoordinatorBootstrapResponse:
		return "CoordinatorBootstrapResponse"
	case TypeRegisterDispatcherRequest:
		return "RegisterDispatcherRequest"
	case TypeMaintainerBootstrapRequest:
		return "BootstrapMaintainerRequest"
	case TypeMaintainerBootstrapResponse:
		return "MaintainerBootstrapResponse"
	case TypeMaintainerCloseRequest:
		return "MaintainerCloseRequest"
	case TypeMaintainerCloseResponse:
		return "MaintainerCloseResponse"
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

func (r RegisterDispatcherRequest) GetTopic() string {
	return EventCollectorTopic
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

func (r RegisterDispatcherRequest) GetChangefeedID() (namespace, id string) {
	return r.Namespace, r.ChangefeedId
}

type IOTypeT interface {
	Unmarshal(data []byte) error
	Marshal() (data []byte, err error)
}

func decodeIOType(ioType IOType, value []byte) (IOTypeT, error) {
	var m IOTypeT
	switch ioType {
	case TypeHeartBeatRequest:
		m = &heartbeatpb.HeartBeatRequest{}
	case TypeHeartBeatResponse:
		m = &heartbeatpb.HeartBeatResponse{}
	case TypeScheduleDispatcherRequest:
		m = &heartbeatpb.ScheduleDispatcherRequest{}
	case TypeCoordinatorBootstrapRequest:
		m = &heartbeatpb.CoordinatorBootstrapRequest{}
	case TypeAddMaintainerRequest:
		m = &heartbeatpb.AddMaintainerRequest{}
	case TypeRemoveMaintainerRequest:
		m = &heartbeatpb.RemoveMaintainerRequest{}
	case TypeMaintainerHeartbeatRequest:
		m = &heartbeatpb.MaintainerHeartbeat{}
	case TypeCoordinatorBootstrapResponse:
		m = &heartbeatpb.CoordinatorBootstrapResponse{}
	case TypeRegisterDispatcherRequest:
		m = &RegisterDispatcherRequest{}
	case TypeMaintainerBootstrapResponse:
		m = &heartbeatpb.MaintainerBootstrapResponse{}
	case TypeMaintainerCloseRequest:
		m = &heartbeatpb.MaintainerCloseRequest{}
	case TypeMaintainerCloseResponse:
		m = &heartbeatpb.MaintainerCloseResponse{}
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
	case *common.TxnEvent:
		ioType = TypeTxnEvent
	case *heartbeatpb.HeartBeatRequest:
		ioType = TypeHeartBeatRequest
	case *heartbeatpb.ScheduleDispatcherRequest:
		ioType = TypeScheduleDispatcherRequest
	case *heartbeatpb.MaintainerBootstrapRequest:
		ioType = TypeMaintainerBootstrapRequest
	case *heartbeatpb.AddMaintainerRequest:
		ioType = TypeAddMaintainerRequest
	case *heartbeatpb.RemoveMaintainerRequest:
		ioType = TypeRemoveMaintainerRequest
	case *heartbeatpb.CoordinatorBootstrapRequest:
		ioType = TypeCoordinatorBootstrapRequest
	case *heartbeatpb.HeartBeatResponse:
		ioType = TypeHeartBeatResponse
	case *MessageError:
		ioType = TypeMessageError
	case *heartbeatpb.MaintainerHeartbeat:
		ioType = TypeMaintainerHeartbeatRequest
	case *heartbeatpb.CoordinatorBootstrapResponse:
		ioType = TypeCoordinatorBootstrapResponse
	case *RegisterDispatcherRequest:
		ioType = TypeRegisterDispatcherRequest
	case *heartbeatpb.MaintainerBootstrapResponse:
		ioType = TypeMaintainerBootstrapResponse
	case *heartbeatpb.MaintainerCloseRequest:
		ioType = TypeMaintainerCloseRequest
	case *heartbeatpb.MaintainerCloseResponse:
		ioType = TypeMaintainerCloseResponse
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
