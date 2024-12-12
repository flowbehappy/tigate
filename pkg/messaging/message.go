package messaging

import (
	"fmt"
	"time"

	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/node"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

type IOType int32

const (
	TypeInvalid IOType = iota
	// LogService related
	TypeDMLEvent
	TypeDDLEvent
	TypeBatchResolvedTs
	TypeSyncPointEvent
	TypeHandshakeEvent
	TypeReadyEvent
	TypeNotReusableEvent

	// LogCoordinator related
	TypeLogCoordinatorBroadcastRequest
	TypeEventStoreState
	TypeReusableEventServiceRequest
	TypeReusableEventServiceResponse

	TypeHeartBeatRequest
	TypeHeartBeatResponse
	TypeScheduleDispatcherRequest
	TypeRegisterDispatcherRequest
	TypeCheckpointTsMessage
	TypeBlockStatusRequest

	TypeCoordinatorBootstrapRequest
	TypeCoordinatorBootstrapResponse
	TypeAddMaintainerRequest
	TypeRemoveMaintainerRequest
	TypeMaintainerHeartbeatRequest
	TypeMaintainerBootstrapRequest
	TypeMaintainerBootstrapResponse
	TypeMaintainerPostBootstrapRequest
	TypeMaintainerPostBootstrapResponse
	TypeMaintainerCloseRequest
	TypeMaintainerCloseResponse

	TypeMessageError
	TypeMessageHandShake
)

func (t IOType) String() string {
	switch t {
	case TypeDMLEvent:
		return "DMLEvent"
	case TypeDDLEvent:
		return "DDLEvent"
	case TypeSyncPointEvent:
		return "SyncPointEvent"
	case TypeBatchResolvedTs:
		return "BatchResolvedTs"
	case TypeHandshakeEvent:
		return "HandshakeEvent"
	case TypeReadyEvent:
		return "TypeReadyEvent"
	case TypeNotReusableEvent:
		return "TypeNotReusableEvent"
	case TypeLogCoordinatorBroadcastRequest:
		return "TypeLogCoordinatorBroadcastRequest"
	case TypeReusableEventServiceRequest:
		return "TypeReusableEventServiceRequest"
	case TypeReusableEventServiceResponse:
		return "TypeReusableEventServiceResponse"
	case TypeEventStoreState:
		return "TypeEventStoreState"
	case TypeHeartBeatRequest:
		return "HeartBeatRequest"
	case TypeHeartBeatResponse:
		return "HeartBeatResponse"
	case TypeBlockStatusRequest:
		return "BlockStatusRequest"
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
	case TypeMaintainerPostBootstrapRequest:
		return "MaintainerPostBootstrapRequest"
	case TypeMaintainerPostBootstrapResponse:
		return "MaintainerPostBootstrapResponse"
	case TypeMaintainerCloseRequest:
		return "MaintainerCloseRequest"
	case TypeMaintainerCloseResponse:
		return "MaintainerCloseResponse"
	case TypeMessageError:
		return "MessageError"
	case TypeMessageHandShake:
		return "MessageHandShake"
	case TypeCheckpointTsMessage:
		return "CheckpointTsMessage"
	default:
	}
	return "Unknown"
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

func (r RegisterDispatcherRequest) GetID() common.DispatcherID {
	return common.NewDispatcherIDFromPB(r.DispatcherId)
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

func (r RegisterDispatcherRequest) GetTableSpan() *heartbeatpb.TableSpan {
	return r.TableSpan
}

func (r RegisterDispatcherRequest) GetStartTs() uint64 {
	return r.StartTs
}

func (r RegisterDispatcherRequest) GetChangefeedID() common.ChangeFeedID {
	return common.NewChangefeedIDFromPB(r.ChangefeedId)
}

func (r RegisterDispatcherRequest) GetFilter() filter.Filter {
	changefeedID := r.GetChangefeedID()
	filter, err := filter.GetSharedFilterStorage().GetOrSetFilter(changefeedID, r.RegisterDispatcherRequest.FilterConfig, "", false)
	if err != nil {
		log.Panic("create filter failed", zap.Error(err), zap.Any("filterConfig", r.RegisterDispatcherRequest.FilterConfig))
	}
	return filter
}

func (r RegisterDispatcherRequest) SyncPointEnabled() bool {
	return r.EnableSyncPoint
}

func (r RegisterDispatcherRequest) GetSyncPointTs() uint64 {
	return r.SyncPointTs
}

func (r RegisterDispatcherRequest) GetSyncPointInterval() time.Duration {
	return time.Duration(r.SyncPointInterval) * time.Second
}

func (r RegisterDispatcherRequest) IsOnlyReuse() bool {
	return r.OnlyReuse
}

type IOTypeT interface {
	Unmarshal(data []byte) error
	Marshal() (data []byte, err error)
}

func decodeIOType(ioType IOType, value []byte) (IOTypeT, error) {
	var m IOTypeT
	switch ioType {
	case TypeDMLEvent:
		m = &commonEvent.DMLEvent{}
	case TypeDDLEvent:
		m = &commonEvent.DDLEvent{}
	case TypeSyncPointEvent:
		m = &commonEvent.SyncPointEvent{}
	case TypeBatchResolvedTs:
		m = &commonEvent.BatchResolvedEvent{}
	case TypeHandshakeEvent:
		m = &commonEvent.HandshakeEvent{}
	case TypeReadyEvent:
		m = &commonEvent.ReadyEvent{}
	case TypeNotReusableEvent:
		m = &commonEvent.NotReusableEvent{}
	case TypeLogCoordinatorBroadcastRequest:
		m = &common.LogCoordinatorBroadcastRequest{}
	case TypeEventStoreState:
		m = &logservicepb.EventStoreState{}
	case TypeReusableEventServiceRequest:
		m = &logservicepb.ReusableEventServiceRequest{}
	case TypeReusableEventServiceResponse:
		m = &logservicepb.ReusableEventServiceResponse{}
	case TypeHeartBeatRequest:
		m = &heartbeatpb.HeartBeatRequest{}
	case TypeHeartBeatResponse:
		m = &heartbeatpb.HeartBeatResponse{}
	case TypeBlockStatusRequest:
		m = &heartbeatpb.BlockStatusRequest{}
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
	case TypeMaintainerPostBootstrapRequest:
		m = &heartbeatpb.MaintainerPostBootstrapRequest{}
	case TypeMaintainerPostBootstrapResponse:
		m = &heartbeatpb.MaintainerPostBootstrapResponse{}
	case TypeMaintainerCloseRequest:
		m = &heartbeatpb.MaintainerCloseRequest{}
	case TypeMaintainerCloseResponse:
		m = &heartbeatpb.MaintainerCloseResponse{}
	case TypeMaintainerBootstrapRequest:
		m = &heartbeatpb.MaintainerBootstrapRequest{}
	case TypeCheckpointTsMessage:
		m = &heartbeatpb.CheckpointTsMessage{}
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
	From     node.ID
	To       node.ID
	Epoch    uint64
	Sequence uint64
	Topic    string
	Type     IOType
	Message  []IOTypeT
	CreateAt time.Time
}

// NewSingleTargetMessage creates a new TargetMessage to be sent to a target server, with a single message.
func NewSingleTargetMessage(To node.ID, Topic string, Message IOTypeT) *TargetMessage {
	var ioType IOType
	switch Message.(type) {
	case *commonEvent.DMLEvent:
		ioType = TypeDMLEvent
	case *commonEvent.DDLEvent:
		ioType = TypeDDLEvent
	case *commonEvent.SyncPointEvent:
		ioType = TypeSyncPointEvent
	case *commonEvent.BatchResolvedEvent:
		ioType = TypeBatchResolvedTs
	case *commonEvent.HandshakeEvent:
		ioType = TypeHandshakeEvent
	case *commonEvent.ReadyEvent:
		ioType = TypeReadyEvent
	case *commonEvent.NotReusableEvent:
		ioType = TypeNotReusableEvent
	case *common.LogCoordinatorBroadcastRequest:
		ioType = TypeLogCoordinatorBroadcastRequest
	case *logservicepb.EventStoreState:
		ioType = TypeEventStoreState
	case *logservicepb.ReusableEventServiceRequest:
		ioType = TypeReusableEventServiceRequest
	case *logservicepb.ReusableEventServiceResponse:
		ioType = TypeReusableEventServiceResponse
	case *heartbeatpb.HeartBeatRequest:
		ioType = TypeHeartBeatRequest
	case *heartbeatpb.BlockStatusRequest:
		ioType = TypeBlockStatusRequest
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
	case *heartbeatpb.MaintainerPostBootstrapRequest:
		ioType = TypeMaintainerPostBootstrapRequest
	case *heartbeatpb.MaintainerPostBootstrapResponse:
		ioType = TypeMaintainerPostBootstrapResponse
	case *heartbeatpb.MaintainerCloseRequest:
		ioType = TypeMaintainerCloseRequest
	case *heartbeatpb.MaintainerCloseResponse:
		ioType = TypeMaintainerCloseResponse
	case *heartbeatpb.CheckpointTsMessage:
		ioType = TypeCheckpointTsMessage
	default:
		panic("unknown io type")
	}

	return &TargetMessage{
		To:       To,
		Type:     ioType,
		Topic:    Topic,
		Message:  []IOTypeT{Message},
		CreateAt: time.Now(),
	}
}

// NewBatchTargetMessage creates a new TargetMessage to be sent to a target server, with multiple messages.
// All messages in the batch should have the same type and topic.
func NewBatchTargetMessage(To node.ID, Topic string, Type IOType, Messages []IOTypeT) *TargetMessage {
	return &TargetMessage{
		To:       To,
		Type:     Type,
		Topic:    Topic,
		Message:  Messages,
		CreateAt: time.Now(),
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
