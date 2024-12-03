package eventservice

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/messaging"
	"go.uber.org/zap"
)

type DispatcherInfo interface {
	// GetID returns the ID of the dispatcher.
	GetID() common.DispatcherID
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64
	GetTopic() string
	GetServerID() string
	GetTableSpan() *heartbeatpb.TableSpan
	GetStartTs() uint64
	GetActionType() eventpb.ActionType
	GetChangefeedID() common.ChangeFeedID
	GetFilter() filter.Filter

	// sync point related
	SyncPointEnabled() bool
	GetSyncPointTs() uint64
	GetSyncPointInterval() time.Duration

	IsOnlyReuse() bool
}

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type eventService struct {
	mc messaging.MessageCenter

	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore

	// clusterID -> eventBrokers
	brokerNum int
	brokers   map[uint64][]*eventBroker

	// TODO: use a better way to cache the acceptorInfos
	dispatcherInfo chan DispatcherInfo
	tz             *time.Location
}

func New(eventStore eventstore.EventStore, schemaStore schemastore.SchemaStore) common.SubModule {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	es := &eventService{
		mc:             mc,
		schemaStore:    schemaStore,
		eventStore:     eventStore,
		brokerNum:      16,
		dispatcherInfo: make(chan DispatcherInfo, defaultChannelSize*16),
		tz:             time.Local, // FIXME use the timezone from the config
	}
	es.brokers = make(map[uint64][]*eventBroker)
	es.mc.RegisterHandler(messaging.EventServiceTopic, es.handleMessage)
	return es
}

func (s *eventService) Name() string {
	return appcontext.EventService
}

func (s *eventService) Run(ctx context.Context) error {
	log.Info("start event service")
	for {
		select {
		case <-ctx.Done():
			log.Info("event service exited")
			return nil
		case info := <-s.dispatcherInfo:
			switch info.GetActionType() {
			case eventpb.ActionType_ACTION_TYPE_REGISTER:
				s.registerDispatcher(ctx, info)
			case eventpb.ActionType_ACTION_TYPE_REMOVE:
				s.deregisterDispatcher(info)
			case eventpb.ActionType_ACTION_TYPE_PAUSE:
				s.pauseDispatcher(info)
			case eventpb.ActionType_ACTION_TYPE_RESUME:
				s.resumeDispatcher(info)
			case eventpb.ActionType_ACTION_TYPE_RESET:
				s.resetDispatcher(info)
			default:
				log.Panic("invalid action type", zap.Any("info", info))
			}
		}
	}
}

func (s *eventService) Close(_ context.Context) error {
	log.Info("event service is closing")
	for _, brokers := range s.brokers {
		for _, broker := range brokers {
			broker.close()
		}
	}
	log.Info("event service is closed")
	return nil
}

func (s *eventService) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	infos := msgToDispatcherInfo(msg)
	for _, info := range infos {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.dispatcherInfo <- info:
		}
	}
	return nil
}

func (s *eventService) getBroker(clusterID uint64, tableID int64) *eventBroker {
	brokerIndex := tableID % int64(s.brokerNum)

	brokers, ok := s.brokers[clusterID]
	if !ok {
		return nil
	}
	return brokers[brokerIndex]
}

func (s *eventService) registerDispatcher(ctx context.Context, info DispatcherInfo) {
	clusterID := info.GetClusterID()
	brokers, ok := s.brokers[clusterID]
	if !ok {
		for i := 0; i < s.brokerNum; i++ {
			broker := newEventBroker(ctx, clusterID, s.eventStore, s.schemaStore, s.mc, s.tz)
			brokers = append(brokers, broker)
		}
		s.brokers[clusterID] = brokers
	}

	broker := s.getBroker(clusterID, info.GetTableSpan().TableID)
	broker.addDispatcher(info)
}

func (s *eventService) deregisterDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	broker := s.getBroker(clusterID, dispatcherInfo.GetTableSpan().TableID)
	if broker == nil {
		return
	}
	broker.removeDispatcher(dispatcherInfo)
}

func (s *eventService) pauseDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	broker := s.getBroker(clusterID, dispatcherInfo.GetTableSpan().TableID)
	if broker == nil {
		return
	}
	broker.pauseDispatcher(dispatcherInfo)
}

func (s *eventService) resumeDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	broker := s.getBroker(clusterID, dispatcherInfo.GetTableSpan().TableID)
	if broker == nil {
		return
	}
	broker.resumeDispatcher(dispatcherInfo)
}

func (s *eventService) resetDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	broker := s.getBroker(clusterID, dispatcherInfo.GetTableSpan().TableID)
	if broker == nil {
		return
	}
	broker.resetDispatcher(dispatcherInfo)
}

func msgToDispatcherInfo(msg *messaging.TargetMessage) []DispatcherInfo {
	res := make([]DispatcherInfo, 0, len(msg.Message))
	for _, m := range msg.Message {
		info, ok := m.(*messaging.RegisterDispatcherRequest)
		if !ok {
			log.Panic("invalid dispatcher info", zap.Any("info", m))
		}
		res = append(res, info)
	}
	return res
}
