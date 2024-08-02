package eventservice

import (
	"context"

	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultChannelSize = 1024
	// TODO: need to adjust the worker count
	defaultWorkerCount = 8192
)

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type EventService interface {
	Name() string
	Run(ctx context.Context) error
	Close(context.Context) error
}

type DispatcherInfo interface {
	// GetID returns the ID of the dispatcher.
	GetID() string
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64

	GetTopic() common.TopicType
	GetServerID() string
	GetTableSpan() *common.TableSpan
	GetStartTs() uint64
	IsRegister() bool
	GetChangefeedID() (namespace, id string)
}

type eventService struct {
	mc         messaging.MessageCenter
	eventStore eventstore.EventStore
	brokers    map[uint64]*eventBroker

	// TODO: use a better way to cache the acceptorInfos
	acceptorInfoCh chan DispatcherInfo
}

func NewEventService() EventService {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	eventStore := appcontext.GetService[eventstore.EventStore](appcontext.EventStore)

	es := &eventService{
		mc:             mc,
		eventStore:     eventStore,
		brokers:        make(map[uint64]*eventBroker),
		acceptorInfoCh: make(chan DispatcherInfo, defaultChannelSize*16),
	}
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
		case info := <-s.acceptorInfoCh:
			if info.IsRegister() {
				s.registerDispatcher(ctx, info)
			} else {
				s.deregisterAcceptor(info)
			}
		}
	}
}

func (s *eventService) Close(_ context.Context) error {
	log.Info("event service is closing")
	for _, c := range s.brokers {
		c.close()
	}
	log.Info("event service is closed")
	return nil
}

func (s *eventService) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.acceptorInfoCh <- msgToAcceptorInfo(msg):
	}
	return nil
}

func (s *eventService) registerDispatcher(ctx context.Context, info DispatcherInfo) {
	clusterID := info.GetClusterID()
	startTs := info.GetStartTs()
	span := info.GetTableSpan()

	c, ok := s.brokers[clusterID]
	if !ok {
		c = newEventBroker(ctx, clusterID, s.eventStore, s.mc)
		s.brokers[clusterID] = c
	}

	dispatcher := newDispatcherStat(startTs, info, c.changedCh)
	c.dispatchers.mu.Lock()
	c.dispatchers.m[info.GetID()] = dispatcher
	c.dispatchers.mu.Unlock()

	id := uuid.MustParse(info.GetID())

	c.eventStore.RegisterDispatcher(
		common.DispatcherID(id),
		span,
		common.Ts(info.GetStartTs()),
		dispatcher.onNewEvent,
		dispatcher.onSubscriptionWatermark,
	)
	log.Info("register acceptor", zap.Uint64("clusterID", clusterID), zap.String("acceptorID", info.GetID()), zap.Uint64("tableID", span.TableID), zap.Uint64("startTs", startTs))
}

func (s *eventService) deregisterAcceptor(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	id := dispatcherInfo.GetID()
	c.removeDispatcher(id)
	log.Info("deregister acceptor", zap.Uint64("clusterID", clusterID), zap.String("acceptorID", id))
}

// TODO: implement the following functions
func msgToAcceptorInfo(msg *messaging.TargetMessage) DispatcherInfo {
	return msg.Message.(messaging.RegisterDispatcherRequest)
}
