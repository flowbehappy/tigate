package eventservice

import (
	"context"

	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

const (
	defaultChannelSize = 1024
	// TODO: need to adjust the worker count
	defaultScanWorkerCount = 8192
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
	GetID() common.DispatcherID
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64
	GetTopic() string
	GetServerID() string
	GetTableSpan() *common.TableSpan
	GetStartTs() uint64
	IsRegister() bool
	GetChangefeedID() (namespace, id string)
	GetFilterConfig() *config.FilterConfig
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
				s.deregisterDispatcher(info)
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
	infos := msgToDispatcherInfo(msg)
	for _, info := range infos {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.acceptorInfoCh <- info:
		}
	}
	return nil
}

func (s *eventService) registerDispatcher(ctx context.Context, info DispatcherInfo) {
	clusterID := info.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		c = newEventBroker(ctx, clusterID, s.eventStore, s.mc)
		s.brokers[clusterID] = c
	}
	c.addDispatcher(info)
}

func (s *eventService) deregisterDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	c.removeDispatcher(dispatcherInfo)
}

func msgToDispatcherInfo(msg *messaging.TargetMessage) []DispatcherInfo {
	res := make([]DispatcherInfo, 0, len(msg.Message))
	for _, m := range msg.Message {
		info, ok := m.(messaging.RegisterDispatcherRequest)
		if !ok {
			log.Panic("invalid dispatcher info", zap.Any("info", m))
		}
		res = append(res, info)
	}
	return res
}
