package eventservice

import (
	"context"

	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultChanelSize  = 2048
	defaultWorkerCount = 32
)

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type EventService interface {
	Run() error
	Close()
}

type EventAcceptorInfo interface {
	// GetID returns the ID of the acceptor.
	GetID() string
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64
	GetTopic() string
	GetServerID() string
	GetTableSpan() *common.TableSpan
	GetStartTs() uint64
	IsRegister() bool
}

type eventService struct {
	ctx        context.Context
	mc         messaging.MessageCenter
	eventStore eventstore.EventStore
	brokers    map[uint64]*eventBroker

	// TODO: use a better way to cache the acceptorInfos
	acceptorInfoCh chan EventAcceptorInfo
}

func NewEventService(ctx context.Context, mc messaging.MessageCenter, eventStore eventstore.EventStore) EventService {
	es := &eventService{
		mc:             mc,
		eventStore:     eventStore,
		ctx:            ctx,
		brokers:        make(map[uint64]*eventBroker),
		acceptorInfoCh: make(chan EventAcceptorInfo, defaultChanelSize*16),
	}
	es.mc.RegisterHandler(messaging.EventServiceTopic, es.handleMessage)
	return es
}

func (s *eventService) Run() error {
	log.Info("start event service")
	for {
		select {
		case <-s.ctx.Done():
			log.Info("event service exited")
			return nil
		case info := <-s.acceptorInfoCh:
			if info.IsRegister() {
				s.registerAcceptor(info)
			} else {
				s.deregisterAcceptor(info.GetClusterID(), info.GetID())
			}
		}
	}
}

func (s *eventService) Close() {
	log.Info("event service is closing")
	for _, c := range s.brokers {
		c.close()
	}
	log.Info("event service is closed")
}

func (s *eventService) handleMessage(msg *messaging.TargetMessage) error {
	acceptorInfo := msgToAcceptorInfo(msg)
	s.acceptorInfoCh <- acceptorInfo
	return nil
}

func (s *eventService) registerAcceptor(acceptor EventAcceptorInfo) {
	clusterID := acceptor.GetClusterID()
	startTs := acceptor.GetStartTs()
	span := acceptor.GetTableSpan()

	c, ok := s.brokers[clusterID]
	if !ok {
		c = newEventBroker(s.ctx, clusterID, s.eventStore, s.mc)
		s.brokers[clusterID] = c
	}

	spanSub := &spanSubscription{
		span: span,
	}
	spanSub.watermark.Store(uint64(startTs))

	// add the acceptor to the cluster.
	ac := &acceptorStat{
		acceptor:         acceptor,
		spanSubscription: spanSub,
		notify:           c.changedAcceptor,
	}
	ac.watermark.Store(uint64(startTs))
	c.acceptors[acceptor.GetID()] = ac
	c.eventStore.RegisterDispatcher(
		acceptor.GetID(),
		acceptor.GetTableSpan(),
		common.Ts(acceptor.GetStartTs()),
		ac.UpdateEventCount,
		ac.UpdateWatermark,
	)
	log.Info("register acceptor", zap.Uint64("clusterID", clusterID), zap.String("acceptorID", acceptor.GetID()))
}

func (s *eventService) deregisterAcceptor(clusterID uint64, accepterID string) {
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	_, ok = c.acceptors[accepterID]
	if !ok {
		return
	}
	//TODO: release the resources of the acceptor.
	delete(c.acceptors, accepterID)
	log.Info("deregister acceptor", zap.Uint64("clusterID", clusterID), zap.String("acceptorID", accepterID))
}

// TODO: implement the following functions
func msgToAcceptorInfo(msg *messaging.TargetMessage) EventAcceptorInfo {
	return msg.Message.(messaging.RegisterDispatcherRequest)
}
