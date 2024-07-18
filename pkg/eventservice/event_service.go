package eventservice

import (
	"context"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultChannelSize = 2048
	defaultWorkerCount = 32
)

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type EventService interface {
	Run() error
	Close()
}

type EventSource interface {
	// SubscribeTableSpan subscribes the table span, and returns the latest progress of the table span.
	// afterUpdate is called when the watermark of the table span is updated.
	SubscribeTableSpan(span *common.TableSpan, startTs uint64, onSpanUpdate func(watermark uint64)) (uint64, error)
	// Read return the event of the data range.
	Read(dataRange ...*common.DataRange) ([][]*common.TxnEvent, error)
}

type EventAcceptorInfo interface {
	// GetID returns the ID of the acceptor.
	GetID() string
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64

	GetTopic() common.TopicType
	GetServerID() string
	GetTableSpan() *common.TableSpan
	GetStartTs() uint64
	IsRegister() bool
}

type eventService struct {
	ctx         context.Context
	mc          messaging.MessageCenter
	eventSource EventSource
	stores      map[uint64]*eventBroker

	// TODO: use a better way to cache the acceptorInfos
	acceptorInfoCh chan EventAcceptorInfo
}

func NewEventService(ctx context.Context, mc messaging.MessageCenter, eventSource EventSource) EventService {
	es := &eventService{
		mc:             mc,
		eventSource:    eventSource,
		ctx:            ctx,
		stores:         make(map[uint64]*eventBroker),
		acceptorInfoCh: make(chan EventAcceptorInfo, defaultChannelSize*16),
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
				s.deregisterAcceptor(info)
			}
		}
	}
}

func (s *eventService) Close() {
	log.Info("event service is closing")
	for _, c := range s.stores {
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

	c, ok := s.stores[clusterID]
	if !ok {
		c = newCluster(s.ctx, clusterID, s.eventSource, s.mc)
		s.stores[clusterID] = c
	}
	// add the acceptor to the cluster.
	ac := &acceptorStat{
		acceptor: acceptor,
	}

	ac.watermark.Store(startTs)
	c.acceptors[acceptor.GetID()] = ac

	// add the acceptor to the table span it wants to listen.
	stat, ok := c.spanStats[span.TableID]
	if !ok {
		stat = &spanSubscription{
			span:      span,
			acceptors: make(map[string]*acceptorStat),
			notify:    c.changedSpanCh,
		}
		stat.watermark.Store(startTs)
		c.spanStats[span.TableID] = stat
	}
	stat.addAcceptor(ac)
	c.eventSource.SubscribeTableSpan(span, startTs, stat.UpdateWatermark)

	log.Info("register acceptor", zap.Uint64("clusterID", clusterID), zap.String("acceptorID", acceptor.GetID()))
}

func (s *eventService) deregisterAcceptor(acceptor EventAcceptorInfo) {
	clusterID := acceptor.GetClusterID()
	c, ok := s.stores[clusterID]
	if !ok {
		return
	}
	acceptorID := acceptor.GetID()
	_, ok = c.acceptors[acceptorID]
	if !ok {
		return
	}
	//TODO: release the resources of the acceptor.
	delete(c.acceptors, acceptorID)
	log.Info("deregister acceptor", zap.Uint64("clusterID", clusterID), zap.String("acceptorID", acceptorID))
}

// TODO: implement the following functions
func msgToAcceptorInfo(msg *messaging.TargetMessage) EventAcceptorInfo {
	return msg.Message.(messaging.RegisterDispatcherRequest)
}
