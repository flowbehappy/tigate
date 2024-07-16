package eventservice

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultChanelSize  = 2048
	defaultWorkerCount = 32
)

type GlobalID uuid.UUID

func (id GlobalID) String() string {
	return uuid.UUID(id).String()
}

type LogService interface {
	// SubscribeTableSpan subscribes the table span, and returns the latest progress of the table span.
	// afterUpdate is called when the watermark of the table span is updated.
	SubscribeTableSpan(span *common.TableSpan, startTs uint64, onSpanUpdate func(watermark uint64)) (common.Ts, error)
	// Read return the event of the data range.
	Read(dataRange *common.DataRange) ([]*common.TxnEvent, error)
}

type EventAcceptor interface {
	// GetID returns the ID of the acceptor.
	GetID() GlobalID
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64
	GetTopic() string
	GetServerID() GlobalID
	GetTableSpan() *common.TableSpan
	GetStartTs() common.Ts
}

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type EventService interface {
	//TODO: Be careful that the a same acceptor may be registered multiple times, think about how to handle this case.
	RegisterAcceptor(acceptor EventAcceptor) error
	DeregisterAcceptor(clusterID uint64, accepterID GlobalID) error
}

type TableSpanStat interface {
	GetTableSpan() common.TableSpan
	UpdateWatermark(watermark common.Ts)
}

// Store the progress of the acceptor, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this acceptor.
type acceptorStat struct {
	acceptor EventAcceptor
	// The watermark of the events that have been sent to the acceptor.
	watermark atomic.Uint64
}

// spanSubscription store the latest progress of the table span in the event store.
// And it also store the acceptors that want to listen to the events of this table span.
type spanSubscription struct {
	span      *common.TableSpan
	mu        sync.RWMutex
	acceptors map[GlobalID]*acceptorStat
	// The watermark of the events that have been stored in the event store.
	watermark atomic.Uint64
	notify    chan struct{}
}

func (s *spanSubscription) GetTableSpan() *common.TableSpan {
	return s.span
}

// UpdateWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (s *spanSubscription) UpdateWatermark(watermark common.Ts) {
	if uint64(watermark) > s.watermark.Load() {
		s.watermark.Store(uint64(watermark))
	}
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *spanSubscription) addAcceptor(ac *acceptorStat) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acceptors[ac.acceptor.GetID()] = ac
}

// getScanTaskStartTs calculates the startTs of the table span.
// The startTs of the table span is the minimum of the watermarks of all the acceptors.
func (s *spanSubscription) getScanTaskStartTs() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	startTs := s.watermark.Load()
	for _, ac := range s.acceptors {
		ts := ac.watermark.Load()
		if ts < startTs {
			startTs = ts
		}
	}
	return startTs
}

func (s *spanSubscription) getAcceptorStats(ts uint64) []*acceptorStat {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var acceptors []*acceptorStat
	for _, ac := range s.acceptors {
		if ac.watermark.Load() < ts {
			acceptors = append(acceptors, ac)
		}
	}
	return acceptors
}

type scanTask struct {
	dataRange *common.DataRange
}

func singleScanTaskLess(a, b *scanTask) bool {
	return a.dataRange.Span.Less(b.dataRange.Span)
}

// cluster represents a TiDB cluster.
type cluster struct {
	ctx        context.Context
	id         uint64
	logService LogService
	spanStats  map[*common.TableSpan]*spanSubscription
	acceptors  map[GlobalID]*acceptorStat
	mc         messaging.MessageSender
	// changedSpanCh is used to notify some tableSpan has new events.
	changedSpanCh chan *common.TableSpan
	taskPool      *scanTaskPool

	eventCh         chan *common.TxnEvent
	scanWorkerCount int

	messageCh chan *messaging.TargetMessage
	wg        *sync.WaitGroup
	cancel    context.CancelFunc
}

func newCluster(
	ctx context.Context,
	id uint64,
	logService LogService,
	mc messaging.MessageSender,
) *cluster {
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	c := &cluster{
		ctx:             ctx,
		id:              id,
		logService:      logService,
		spanStats:       make(map[*common.TableSpan]*spanSubscription),
		acceptors:       make(map[GlobalID]*acceptorStat),
		mc:              mc,
		changedSpanCh:   make(chan *common.TableSpan, defaultChanelSize),
		taskPool:        newScanTaskPool(),
		eventCh:         make(chan *common.TxnEvent, defaultChanelSize),
		scanWorkerCount: defaultWorkerCount,
		messageCh:       make(chan *messaging.TargetMessage, defaultChanelSize),
		cancel:          cancel,
		wg:              wg,
	}
	c.runGenerateScanTask(ctx, wg)
	c.runScanWorker(ctx, wg)
	c.runPushMessageWorker(ctx, wg)
	return c
}

func (c *cluster) runGenerateScanTask(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case span := <-c.changedSpanCh:
				stat := c.spanStats[span]
				// The span may be deleted. In such case, we just the stale notification.
				if stat == nil {
					continue
				}
				startTs := stat.getScanTaskStartTs()
				endTs := stat.watermark.Load()
				dataRange := common.NewDataRange(c.id, span, startTs, endTs)
				task := &scanTask{
					dataRange: dataRange,
				}
				c.taskPool.pushTask(task)
			}
		}
	}()
}

func (c *cluster) runScanWorker(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < c.scanWorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-c.taskPool.popTask():
					event, err := c.logService.Read(task)
					if err != nil {
						log.Info("read events failed", zap.Error(err))
						// push the task back to the task pool.
						c.taskPool.pushTask(&scanTask{dataRange: task})
						continue
					}
					spanStats := c.spanStats[task.Span]
					acceptorStats := spanStats.getAcceptorStats(task.EndTs)
					for _, ac := range acceptorStats {
						remoteID := messaging.ServerId(ac.acceptor.GetServerID())
						topic := ac.acceptor.GetTopic()
						for _, e := range event {
							if e.CommitTs <= ac.watermark.Load() {
								continue
							}
							var evenType messaging.IOType
							if e.IsDDLEvent() {
								evenType = messaging.TypeDDLEvent
							} else {
								evenType = messaging.TypeDMLEvent
							}
							// Send the event to the acceptor.
							c.messageCh <- messaging.NewTargetMessage(remoteID, topic, evenType, e)
						}
						// After all the events are sent, we send the watermark to the acceptor.
						c.messageCh <- messaging.NewTargetMessage(remoteID, topic, messaging.TypeWaterMark, spanStats.watermark.Load())
					}
				}
			}
		}()
	}
}

func (c *cluster) runPushMessageWorker(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-c.messageCh:
				c.mc.SendEvent(msg)
			}
		}
	}()
}

func (c *cluster) close() {
	c.cancel()
	c.wg.Wait()
}

type scanTaskPool struct {
	mu sync.Mutex
	// taskSet is used to merge the tasks with the same table span.
	taskSet map[*common.TableSpan]*scanTask
	// fifoQueue is used to store the tasks that have new changes.
	fifoQueue chan *common.DataRange
}

func newScanTaskPool() *scanTaskPool {
	return &scanTaskPool{
		taskSet:   make(map[*common.TableSpan]*scanTask),
		fifoQueue: make(chan *common.DataRange, defaultChanelSize),
	}
}

// addTask adds a task to the pool, and merge the task if the task is overlapped with the existing tasks.
func (p *scanTaskPool) pushTask(task *scanTask) {
	p.mu.Lock()
	defer p.mu.Unlock()
	spanTask, ok := p.taskSet[task.dataRange.Span]
	if !ok {
		p.taskSet[task.dataRange.Span] = task
		return
	}
	// Merge the task into the existing task.
	mergedRange := task.dataRange.Merge(spanTask.dataRange)
	// Update the existing task.
	select {
	case p.fifoQueue <- mergedRange:
		spanTask.dataRange = nil
	default:
		// The queue is full, we just update the task.
		spanTask.dataRange = mergedRange
	}
}

func (p *scanTaskPool) popTask() <-chan *common.DataRange {
	return p.fifoQueue
}

type eventService struct {
	ctx           context.Context
	messageSender messaging.MessageSender
	logService    LogService
	cluster       map[uint64]*cluster
}

func NewEventService(ctx context.Context, sender messaging.MessageSender, logService LogService) EventService {
	return &eventService{
		messageSender: sender,
		logService:    logService,
		ctx:           ctx,
		cluster:       make(map[uint64]*cluster),
	}
}

func (s *eventService) RegisterAcceptor(acceptor EventAcceptor) error {
	clusterID := acceptor.GetClusterID()
	startTs := acceptor.GetStartTs()
	span := acceptor.GetTableSpan()

	c, ok := s.cluster[clusterID]
	if !ok {
		c := newCluster(s.ctx, clusterID, s.logService, s.messageSender)
		s.cluster[clusterID] = c
	}
	// add the acceptor to the cluster.
	ac := &acceptorStat{
		acceptor: acceptor,
	}
	ac.watermark.Store(uint64(startTs))
	c.acceptors[acceptor.GetID()] = ac

	// add the acceptor to the table span it wants to listen.
	stat, ok := c.spanStats[span]
	if !ok {
		stat = &spanSubscription{
			span:      acceptor.GetTableSpan(),
			acceptors: make(map[GlobalID]*acceptorStat),
		}
		c.spanStats[span] = stat
	}
	stat.addAcceptor(ac)
	return nil
}

func (s *eventService) DeregisterAcceptor(clusterID uint64, accepterID GlobalID) error {
	c, ok := s.cluster[clusterID]
	if !ok {
		return nil
	}
	_, ok = c.acceptors[accepterID]
	if !ok {
		return nil
	}
	//TODO: release the resources of the acceptor.
	delete(c.acceptors, accepterID)
	return nil
}

func (s *eventService) Close() {
	for _, c := range s.cluster {
		c.close()
	}
}
