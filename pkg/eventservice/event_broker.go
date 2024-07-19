package eventservice

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// eventBroker get event from the logpuller, and send the event to the acceptors.
// Every TiDB cluster has a eventBroker.
// All span subscriptions and acceptors of the TiDB cluster are managed by the eventBroker.
type eventBroker struct {
	ctx context.Context
	// tidbClusterID is the ID of the TiDB cluster this eventStore belongs to.
	tidbClusterID uint64
	logpuller     logpuller
	msgSender     messaging.MessageSender

	// tableID -> spanSubscription
	// TODO: use table span as the key.
	spanStats map[uint64]*spanSubscription
	acceptors map[string]*acceptorStat
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
	logService logpuller,
	mc messaging.MessageSender,
) *eventBroker {
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	c := &eventBroker{
		ctx:             ctx,
		tidbClusterID:   id,
		logpuller:       logService,
		spanStats:       make(map[uint64]*spanSubscription),
		acceptors:       make(map[string]*acceptorStat),
		msgSender:       mc,
		changedSpanCh:   make(chan *common.TableSpan, defaultChanelSize),
		taskPool:        newScanTaskPool(),
		eventCh:         make(chan *common.TxnEvent, defaultChanelSize),
		scanWorkerCount: defaultWorkerCount,
		messageCh:       make(chan *messaging.TargetMessage, defaultChanelSize),
		cancel:          cancel,
		wg:              wg,
	}
	c.runGenerateScanTask()
	c.runScanWorker()
	c.runPushMessageWorker()
	return c
}

func (c *eventBroker) runGenerateScanTask() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case span := <-c.changedSpanCh:
				stat := c.spanStats[span.TableID]
				// The span may be deleted. In such case, we just the stale notification.
				if stat == nil {
					continue
				}
				startTs := stat.getScanTaskStartTs()
				endTs := stat.watermark.Load()
				dataRange := common.NewDataRange(c.tidbClusterID, span, startTs, endTs)
				task := &scanTask{
					dataRange: dataRange,
				}
				c.taskPool.pushTask(task)
			}
		}
	}()
}

func (c *eventBroker) runScanWorker() {
	for i := 0; i < c.scanWorkerCount; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-c.ctx.Done():
					return
				case task := <-c.taskPool.popTask():
					events, err := c.logpuller.Read(task)
					if err != nil {
						log.Info("read events failed", zap.Error(err))
						// push the task back to the task pool.
						c.taskPool.pushTask(&scanTask{dataRange: task})
						continue
					}
					// TODO: current we only pass a single task to the logService,
					// so that we only get a single event slice from the logService.
					event := events[0]
					spanStats := c.spanStats[task.Span.TableID]
					acceptorStats := spanStats.getAcceptorStats(task.EndTs)

					for _, ac := range acceptorStats {
						remoteID := messaging.ServerId(ac.acceptor.GetServerID())
						topic := ac.acceptor.GetTopic()

						// If the event is empty, it means no new events in the data range,
						// so we just send the watermark to the acceptor.
						if len(event) == 0 {
							waterMarkMsg := &eventpb.EventFeed{
								ResolvedTs:   task.EndTs,
								DispatcherId: ac.acceptor.GetID(),
							}
							// After all the events are sent, we send the watermark to the acceptor.
							c.messageCh <- messaging.NewTargetMessage(remoteID, topic, waterMarkMsg)
							continue
						}

						for _, e := range event {
							// Skip the events that have been sent to the acceptor.
							if e.CommitTs <= ac.watermark.Load() {
								continue
							}
							if e.IsDDLEvent() {
								msg := &eventpb.EventFeed{
									DispatcherId: ac.acceptor.GetID(),
								}
								// Send the event to the acceptor.
								c.messageCh <- messaging.NewTargetMessage(remoteID, topic, msg)
							} else {
								msg := &eventpb.EventFeed{
									TxnEvents: []*eventpb.TxnEvent{
										{
											Events:   nil,
											StartTs:  e.StartTs,
											CommitTs: e.CommitTs,
										},
									},
									DispatcherId: ac.acceptor.GetID(),
								}
								// Send the event to the acceptor.
								c.messageCh <- messaging.NewTargetMessage(remoteID, topic, msg)
							}
						}
					}
				}
			}
		}()
	}
}

func (c *eventBroker) runPushMessageWorker() {
	c.wg.Add(1)
	// Use a single goroutine to send the messages in order.
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case msg := <-c.messageCh:
				log.Info("send message", zap.Any("message", msg))
				c.msgSender.SendEvent(msg)
			}
		}
	}()
}

func (c *eventBroker) close() {
	c.cancel()
	c.wg.Wait()
}

// Store the progress of the acceptor, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this acceptor.
type acceptorStat struct {
	acceptor EventAcceptorInfo
	// The watermark of the events that have been sent to the acceptor.
	watermark atomic.Uint64
}

// spanSubscription store the latest progress of the table span in the event store.
// And it also store the acceptors that want to listen to the events of this table span.
type spanSubscription struct {
	span      *common.TableSpan
	mu        sync.RWMutex
	acceptors map[string]*acceptorStat
	// The watermark of the events that have been stored in the event store.
	watermark atomic.Uint64
	notify    chan *common.TableSpan
}

func (s *spanSubscription) GetTableSpan() *common.TableSpan {
	return s.span
}

// UpdateWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (s *spanSubscription) UpdateWatermark(watermark uint64) {
	if uint64(watermark) > s.watermark.Load() {
		s.watermark.Store(uint64(watermark))
	}
	select {
	case s.notify <- s.span:
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
		spanTask = task
		p.taskSet[task.dataRange.Span] = spanTask
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
