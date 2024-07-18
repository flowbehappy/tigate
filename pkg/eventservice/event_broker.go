package eventservice

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// eventBroker get event from the eventSource, and send the event to the acceptors.
// Every TiDB cluster has a eventBroker.
// All span subscriptions and acceptors of the TiDB cluster are managed by the eventBroker.
type eventBroker struct {
	ctx context.Context
	// tidbClusterID is the ID of the TiDB cluster this eventStore belongs to.
	tidbClusterID uint64
	eventStore    eventstore.EventStore
	msgSender     messaging.MessageSender

	acceptors map[string]*acceptorStat
	// changedSpanCh is used to notify some acceptors may have new events.
	changedAcceptor chan *acceptorStat
	taskPool        *scanTaskPool

	eventCh         chan *common.TxnEvent
	scanWorkerCount int

	messageCh chan *messaging.TargetMessage
	wg        *sync.WaitGroup
	cancel    context.CancelFunc
}

func newEventBroker(
	ctx context.Context,
	id uint64,
	eventStore eventstore.EventStore,
	mc messaging.MessageSender,
) *eventBroker {
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	c := &eventBroker{
		ctx:             ctx,
		tidbClusterID:   id,
		eventStore:      eventStore,
		acceptors:       make(map[string]*acceptorStat),
		msgSender:       mc,
		changedAcceptor: make(chan *acceptorStat, defaultChanelSize),
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
			case acceptor := <-c.changedAcceptor:
				acceptor, ok := c.acceptors[acceptor.acceptor.GetID()]
				// The acceptor may be deleted. In such case, we just the stale notification.
				if !ok {
					continue
				}
				startTs := acceptor.watermark.Load()
				endTs := acceptor.spanSubscription.watermark.Load()
				dataRange := common.NewDataRange(c.tidbClusterID, acceptor.acceptor.GetTableSpan(), startTs, endTs)
				task := &scanTask{
					acceptorStat: acceptor,
					dataRange:    dataRange,
					eventCount:   acceptor.GetAndResetNewEventCount(),
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
					remoteID := messaging.ServerId(task.acceptorStat.acceptor.GetServerID())
					topic := task.acceptorStat.acceptor.GetTopic()
					dispatcherID := task.acceptorStat.acceptor.GetID()

					// The acceptor has no new events. In such case, we don't need to scan the event store.
					if task.eventCount == 0 {
						// After all the events are sent, we send the watermark to the acceptor.
						c.messageCh <- messaging.NewTargetMessage(remoteID, topic, waterMarkMsg)
						task.acceptorStat.watermark.Store(task.dataRange.EndTs)
						continue
					}

					// scan the event store to get the events in the data range.
					events, err := c.eventStore.GetIterator(task.dataRange)
					if err != nil {
						log.Info("read events failed", zap.Error(err))
						// push the task back to the task pool.
						c.taskPool.pushTask(task)
						continue
					}

					// TODO: current we only pass a single task to the logService,
					// so that we only get a single event slice from the logService.
					event := events[0]
					// If the event is empty, it means no new events in the data range,
					// so we just send the watermark to the acceptor.
					if len(event) == 0 {
						waterMarkMsg := &eventpb.EventFeed{
							ResolvedTs:   task.dataRange.EndTs,
							DispatcherId: dispatcherID,
						}
						// After all the events are sent, we send the watermark to the acceptor.
						c.messageCh <- messaging.NewTargetMessage(remoteID, topic, waterMarkMsg)
						task.acceptorStat.watermark.Store(task.dataRange.EndTs)
					}

					for _, e := range event {
						// Skip the events that have been sent to the acceptor.
						if e.CommitTs <= task.acceptorStat.watermark.Load() {
							continue
						}
						if e.IsDDLEvent() {
							msg := &eventpb.EventFeed{
								DispatcherId: dispatcherID,
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
								DispatcherId: dispatcherID,
							}
							// Send the event to the acceptor.
							c.messageCh <- messaging.NewTargetMessage(remoteID, topic, msg)
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
	spanSubscription *spanSubscription
	acceptor         EventAcceptorInfo
	// The watermark of the events that have been sent to the acceptor.
	watermark atomic.Uint64
	notify    chan *acceptorStat
}

// UpdateWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (a *acceptorStat) UpdateWatermark(watermark uint64) {
	if uint64(watermark) > a.spanSubscription.watermark.Load() {
		a.spanSubscription.watermark.Store(uint64(watermark))
	}
	select {
	case a.notify <- a:
	default:
	}
}

// TODO: consider to use a better way to update the event count, may be we only need to
// know there are new events, and we don't need to know the exact number of the new events.
// So we can reduce the contention of the lock.
func (a *acceptorStat) UpdateEventCount(raw *common.RawKVEntry) {
	a.spanSubscription.newEventCount.mu.Lock()
	defer a.spanSubscription.newEventCount.mu.Unlock()
	a.spanSubscription.newEventCount.v++
}

func (a *acceptorStat) GetAndResetNewEventCount() uint64 {
	a.spanSubscription.newEventCount.mu.Lock()
	defer a.spanSubscription.newEventCount.mu.Unlock()
	v := a.spanSubscription.newEventCount.v
	a.spanSubscription.newEventCount.v = 0
	return v
}

// spanSubscription store the latest progress of the table span in the event store.
// And it also store the acceptors that want to listen to the events of this table span.
type spanSubscription struct {
	span *common.TableSpan
	// The watermark of the events that have been stored in the event store.
	watermark atomic.Uint64

	newEventCount struct {
		mu sync.Mutex
		v  uint64
	}
}

type acceptorChange struct {
	acceptor EventAcceptorInfo
	count    uint64
}

type scanTask struct {
	acceptorStat *acceptorStat
	dataRange    *common.DataRange
	eventCount   uint64
}

type scanTaskPool struct {
	mu sync.Mutex
	// taskSet is used to merge the tasks with the same table span.
	taskSet map[string]*scanTask
	// fifoQueue is used to store the tasks that have new changes.
	fifoQueue chan *scanTask
}

func newScanTaskPool() *scanTaskPool {
	return &scanTaskPool{
		taskSet:   make(map[string]*scanTask),
		fifoQueue: make(chan *scanTask, defaultChanelSize),
	}
}

// addTask adds a task to the pool, and merge the task if the task is overlapped with the existing tasks.
func (p *scanTaskPool) pushTask(task *scanTask) {
	p.mu.Lock()
	defer p.mu.Unlock()
	spanTask := p.taskSet[task.acceptorStat.acceptor.GetID()]

	if spanTask == nil {
		spanTask = task
		p.taskSet[task.acceptorStat.acceptor.GetID()] = spanTask
	}

	// Merge the task into the existing task.
	mergedRange := task.dataRange.Merge(spanTask.dataRange)
	spanTask.dataRange = mergedRange
	spanTask.eventCount += task.eventCount
	// Update the existing task.
	select {
	case p.fifoQueue <- spanTask:
		p.taskSet[task.acceptorStat.acceptor.GetID()] = nil
	default:
	}
}

func (p *scanTaskPool) popTask() <-chan *scanTask {
	return p.fifoQueue
}
