package eventservice

import (
	"context"
	"sync"
	"sync/atomic"

	"hash/crc32"

	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// eventBroker get event from the eventStore, and send the event to the dispatchers.
// Every TiDB cluster has a eventBroker.
// All span subscriptions and dispatchers of the TiDB cluster are managed by the eventBroker.
type eventBroker struct {
	// tidbClusterID is the ID of the TiDB cluster this eventStore belongs to.
	tidbClusterID uint64
	// eventStore is the source of the events, eventBroker get the events from the eventStore.
	eventStore eventstore.EventStore
	// msgSender is used to send the events to the dispatchers.
	msgSender messaging.MessageSender

	// All the dispatchers that register to the eventBroker.
	dispatchers struct {
		mu sync.RWMutex
		m  map[string]*dispatcherStat
	}
	// changedCh is used to notify span subscription has new events.
	changedCh chan *subscriptionChange
	// taskPool is used to store the scan tasks and merge the tasks of same dispatcher.
	// TODO: Make it support merge the tasks of the same table span, even if the tasks are from different dispatchers.
	taskPool *scanTaskPool

	// scanWorkerCount is the number of the scan workers to spawn.
	scanWorkerCount int

	// messageCh is used to receive message from the scanWorker,
	// and a goroutine is responsible for sending the message to the dispatchers.
	messageCh chan *messaging.TargetMessage
	// wg is used to spawn the goroutines.
	wg *sync.WaitGroup
	// cancel is used to cancel the goroutines spawned by the eventBroker.
	cancel context.CancelFunc
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
		tidbClusterID: id,
		eventStore:    eventStore,
		dispatchers: struct {
			mu sync.RWMutex
			m  map[string]*dispatcherStat
		}{m: make(map[string]*dispatcherStat)},
		msgSender:       mc,
		changedCh:       make(chan *subscriptionChange, defaultChannelSize),
		taskPool:        newScanTaskPool(),
		scanWorkerCount: defaultWorkerCount,
		messageCh:       make(chan *messaging.TargetMessage, defaultChannelSize),
		cancel:          cancel,
		wg:              wg,
	}
	c.runGenerateScanTask(ctx)
	c.runScanWorker(ctx)
	c.runPushMessageWorker(ctx)
	return c
}

func (c *eventBroker) runGenerateScanTask(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case change := <-c.changedCh:
				c.dispatchers.mu.RLock()
				dispatcher, ok := c.dispatchers.m[change.dispatcherInfo.GetID()]
				c.dispatchers.mu.RUnlock()
				// The dispatcher may be deleted. In such case, we just the stale notification.
				if !ok {
					continue
				}
				startTs := dispatcher.watermark.Load()
				endTs := dispatcher.spanSubscription.watermark.Load()
				dataRange := common.NewDataRange(c.tidbClusterID, dispatcher.info.GetTableSpan(), startTs, endTs)
				task := &scanTask{
					dispatcherStat: dispatcher,
					dataRange:      dataRange,
					eventCount:     change.eventCount,
				}
				c.taskPool.pushTask(task)
			}
		}
	}()
}

func (c *eventBroker) runScanWorker(ctx context.Context) {
	for i := 0; i < c.scanWorkerCount; i++ {
		chIndex := i
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-c.taskPool.popTask(chIndex):
					remoteID := messaging.ServerId(task.dispatcherStat.info.GetServerID())
					topic := task.dispatcherStat.info.GetTopic()
					dispatcherID := task.dispatcherStat.info.GetID()
					// The dispatcher has no new events. In such case, we don't need to scan the event store.
					// We just send the watermark to the dispatcher.
					if task.eventCount == 0 {
						waterMarkMsg := &common.TxnEvent{
							DispatcherID: dispatcherID,
							ResolvedTs:   task.dataRange.EndTs,
						}
						c.messageCh <- messaging.NewTargetMessage(remoteID, topic, waterMarkMsg)
						task.dispatcherStat.watermark.Store(task.dataRange.EndTs)
						continue
					}

					// scan the event store to get the events in the data range.
					//events, err := c.eventStore.GetIterator(task.dataRange)
					iter, err := c.eventStore.GetIterator(task.dataRange)
					if err != nil {
						log.Info("read events failed, push the task back to queue", zap.Error(err))
						// push the task back to the task pool.
						c.taskPool.pushTask(task)
						continue
					}
					defer iter.Close()

					var txnEvent *common.TxnEvent
					isFirstEvent := true
					for {
						// The first event of the txn must return isNewTxn as true.
						e, isNewTxn, err := iter.Next()
						if err != nil {
							log.Panic("read events failed", zap.Error(err))
						}

						if e == nil {
							if txnEvent != nil {
								// Send the last txnEvent to the dispatcher.
								c.messageCh <- messaging.NewTargetMessage(remoteID, topic, txnEvent)
								task.dispatcherStat.watermark.Store(txnEvent.CommitTs)
							}

							// After all the events are sent, we send the watermark to the dispatcher.
							watermark := &common.TxnEvent{
								ResolvedTs:   task.dataRange.EndTs,
								DispatcherID: dispatcherID,
							}
							c.messageCh <- messaging.NewTargetMessage(remoteID, topic, watermark)
							task.dispatcherStat.watermark.Store(task.dataRange.EndTs)
							break
						}

						if isFirstEvent || isNewTxn {
							// Send the previous txnEvent to the dispatcher.
							if txnEvent != nil {
								c.messageCh <- messaging.NewTargetMessage(remoteID, topic, txnEvent)
								task.dispatcherStat.watermark.Store(txnEvent.CommitTs)
							}
							// Create a new txnEvent.
							txnEvent = &common.TxnEvent{
								DispatcherID: dispatcherID,
								StartTs:      e.StartTs,
								CommitTs:     e.CommitTs,
								Rows:         make([]*common.RowChangedEvent, 0),
							}
							isFirstEvent = false
						}

						// Skip the events that have been sent to the dispatcher.
						if e.CommitTs <= task.dispatcherStat.watermark.Load() {
							continue
						}

						txnEvent.Rows = append(txnEvent.Rows, e)
					}
				}
			}
		}()
	}
}

func (c *eventBroker) runPushMessageWorker(ctx context.Context) {
	c.wg.Add(1)
	// Use a single goroutine to send the messages in order.
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-c.messageCh:
				c.msgSender.SendEvent(msg)
			}
		}
	}()
}

func (c *eventBroker) close() {
	c.cancel()
	c.wg.Wait()
}

func (c *eventBroker) removeDispatcher(id string) {
	c.dispatchers.mu.Lock()
	defer c.dispatchers.mu.Unlock()
	delete(c.dispatchers.m, id)
	c.taskPool.removeTask(id)
}

// Store the progress of the dispatcher, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this dispatcher.
type dispatcherStat struct {
	info             DispatcherInfo
	spanSubscription *spanSubscription
	// The watermark of the events that have been sent to the dispatcher.
	watermark atomic.Uint64
	notify    chan *subscriptionChange
	// The index of the task queue channel in the taskPool.
	// We need to make sure the tasks of the same dispatcher are sent to the same task queue
	// so that it will be handle by the same scan worker. To ensure all events of the dispatcher
	// are sent in order.
	chanIndex int
}

func newDispatcherStat(
	startTs uint64,
	info DispatcherInfo,
	notify chan *subscriptionChange) *dispatcherStat {
	subscription := &spanSubscription{
		span: info.GetTableSpan(),
	}
	subscription.watermark.Store(uint64(startTs))

	res := &dispatcherStat{
		info:             info,
		spanSubscription: subscription,
		notify:           notify,
	}
	res.watermark.Store(startTs)
	hasher := crc32.NewIEEE()
	hasher.Write([]byte(info.GetID()))
	res.chanIndex = int(hasher.Sum32() % defaultWorkerCount)
	return res
}

// onSubscriptionWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (a *dispatcherStat) onSubscriptionWatermark(watermark uint64) {
	if watermark < a.spanSubscription.watermark.Load() {
		return
	}
	a.spanSubscription.watermark.Store(watermark)
	sub := &subscriptionChange{
		dispatcherInfo: a.info,
		eventCount:     a.spanSubscription.newEventCount.Swap(0),
	}
	select {
	case a.notify <- sub:
	default:
	}
}

// TODO: consider to use a better way to update the event count, may be we only need to
// know there are new events, and we don't need to know the exact number of the new events.
// So we can reduce the contention of the lock.
func (a *dispatcherStat) onNewEvent(raw *common.RawKVEntry) {
	if raw == nil {
		return
	}
	a.spanSubscription.newEventCount.Add(1)
}

// spanSubscription store the latest progress of the table span in the event store.
// And it also store the dispatchers that want to listen to the events of this table span.
type spanSubscription struct {
	span *common.TableSpan
	// The watermark of the events that have been stored in the event store.
	watermark atomic.Uint64
	// newEventCount is used to store the number of the new events that have been stored in the event store
	// since last scanTask is generated.
	newEventCount atomic.Uint64
}

type subscriptionChange struct {
	dispatcherInfo DispatcherInfo
	eventCount     uint64
}

type scanTask struct {
	dispatcherStat *dispatcherStat
	dataRange      *common.DataRange
	eventCount     uint64
}

type scanTaskPool struct {
	mu      sync.Mutex
	taskSet map[string]*scanTask
	// pendingTaskQueue is used to store the tasks that are waiting to be handled by the scan workers.
	// The length of the pendingTaskQueue is equal to the number of the scan workers.
	pendingTaskQueue []chan *scanTask
}

func newScanTaskPool() *scanTaskPool {
	res := &scanTaskPool{
		taskSet:          make(map[string]*scanTask),
		pendingTaskQueue: make([]chan *scanTask, defaultWorkerCount),
	}
	for i := 0; i < defaultWorkerCount; i++ {
		res.pendingTaskQueue[i] = make(chan *scanTask, defaultChannelSize)
	}
	return res
}

// pushTask pushes a task to the pool,
// and merge the task if the task is overlapped with the existing tasks.
func (p *scanTaskPool) pushTask(task *scanTask) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := task.dispatcherStat.info.GetID()
	spanTask := p.taskSet[id]
	if spanTask == nil {
		spanTask = task
		p.taskSet[id] = spanTask
	}
	// Merge the task into the existing task.
	mergedRange := task.dataRange.Merge(spanTask.dataRange)
	spanTask.dataRange = mergedRange
	spanTask.eventCount += task.eventCount

	// Send the task to the corresponding scan worker.
	ch := p.pendingTaskQueue[spanTask.dispatcherStat.chanIndex]
	select {
	case ch <- spanTask:
		// The task is sent to the scan worker, we remove it from the taskSet.
		p.taskSet[id] = nil
	default:
		// The task pool is full, we just add it back
		// to the taskSet, and it will be merged in the next round.
		p.taskSet[id] = spanTask
	}
}

func (p *scanTaskPool) popTask(chanIndex int) <-chan *scanTask {
	return p.pendingTaskQueue[chanIndex]
}

func (p *scanTaskPool) removeTask(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.taskSet, id)
}
