package eventservice

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"hash/crc32"

	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"

	"go.uber.org/zap"
)

var metricEventServiceSendEventDuration = metrics.EventServiceSendEventDuration.WithLabelValues("txn")

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
		m  map[common.DispatcherID]*dispatcherStat
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
	messageCh chan *wrapMessage
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
			m  map[common.DispatcherID]*dispatcherStat
		}{m: make(map[common.DispatcherID]*dispatcherStat)},
		msgSender:       mc,
		changedCh:       make(chan *subscriptionChange, defaultChannelSize),
		taskPool:        newScanTaskPool(),
		scanWorkerCount: defaultWorkerCount,
		messageCh:       make(chan *wrapMessage, defaultChannelSize),
		cancel:          cancel,
		wg:              wg,
	}
	c.runGenerateScanTask(ctx)
	c.runScanWorker(ctx)
	c.runSendMessageWorker(ctx)
	c.logSlowDispatchers(ctx)
	return c
}

func (c *eventBroker) sendWatermark(
	serverID messaging.ServerId,
	topicID string,
	dispatcherID common.DispatcherID,
	watermark uint64,
	counter prometheus.Counter,
) {
	c.messageCh <- newWrapMessage(
		messaging.NewTargetMessage(
			serverID,
			topicID,
			&common.TxnEvent{
				DispatcherID: dispatcherID,
				ResolvedTs:   watermark,
			}),
		true)
	if counter != nil {
		counter.Inc()
	}
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
					needScan := task.checkAndAdjustScanTask()
					if !needScan {
						continue
					}

					remoteID := messaging.ServerId(task.dispatcherStat.info.GetServerID())
					dispatcherID := task.dispatcherStat.info.GetID()
					topic := task.dispatcherStat.info.GetTopic()
					//1.The dispatcher has no new events. In such case, we don't need to scan the event store.
					//We just send the watermark to the dispatcher.
					if task.eventCount == 0 {
						c.sendWatermark(remoteID, topic, dispatcherID, task.dataRange.EndTs, task.dispatcherStat.metricEventServiceSendResolvedTsCount)
						task.dispatcherStat.watermark.Store(task.dataRange.EndTs)
						task.dispatcherStat.lastSent.Store(time.Now())
						continue
					}

					//2. Get events iterator from eventStore.
					iter, err := c.eventStore.GetIterator(task.dataRange)
					if err != nil {
						log.Info("read events failed, push the task back to queue", zap.Error(err))
						// push the task back to the task pool.
						c.taskPool.pushTask(task)
						continue
					}
					var txnEvent *common.TxnEvent
					isFirstEvent := true
					eventCount := 0
					// 3. Get the events from the iterator and send them to the dispatcher.
					for {
						//TODO: The first event of the txn must return isNewTxn as true.
						e, isNewTxn, err := iter.Next()
						if err != nil {
							log.Panic("read events failed", zap.Error(err))
						}

						if e == nil {
							// Send the last txnEvent to the dispatcher.
							if txnEvent != nil {
								c.messageCh <- newWrapMessage(messaging.NewTargetMessage(remoteID, topic, txnEvent), false)
								task.dispatcherStat.watermark.Store(txnEvent.CommitTs)
								task.dispatcherStat.metricEventServiceSendKvCount.Add(float64(len(txnEvent.Rows)))
								task.dispatcherStat.lastSent.Store(time.Now())
							}
							// After all the events are sent, we send the watermark to the dispatcher.
							c.sendWatermark(remoteID, topic, dispatcherID, task.dataRange.EndTs, task.dispatcherStat.metricEventServiceSendResolvedTsCount)
							task.dispatcherStat.watermark.Store(task.dataRange.EndTs)
							iter.Close()
							break
						}

						// If the commitTs of the event is less than the watermark of the dispatcher,
						// we just skip the event.
						if e.CommitTs < task.dispatcherStat.watermark.Load() {
							log.Panic("should never Happen", zap.Uint64("commitTs", e.CommitTs), zap.Uint64("watermark", task.dispatcherStat.watermark.Load()))
							continue
						}

						eventCount++
						if isFirstEvent || isNewTxn {
							// Send the previous txnEvent to the dispatcher.
							if txnEvent != nil {
								c.messageCh <- newWrapMessage(messaging.NewTargetMessage(remoteID, topic, txnEvent), false)
								task.dispatcherStat.watermark.Store(txnEvent.CommitTs)
								task.dispatcherStat.metricEventServiceSendKvCount.Add(float64(len(txnEvent.Rows)))
								task.dispatcherStat.lastSent.Store(time.Now())
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
						txnEvent.Rows = append(txnEvent.Rows, e)
						if txnEvent.CommitTs != e.CommitTs {
							log.Panic("commitTs of the event is different from the commitTs of the txnEvent", zap.Uint64("eventCommitTs", e.CommitTs), zap.Uint64("txnCommitTs", txnEvent.CommitTs))
						}
					}
					task.dispatcherStat.metricSorterOutputEventCountKV.Add(float64(eventCount))
					iter.Close()
				}
			}
		}()
	}
}

func (c *eventBroker) runSendMessageWorker(ctx context.Context) {
	c.wg.Add(1)
	// Use a single goroutine to send the messages in order.
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-c.messageCh:

				// Send the message to messageCenter. Retry if the send failed.
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					start := time.Now()
					// Send the message to the dispatcher.
					err := c.msgSender.SendEvent(m.msg)
					// If the message is a watermark, we don't need to retry. Since the watermark is continuous.
					if err != nil && !m.isWatermark {
						log.Debug("send message failed", zap.Error(err))
						continue
					}

					metricEventServiceSendEventDuration.Observe(time.Since(start).Seconds())
					break
				}
			}
		}
	}()
}

func (c *eventBroker) logSlowDispatchers(ctx context.Context) {
	c.wg.Add(1)
	ticker := time.NewTicker(time.Second * 10)
	logDispatcherCount := 0
	log.Info("start log slow dispatchers")
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.dispatchers.mu.RLock()
				for _, dispatcher := range c.dispatchers.m {
					lastUpdate := dispatcher.spanSubscription.lastUpdate.Load().(time.Time)
					lastSent := dispatcher.lastSent.Load().(time.Time)
					if time.Since(lastSent) > time.Second*30 {
						// limit the log count to avoid log flooding.
						if logDispatcherCount > 10 {
							break
						}
						logDispatcherCount++
						_, id := dispatcher.info.GetChangefeedID()
						log.Warn("dispatcher is slow",
							zap.String("changefeed", id),
							zap.Any("dispatcher", dispatcher.info.GetID()),
							zap.Time("last-update", lastUpdate),
							zap.Time("last-sent", lastSent),
							zap.Uint64("subscription-watermark", dispatcher.spanSubscription.watermark.Load()),
							zap.Uint64("dispatcher-watermark", dispatcher.watermark.Load()),
							zap.Uint64("subscription-eventCount", dispatcher.spanSubscription.newEventCount.Load()))
					}
				}
				c.dispatchers.mu.RUnlock()
			}
		}
	}()
}

func (c *eventBroker) close() {
	c.cancel()
	c.wg.Wait()
}

func (c *eventBroker) removeDispatcher(id common.DispatcherID) {
	c.dispatchers.mu.Lock()
	defer c.dispatchers.mu.Unlock()

	_, ok := c.dispatchers.m[id]
	if !ok {
		return
	}
	c.eventStore.UnregisterDispatcher(id)
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
	workerIndex int

	metricSorterOutputEventCountKV        prometheus.Counter
	metricEventServiceSendKvCount         prometheus.Counter
	metricEventServiceSendResolvedTsCount prometheus.Counter

	lastSent atomic.Value
}

func newDispatcherStat(
	startTs uint64,
	info DispatcherInfo,
	notify chan *subscriptionChange) *dispatcherStat {
	subscription := &spanSubscription{
		span:       info.GetTableSpan(),
		lastUpdate: atomic.Value{},
	}

	subscription.lastUpdate.Store(time.Now())
	subscription.watermark.Store(uint64(startTs))

	namespace, id := info.GetChangefeedID()
	res := &dispatcherStat{
		info:             info,
		spanSubscription: subscription,
		notify:           notify,

		metricSorterOutputEventCountKV:        metrics.SorterOutputEventCount.WithLabelValues(namespace, id, "kv"),
		metricEventServiceSendKvCount:         metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "kv"),
		metricEventServiceSendResolvedTsCount: metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "resolved_ts"),
		lastSent:                              atomic.Value{},
	}
	res.lastSent.Store(time.Now())
	res.watermark.Store(startTs)
	hasher := crc32.NewIEEE()
	hasher.Write(info.GetID().Marshal())
	res.workerIndex = int(hasher.Sum32() % defaultWorkerCount)
	return res
}

// onSubscriptionWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (a *dispatcherStat) onSubscriptionWatermark(watermark uint64) {
	if watermark < a.spanSubscription.watermark.Load() {
		return
	}
	a.spanSubscription.watermark.Store(watermark)
	a.spanSubscription.lastUpdate.Store(time.Now())

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
	lastUpdate    atomic.Value
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

func (t *scanTask) checkAndAdjustScanTask() bool {
	if t.dispatcherStat.watermark.Load() >= t.dataRange.EndTs {
		return false
	}
	if t.dispatcherStat.watermark.Load() > t.dataRange.StartTs {
		t.dataRange.StartTs = t.dispatcherStat.watermark.Load()
	}
	return true
}

type scanTaskPool struct {
	mu      sync.Mutex
	taskSet map[common.DispatcherID]*scanTask
	// pendingTaskQueue is used to store the tasks that are waiting to be handled by the scan workers.
	// The length of the pendingTaskQueue is equal to the number of the scan workers.
	pendingTaskQueue []chan *scanTask
}

func newScanTaskPool() *scanTaskPool {
	res := &scanTaskPool{
		taskSet:          make(map[common.DispatcherID]*scanTask),
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

	// There is already a task for the dispatcher, we need to merge the task to the existing task.
	if spanTask != nil {
		mergedRange := task.dataRange.Merge(spanTask.dataRange)
		if mergedRange == nil {
			log.Panic("merge data range failed", zap.Any("task", task), zap.Any("spanTask", spanTask))
		}
		spanTask.dataRange = mergedRange
		spanTask.eventCount += task.eventCount
	} else {
		spanTask = task
	}

	select {
	// Send the task to the corresponding scan worker.
	case p.pendingTaskQueue[spanTask.dispatcherStat.workerIndex] <- spanTask:
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

func (p *scanTaskPool) removeTask(id common.DispatcherID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.taskSet, id)
}

type wrapMessage struct {
	msg         *messaging.TargetMessage
	isWatermark bool
}

func newWrapMessage(msg *messaging.TargetMessage, isWatermark bool) *wrapMessage {
	return &wrapMessage{
		msg:         msg,
		isWatermark: isWatermark,
	}
}
