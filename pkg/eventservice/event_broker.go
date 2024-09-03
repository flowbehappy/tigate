package eventservice

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"hash/crc32"

	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	"go.uber.org/zap"
)

const (
	resolvedTsCacheSize = 8192
)

var metricEventServiceSendEventDuration = metrics.EventServiceSendEventDuration.WithLabelValues("txn")

// eventBroker get event from the eventStore, and send the event to the dispatchers.
// Every TiDB cluster has a eventBroker.
// All span subscriptions and dispatchers of the TiDB cluster are managed by the eventBroker.
type eventBroker struct {
	// tidbClusterID is the ID of the TiDB cluster this eventStore belongs to.
	tidbClusterID uint64
	// eventStore is the source of the events, eventBroker get the events from the eventStore.
	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	// msgSender is used to send the events to the dispatchers.
	msgSender messaging.MessageSender

	// All the dispatchers that register to the eventBroker.
	dispatchers sync.Map
	// changedCh is used to notify span subscription has new events.
	changedCh chan subscriptionChange
	// taskPool is used to store the scan tasks and merge the tasks of same dispatcher.
	// TODO: Make it support merge the tasks of the same table span, even if the tasks are from different dispatchers.
	taskPool *scanTaskPool

	// scanWorkerCount is the number of the scan workers to spawn.
	scanWorkerCount int

	// messageCh is used to receive message from the scanWorker,
	// and a goroutine is responsible for sending the message to the dispatchers.
	messageCh        chan wrapEvent
	resolvedTsCaches map[messaging.ServerId]*resolvedTsCache

	// wg is used to spawn the goroutines.
	wg *sync.WaitGroup
	// cancel is used to cancel the goroutines spawned by the eventBroker.
	cancel context.CancelFunc

	metricEventServicePullerResolvedTs     prometheus.Gauge
	metricEventServiceDispatcherResolvedTs prometheus.Gauge
	metricEventServiceResolvedTsLag        prometheus.Gauge
	metricTaskInQueueDuration              prometheus.Observer
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
		dispatchers:   sync.Map{},
		msgSender:     mc,
		// The size of the channel is 16 times of the defaultChannelSize, since the eventBroker may have many dispatchers.
		// Otherwise, the resolvedTs may be delayed.
		changedCh:                              make(chan subscriptionChange, defaultChannelSize*16),
		taskPool:                               newScanTaskPool(),
		scanWorkerCount:                        defaultScanWorkerCount,
		messageCh:                              make(chan wrapEvent, defaultChannelSize),
		resolvedTsCaches:                       make(map[messaging.ServerId]*resolvedTsCache),
		cancel:                                 cancel,
		wg:                                     wg,
		metricEventServicePullerResolvedTs:     metrics.EventServiceResolvedTsGauge,
		metricEventServiceResolvedTsLag:        metrics.EventServiceResolvedTsLagGauge.WithLabelValues("puller"),
		metricEventServiceDispatcherResolvedTs: metrics.EventServiceResolvedTsLagGauge.WithLabelValues("dispatcher"),
		metricTaskInQueueDuration:              metrics.EventServiceScanTaskInQueueDuration,
	}
	c.runGenerateScanTask(ctx)
	c.runScanWorker(ctx)
	c.runSendMessageWorker(ctx)
	c.updateMetrics(ctx)
	return c
}

func (c *eventBroker) sendWatermark(
	serverID messaging.ServerId,
	dispatcherID common.DispatcherID,
	watermark uint64,
	counter prometheus.Counter,
) {
	c.messageCh <- newWrapResolvedEvent(
		serverID,
		common.ResolvedEvent{
			DispatcherID: dispatcherID,
			ResolvedTs:   watermark})
	if counter != nil {
		counter.Inc()
	}
}

func (c *eventBroker) onAsyncNotify(change subscriptionChange) {
	c.changedCh <- change
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
				v, ok := c.dispatchers.Load(change.dispatcherInfo.GetID())
				// The dispatcher may be deleted. In such case, we just the stale notification.
				if !ok {
					continue
				}
				dispatcher := v.(*dispatcherStat)
				startTs := dispatcher.watermark.Load()
				endTs := dispatcher.spanSubscription.watermark.Load()
				dataRange := common.NewDataRange(c.tidbClusterID, dispatcher.info.GetTableSpan(), startTs, endTs)
				task := &scanTask{
					dispatcherStat: dispatcher,
					dataRange:      dataRange,
					eventCount:     change.eventCount,
					createTime:     time.Now(),
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
					c.metricTaskInQueueDuration.Observe(time.Since(task.createTime).Seconds())

					remoteID := messaging.ServerId(task.dispatcherStat.info.GetServerID())
					dispatcherID := task.dispatcherStat.info.GetID()
					//1. The dispatcher has no new events. In such case, we don't need to scan the event store.
					//We just send the watermark to the dispatcher.
					if task.eventCount == 0 {
						c.sendWatermark(remoteID, dispatcherID, task.dataRange.EndTs, task.dispatcherStat.metricEventServiceSendResolvedTsCount)
						task.dispatcherStat.watermark.Store(task.dataRange.EndTs)
						continue
					}

					//2. Get event iterator from eventStore.
					iter, err := c.eventStore.GetIterator(dispatcherID, task.dataRange)
					if err != nil {
						log.Panic("read events failed", zap.Error(err))
					}
					// 3. Get the events from the iterator and send them to the dispatcher.
					var txnEvent *common.TxnEvent
					eventCount := 0
					for {
						//Node: The first event of the txn must return isNewTxn as true.
						e, isNewTxn, err := iter.Next()
						if err != nil {
							log.Panic("read events failed", zap.Error(err))
						}

						if e == nil {
							// Send the last txnEvent to the dispatcher.
							if txnEvent != nil {
								c.messageCh <- newWrapTxnEvent(remoteID, txnEvent)
								task.dispatcherStat.watermark.Store(txnEvent.CommitTs)
								task.dispatcherStat.metricEventServiceSendKvCount.Add(float64(len(txnEvent.Rows)))
							}
							// After all the events are sent, we send the watermark to the dispatcher.
							c.sendWatermark(remoteID, dispatcherID, task.dataRange.EndTs, task.dispatcherStat.metricEventServiceSendResolvedTsCount)
							task.dispatcherStat.watermark.Store(task.dataRange.EndTs)
							break
						}

						// If the commitTs of the event is less than the watermark of the dispatcher,
						// we just skip the event.
						if e.CommitTs < task.dispatcherStat.watermark.Load() {
							log.Panic("should never Happen", zap.Uint64("commitTs", e.CommitTs), zap.Uint64("watermark", task.dispatcherStat.watermark.Load()))
						}

						if isNewTxn {
							txnEvent = &common.TxnEvent{
								DispatcherID: dispatcherID,
								StartTs:      e.StartTs,
								CommitTs:     e.CommitTs,
								Rows:         make([]*common.RowChangedEvent, 0),
							}
						}
						txnEvent.Rows = append(txnEvent.Rows, e)
						eventCount++

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
	flushResolvedTsTicker := time.NewTicker(time.Millisecond * 300)
	// Use a single goroutine to send the messages in order.
	go func() {
		defer c.wg.Done()
		defer flushResolvedTsTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-c.messageCh:
				if m.msgType == common.TypeResolvedEvent {
					// The message is a watermark, we need to cache it, and send it to the dispatcher
					// when the dispatcher is registered.
					c.handleResolvedTs(ctx, m)
					continue
				}
				tMsg := messaging.NewSingleTargetMessage(
					m.serverID,
					messaging.EventCollectorTopic,
					m.txnEvent)
				c.flushResolvedTs(ctx, m.serverID)
				c.sendMsg(ctx, tMsg)
			case <-flushResolvedTsTicker.C:
				for serverID := range c.resolvedTsCaches {
					c.flushResolvedTs(ctx, serverID)
				}
			}
		}
	}()
}

func (c *eventBroker) handleResolvedTs(ctx context.Context, e wrapEvent) {
	cache, ok := c.resolvedTsCaches[e.serverID]
	if !ok {
		cache = newResolvedTsCache(resolvedTsCacheSize)
		c.resolvedTsCaches[e.serverID] = cache
	}
	cache.add(e.resolvedEvent)
	if cache.isFull() {
		c.flushResolvedTs(ctx, e.serverID)
	}
}

func (c *eventBroker) flushResolvedTs(ctx context.Context, serverID messaging.ServerId) {
	cache, ok := c.resolvedTsCaches[serverID]
	if !ok || cache.len == 0 {
		return
	}
	msg := &common.BatchResolvedTs{}
	msg.Events = append(msg.Events, cache.getAll()...)
	tMsg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventCollectorTopic,
		msg)
	c.sendMsg(ctx, tMsg)
}

func (c *eventBroker) sendMsg(ctx context.Context, tMsg *messaging.TargetMessage) {
	start := time.Now()
	// Send the message to messageCenter. Retry if the send failed.
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Send the message to the dispatcher.
		err := c.msgSender.SendEvent(tMsg)
		if err != nil {
			log.Debug("send message failed, retry it", zap.Error(err))
			continue
		}
		metricEventServiceSendEventDuration.Observe(time.Since(start).Seconds())
		break
	}
}

func (c *eventBroker) updateMetrics(ctx context.Context) {
	c.wg.Add(1)
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		defer c.wg.Done()
		log.Info("update metrics goroutine is started")
		for {
			select {
			case <-ctx.Done():
				log.Info("update metrics goroutine is closing")
				return
			case <-ticker.C:
				pullerMinResolvedTs := uint64(0)
				dispatcherMinWaterMark := uint64(0)
				c.dispatchers.Range(func(key, value interface{}) bool {
					dispatcher := value.(*dispatcherStat)
					resolvedTs := dispatcher.spanSubscription.watermark.Load()
					if pullerMinResolvedTs == 0 || resolvedTs < pullerMinResolvedTs {
						pullerMinResolvedTs = resolvedTs
					}
					watermark := dispatcher.watermark.Load()
					if dispatcherMinWaterMark == 0 || watermark < dispatcherMinWaterMark {
						dispatcherMinWaterMark = watermark
					}
					return true
				})
				if pullerMinResolvedTs == 0 {
					continue
				}
				phyResolvedTs := oracle.ExtractPhysical(pullerMinResolvedTs)
				lag := (oracle.GetPhysical(time.Now()) - phyResolvedTs) / 1e3
				c.metricEventServicePullerResolvedTs.Set(float64(phyResolvedTs))
				c.metricEventServiceResolvedTsLag.Set(float64(lag))

				lag = (oracle.GetPhysical(time.Now()) - oracle.ExtractPhysical(dispatcherMinWaterMark)) / 1e3
				c.metricEventServiceDispatcherResolvedTs.Set(float64(lag))
			}
		}
	}()
}

func (c *eventBroker) close() {
	c.cancel()
	c.wg.Wait()
}

func (c *eventBroker) removeDispatcher(id common.DispatcherID) {
	_, ok := c.dispatchers.Load(id)
	if !ok {
		return
	}
	c.eventStore.UnregisterDispatcher(id)
	c.schemaStore.UnregisterDispatcher(id)
	c.dispatchers.Delete(id)
}

// Store the progress of the dispatcher, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this dispatcher.
type dispatcherStat struct {
	info             DispatcherInfo
	spanSubscription *spanSubscription
	// The watermark of the events that have been sent to the dispatcher.
	watermark     atomic.Uint64
	onAsyncNotify func(subscriptionChange)
	// The index of the task queue channel in the taskPool.
	// We need to make sure the tasks of the same dispatcher are sent to the same task queue
	// so that it will be handle by the same scan worker. To ensure all events of the dispatcher
	// are sent in order.
	workerIndex int

	metricSorterOutputEventCountKV        prometheus.Counter
	metricEventServiceSendKvCount         prometheus.Counter
	metricEventServiceSendResolvedTsCount prometheus.Counter
}

func newDispatcherStat(
	startTs uint64, info DispatcherInfo, onAsyncNotify func(subscriptionChange),
) *dispatcherStat {
	subscription := &spanSubscription{
		span:       info.GetTableSpan(),
		lastUpdate: atomic.Value{},
	}
	subscription.lastUpdate.Store(time.Now())
	subscription.watermark.Store(uint64(startTs))

	namespace, id := info.GetChangefeedID()
	dispStat := &dispatcherStat{
		info:             info,
		spanSubscription: subscription,
		onAsyncNotify:    onAsyncNotify,

		metricSorterOutputEventCountKV:        metrics.SorterOutputEventCount.WithLabelValues(namespace, id, "kv"),
		metricEventServiceSendKvCount:         metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "kv"),
		metricEventServiceSendResolvedTsCount: metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "resolved_ts"),
	}
	dispStat.watermark.Store(startTs)
	hasher := crc32.NewIEEE()
	hasher.Write(info.GetID().Marshal())
	dispStat.workerIndex = int(hasher.Sum32() % defaultScanWorkerCount)
	return dispStat
}

// onSubscriptionWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (a *dispatcherStat) onSubscriptionWatermark(watermark uint64) {
	if watermark < a.spanSubscription.watermark.Load() {
		return
	}
	a.spanSubscription.watermark.Store(watermark)
	a.spanSubscription.lastUpdate.Store(time.Now())
	a.onAsyncNotify(subscriptionChange{
		dispatcherInfo: a.info,
		eventCount:     a.spanSubscription.newEventCount.Swap(0),
	})
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
	createTime     time.Time
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
	taskSet map[common.DispatcherID]*scanTask
	// pendingTaskQueue is used to store the tasks that are waiting to be handled by the scan workers.
	// The length of the pendingTaskQueue is equal to the number of the scan workers.
	pendingTaskQueue []chan *scanTask
}

func newScanTaskPool() *scanTaskPool {
	res := &scanTaskPool{
		taskSet:          make(map[common.DispatcherID]*scanTask),
		pendingTaskQueue: make([]chan *scanTask, defaultScanWorkerCount),
	}
	for i := 0; i < defaultScanWorkerCount; i++ {
		res.pendingTaskQueue[i] = make(chan *scanTask, defaultChannelSize)
	}
	return res
}

// pushTask pushes a task to the pool,
// and merge the task if the task is overlapped with the existing tasks.
func (p *scanTaskPool) pushTask(task *scanTask) {
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
		delete(p.taskSet, id)
	default:
		// The task pool is full, we just add it back
		// to the taskSet, and it will be merged in the next round.
		p.taskSet[id] = spanTask
	}
}

func (p *scanTaskPool) popTask(chanIndex int) <-chan *scanTask {
	return p.pendingTaskQueue[chanIndex]
}

type wrapEvent struct {
	serverID messaging.ServerId
	// TODO: change the type of the txnEvent to common.TEvent
	txnEvent      *common.TxnEvent
	resolvedEvent common.ResolvedEvent
	ddlEvent      common.DDLEvent
	msgType       int
}

func newWrapTxnEvent(serverID messaging.ServerId, e *common.TxnEvent) wrapEvent {
	return wrapEvent{
		serverID: serverID,
		txnEvent: e,
		msgType:  common.TypeTEvent,
	}
}

func newWrapResolvedEvent(serverID messaging.ServerId, e common.ResolvedEvent) wrapEvent {
	return wrapEvent{
		serverID:      serverID,
		resolvedEvent: e,
		msgType:       common.TypeResolvedEvent,
	}
}

func newWrapDDLEvent(serverID messaging.ServerId, e common.DDLEvent) wrapEvent {
	return wrapEvent{
		serverID: serverID,
		ddlEvent: e,
		msgType:  common.TypeDDLEvent,
	}
}

// resolvedTsCache is used to cache the resolvedTs events.
// We use it instead of a primitive slice to reduce the allocation
// of the memory and reduce the GC pressure.
type resolvedTsCache struct {
	cache []common.ResolvedEvent
	// len is the number of the events in the cache.
	len int
	// limit is the max number of the events that the cache can store.
	limit int
}

func newResolvedTsCache(limit int) *resolvedTsCache {
	return &resolvedTsCache{
		cache: make([]common.ResolvedEvent, limit),
		limit: limit,
	}
}

func (c *resolvedTsCache) add(e common.ResolvedEvent) {
	c.cache[c.len] = e
	c.len++
}

func (c *resolvedTsCache) isFull() bool {
	return c.len >= c.limit
}

func (c *resolvedTsCache) getAll() []common.ResolvedEvent {
	res := c.cache[:c.len]
	c.reset()
	return res
}

func (c *resolvedTsCache) reset() {
	c.len = 0
}
