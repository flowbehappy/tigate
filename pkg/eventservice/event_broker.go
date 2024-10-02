package eventservice

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/utils/dynstream"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/pkg/mounter"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	"go.uber.org/zap"
)

const (
	resolvedTsCacheSize = 8192
	//resolvedEventRetryTime = 3
)

var metricEventServiceSendEventDuration = metrics.EventServiceSendEventDuration.WithLabelValues("txn")
var metricEventBrokerDropTaskCount = metrics.EventServiceDropScanTaskCount
var metricEventBrokerDropResolvedTsCount = metrics.EventServiceDropResolvedTsCount
var metricScanTaskQueueDuration = metrics.EventServiceScanTaskQueueDuration

// eventBroker get event from the eventStore, and send the event to the dispatchers.
// Every TiDB cluster has a eventBroker.
// All span subscriptions and dispatchers of the TiDB cluster are managed by the eventBroker.
type eventBroker struct {
	// tidbClusterID is the ID of the TiDB cluster this eventStore belongs to.
	tidbClusterID uint64
	// eventStore is the source of the events, eventBroker get the events from the eventStore.
	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	// todo: only one mounter, this may become the bottleneck affect the throughput performance
	mounter mounter.Mounter
	// msgSender is used to send the events to the dispatchers.
	msgSender messaging.MessageSender

	// All the dispatchers that register to the eventBroker.
	dispatchers sync.Map

	queue *dispatcherQueue
	// Not support split table span yet.
	spans map[common.TableID]*spanSubscription
	// dispatcherID -> dispatcherStat map, track all table trigger dispatchers.
	tableTriggerDispatchers sync.Map
	// taskPool is used to store the scan tasks and merge the tasks of same dispatcher.
	// TODO: Make it support merge the tasks of the same table span, even if the tasks are from different dispatchers.
	taskPool *scanTaskPool

	ds dynstream.DynamicStream[common.DispatcherID, scanTask, *eventBroker]

	dispatcherCount int
	// scanWorkerCount is the number of the scan workers to spawn.
	scanWorkerCount int

	// messageCh is used to receive message from the scanWorker,
	// and a goroutine is responsible for sending the message to the dispatchers.
	messageCh        chan wrapEvent
	resolvedTsCaches map[node.ID]*resolvedTsCache
	notifyCh         chan *spanSubscription

	// wg is used to spawn the goroutines.
	wg *sync.WaitGroup
	// cancel is used to cancel the goroutines spawned by the eventBroker.
	cancel context.CancelFunc

	metricDispatcherCount                  prometheus.Gauge
	metricEventServicePullerResolvedTs     prometheus.Gauge
	metricEventServiceDispatcherResolvedTs prometheus.Gauge
	metricEventServiceResolvedTsLag        prometheus.Gauge
	metricScanEventDuration                prometheus.Observer
}

func newEventBroker(
	ctx context.Context,
	id uint64,
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	mc messaging.MessageSender,
	tz *time.Location,
) *eventBroker {
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	option := dynstream.NewOption()
	option.MaxPendingLength = 1
	option.DropPolicy = dynstream.DropEarly

	ds := dynstream.NewDynamicStream[common.DispatcherID, scanTask, *eventBroker](&dispatcherEventsHandler{}, option)
	ds.Start()

	c := &eventBroker{
		tidbClusterID:                          id,
		eventStore:                             eventStore,
		mounter:                                mounter.NewMounter(tz),
		schemaStore:                            schemaStore,
		notifyCh:                               make(chan *spanSubscription, defaultChannelSize*16),
		dispatchers:                            sync.Map{},
		queue:                                  newDispatcherQueue(),
		tableTriggerDispatchers:                sync.Map{},
		spans:                                  make(map[common.TableID]*spanSubscription),
		msgSender:                              mc,
		taskPool:                               newScanTaskPool(),
		scanWorkerCount:                        defaultScanWorkerCount,
		ds:                                     ds,
		messageCh:                              make(chan wrapEvent, defaultChannelSize),
		resolvedTsCaches:                       make(map[node.ID]*resolvedTsCache),
		cancel:                                 cancel,
		wg:                                     wg,
		metricDispatcherCount:                  metrics.EventServiceDispatcherGuage.WithLabelValues(strconv.FormatUint(id, 10)),
		metricEventServicePullerResolvedTs:     metrics.EventServiceResolvedTsGauge,
		metricEventServiceResolvedTsLag:        metrics.EventServiceResolvedTsLagGauge.WithLabelValues("puller"),
		metricEventServiceDispatcherResolvedTs: metrics.EventServiceResolvedTsLagGauge.WithLabelValues("dispatcher"),
		metricScanEventDuration:                metrics.EventServiceScanDuration,
	}

	c.runScanWorker(ctx)
	c.tickTableTriggerDispatchers(ctx)
	c.runSendMessageWorker(ctx)
	c.updateMetrics(ctx)
	c.updateDispatcherSendTs(ctx)
	c.runGenTasks(ctx)
	return c
}

func (c *eventBroker) sendWatermark(
	server node.ID,
	dispatcherID common.DispatcherID,
	watermark uint64,
	counter prometheus.Counter,
) {
	resolvedEvent := newWrapResolvedEvent(
		server,
		common.ResolvedEvent{
			DispatcherID: dispatcherID,
			ResolvedTs:   watermark,
		})
	select {
	case c.messageCh <- resolvedEvent:
		if counter != nil {
			counter.Inc()
		}
	default:
		metricEventBrokerDropResolvedTsCount.Inc()
	}
}

func (c *eventBroker) runScanWorker(ctx context.Context) {
	for i := 0; i < c.scanWorkerCount; i++ {
		taskCh := c.taskPool.popTask(i)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-taskCh:
					c.doScan(ctx, task)
				}
			}
		}()
	}
}

func (c *eventBroker) runGenTasks(ctx context.Context) {
	c.wg.Add(1)
	ticker := time.NewTicker(time.Millisecond * 10)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// generate at most 3K tasks per 10 ms
				for i := 0; i < 3000; i++ {
					id, ok := c.queue.next()
					if !ok {
						break
					}
					value, ok := c.dispatchers.Load(id)
					if !ok {
						// the dispatcher may be removed?
						continue
					}
					state := value.(*dispatcherStat)
					dmlState := c.eventStore.GetDispatcherDMLEventState(id, state.info.GetTableSpan())
					ddlState := c.schemaStore.GetTableDDLEventState(state.info.GetTableSpan().TableID)
					resolvedTs := dmlState.ResolvedTs
					if ddlState.ResolvedTs < resolvedTs {
						resolvedTs = ddlState.ResolvedTs
					}
					startTs := state.nextTaskStartTs.Load()
					if resolvedTs <= startTs {
						continue
					}
					// target range: (startTs, resolvedTs]
					if dmlState.MaxEventCommitTs > startTs || ddlState.MaxEventCommitTs > startTs {
						c.ds.In() <- newScanTask(scanTaskTypeData, common.DataRange{
							ClusterID: c.tidbClusterID,
							Span:      state.info.GetTableSpan(),
							StartTs:   startTs,
							EndTs:     resolvedTs,
						}, state)
					} else {
						c.ds.In() <- newScanTask(scanTaskTypeResolvedTs, common.DataRange{
							ClusterID: c.tidbClusterID,
							Span:      state.info.GetTableSpan(),
							StartTs:   startTs,
							EndTs:     resolvedTs,
						}, state)
					}
					state.nextTaskStartTs.Store(resolvedTs)
				}
			}
		}
	}()
}

// TODO: maybe event driven model is better. It is coupled with the detail implementation of
// the schemaStore, we will refactor it later.
func (c *eventBroker) tickTableTriggerDispatchers(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(time.Millisecond * 50)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.tableTriggerDispatchers.Range(func(key, value interface{}) bool {
					dispatcher := value.(*dispatcherStat)
					startTs := dispatcher.watermark.Load()
					remoteID := node.ID(dispatcher.info.GetServerID())
					// TODO: maybe limit 1 is enough.
					ddlEvents, endTs, err := c.schemaStore.FetchTableTriggerDDLEvents(dispatcher.filter, startTs, 100)
					if err != nil {
						log.Panic("get table trigger events failed", zap.Error(err))
					}
					for _, e := range ddlEvents {
						c.sendDDL(ctx, remoteID, e, dispatcher)
					}
					// After all the events are sent, we send the watermark to the dispatcher.
					c.sendWatermark(remoteID, dispatcher.info.GetID(), endTs, dispatcher.metricEventServiceSendResolvedTsCount)
					dispatcher.watermark.Store(endTs)
					return true
				})
			}
		}
	}()
}

func (c *eventBroker) sendDDL(ctx context.Context, remoteID node.ID, e common.DDLEvent, d *dispatcherStat) {
	e.DispatcherID = d.info.GetID()
	ddlEvent := newWrapDDLEvent(remoteID, &e)
	select {
	case <-ctx.Done():
		return
	case c.messageCh <- ddlEvent:
		d.metricEventServiceSendDDLCount.Inc()
	}
}

func (c *eventBroker) wakeDispatcher(dispatcherID common.DispatcherID) {
	c.ds.Wake() <- dispatcherID
}

func (c *eventBroker) sendResolvedTs(ctx context.Context, task scanTask) {
	if task.taskType != scanTaskTypeResolvedTs {
		log.Panic("should not happen")
	}
	remoteID := node.ID(task.dispatcherStat.info.GetServerID())
	dispatcherID := task.dispatcherStat.info.GetID()
	resolvedTs := task.dataRange.EndTs
	c.sendWatermark(remoteID, dispatcherID, resolvedTs, task.dispatcherStat.metricEventServiceSendResolvedTsCount)
	task.dispatcherStat.watermark.Store(resolvedTs)
}

// TODO: handle error properly.
func (c *eventBroker) doScan(ctx context.Context, task scanTask) {
	task.handle()
	start := time.Now()

	dataRange := task.dataRange
	ddlEvents, endTs, err := c.schemaStore.FetchTableDDLEvents(dataRange.Span.TableID, task.dispatcherStat.filter, dataRange.StartTs, dataRange.EndTs)
	if err != nil {
		log.Panic("get ddl events failed", zap.Error(err))
	}
	// FIXME: the endTs may not be smaller than dataRange.EndTs any more
	if endTs < dataRange.EndTs {
		dataRange.EndTs = endTs
	}

	remoteID := node.ID(task.dispatcherStat.info.GetServerID())
	dispatcherID := task.dispatcherStat.info.GetID()
	// After all the events are sent, we need to
	// drain the ddlEvents and wake up the dispatcher.
	defer func() {
		for _, e := range ddlEvents {
			c.sendDDL(ctx, remoteID, e, task.dispatcherStat)
		}
		// After all the events are sent, we send the watermark to the dispatcher.
		c.sendWatermark(remoteID,
			dispatcherID,
			dataRange.EndTs,
			task.dispatcherStat.metricEventServiceSendResolvedTsCount)
		c.wakeDispatcher(dispatcherID)
	}()

	//2. Get event iterator from eventStore.
	iter, err := c.eventStore.GetIterator(dispatcherID, dataRange)
	if err != nil {
		log.Panic("read events failed", zap.Error(err))
	}
	defer func() {
		eventCount, _ := iter.Close()
		if eventCount != 0 {
			task.dispatcherStat.metricSorterOutputEventCountKV.Add(float64(eventCount))
		}
	}()
	sendTxn := func(t *common.DMLEvent) {
		if t != nil {
			for len(ddlEvents) > 0 && t.CommitTs > ddlEvents[0].FinishedTs {
				c.sendDDL(ctx, remoteID, ddlEvents[0], task.dispatcherStat)
				ddlEvents = ddlEvents[1:]
			}
			c.messageCh <- newWrapTxnEvent(remoteID, t)
			task.dispatcherStat.watermark.Store(t.CommitTs)
			task.dispatcherStat.metricEventServiceSendKvCount.Add(float64(t.Len()))
		}
	}

	// 3. Send the events to the dispatcher.
	var txnEvent *common.DMLEvent
	for {
		//Node: The first event of the txn must return isNewTxn as true.
		e, isNewTxn, err := iter.Next()
		if err != nil {
			log.Panic("read events failed", zap.Error(err))
		}
		if e == nil {
			// Send the last txnEvent to the dispatcher.
			sendTxn(txnEvent)
			c.metricScanEventDuration.Observe(time.Since(start).Seconds())
			return
		}
		if e.CRTs < task.dispatcherStat.watermark.Load() {
			// If the commitTs of the event is less than the watermark of the dispatcher,
			// there are some bugs in the eventStore.
			log.Panic("should never Happen", zap.Uint64("commitTs", e.CRTs), zap.Uint64("watermark", task.dispatcherStat.watermark.Load()))
		}
		if isNewTxn {
			sendTxn(txnEvent)
			tableID := task.dispatcherStat.info.GetTableSpan().TableID
			tableInfo, err := c.schemaStore.GetTableInfo(tableID, e.CRTs-1)
			if err != nil {
				// FIXME handle the error
				log.Panic("get table info failed", zap.Error(err))
			}
			txnEvent = common.NewDMLEvent(dispatcherID, tableID, e.StartTs, e.CRTs, tableInfo)
		}
		txnEvent.AppendRow(e, c.mounter.DecodeToChunk)
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
					m.e)
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

func (c *eventBroker) handleResolvedTs(ctx context.Context, m wrapEvent) {
	cache, ok := c.resolvedTsCaches[m.serverID]
	if !ok {
		cache = newResolvedTsCache(resolvedTsCacheSize)
		c.resolvedTsCaches[m.serverID] = cache
	}
	cache.add(*m.e.(*common.ResolvedEvent))
	if cache.isFull() {
		c.flushResolvedTs(ctx, m.serverID)
	}
}

func (c *eventBroker) flushResolvedTs(ctx context.Context, serverID node.ID) {
	cache, ok := c.resolvedTsCaches[serverID]
	if !ok || cache.len == 0 {
		return
	}
	msg := &common.BatchResolvedEvent{}
	msg.Events = append(msg.Events, cache.getAll()...)
	tMsg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventCollectorTopic,
		msg)
	c.sendMsg(ctx, tMsg)
}

func (c *eventBroker) sendMsg(ctx context.Context, tMsg *messaging.TargetMessage) {
	start := time.Now()
	// Send the message to messageCenter. Retry if to send failed.
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Send the message to the dispatcher.
		err := c.msgSender.SendEvent(tMsg)
		// todo: errors should be distinguished here, only retry if congested ?
		if err != nil {
			log.Debug("send message failed, retry it", zap.Error(err))
			// Wait for a while and retry to avoid the dropped message flood.
			time.Sleep(time.Millisecond * 10)
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

func (c *eventBroker) updateDispatcherSendTs(ctx context.Context) {
	c.wg.Add(1)
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		defer c.wg.Done()
		log.Info("update dispatcher send ts goroutine is started")
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.dispatchers.Range(func(key, value interface{}) bool {
					dispatcher := value.(*dispatcherStat)
					watermark := dispatcher.watermark.Load()
					c.eventStore.UpdateDispatcherSendTs(dispatcher.info.GetID(), dispatcher.info.GetTableSpan(), watermark)
					return true
				})
			}
		}
	}()
}

func (c *eventBroker) close() {
	c.cancel()
	c.wg.Wait()
	c.ds.Close()
}

func (c *eventBroker) addDispatcher(info DispatcherInfo) {
	filterConfig := info.GetFilterConfig()
	filter, err := filter.NewFilter(filterConfig, "", false)
	if err != nil {
		log.Panic("create filter failed", zap.Error(err), zap.Any("filterConfig", filterConfig))
	}

	defer c.metricDispatcherCount.Inc()
	start := time.Now()
	id := info.GetID()
	span := info.GetTableSpan()
	startTs := info.GetStartTs()
	spanSubscription, ok := c.spans[span.TableID]
	if !ok {
		spanSubscription = newSpanSubscription(span, startTs)
		c.spans[span.TableID] = spanSubscription
	}
	dispatcher := newDispatcherStat(startTs, info, spanSubscription, filter, c.dispatcherCount)
	if span.Equal(heartbeatpb.DDLSpan) {
		c.tableTriggerDispatchers.Store(id, dispatcher)
		log.Info("table trigger dispatcher register acceptor", zap.Uint64("clusterID", c.tidbClusterID),
			zap.Any("acceptorID", id), zap.Int64("tableID", span.TableID),
			zap.Uint64("startTs", startTs), zap.Duration("brokerRegisterDuration", time.Since(start)))
		return
	}

	c.dispatchers.Store(id, dispatcher)
	c.dispatcherCount++

	brokerRegisterDuration := time.Since(start)

	start = time.Now()
	c.eventStore.RegisterDispatcher(id, span, info.GetStartTs())
	c.schemaStore.RegisterTable(span.GetTableID(), info.GetStartTs())
	eventStoreRegisterDuration := time.Since(start)
	c.ds.AddPath(id, c)

	// add last
	c.queue.add(id)

	log.Info("register acceptor", zap.Uint64("clusterID", c.tidbClusterID),
		zap.Any("acceptorID", id), zap.Int64("tableID", span.TableID),
		zap.Uint64("startTs", startTs), zap.Duration("brokerRegisterDuration", brokerRegisterDuration),
		zap.Duration("eventStoreRegisterDuration", eventStoreRegisterDuration))
}

func (c *eventBroker) removeDispatcher(dispatcherInfo DispatcherInfo) {
	defer c.metricDispatcherCount.Dec()
	id := dispatcherInfo.GetID()
	stat, ok := c.dispatchers.Load(id)
	if !ok {
		c.tableTriggerDispatchers.Delete(id)
		return
	}
	// remove first
	c.queue.remove(id)

	c.eventStore.UnregisterDispatcher(id, dispatcherInfo.GetTableSpan())
	c.schemaStore.UnregisterTable(dispatcherInfo.GetTableSpan().TableID)
	c.dispatchers.Delete(id)

	spanSubscription := stat.(*dispatcherStat).spanSubscription
	dispatcherCnt := spanSubscription.removeDispatcher(id)
	if dispatcherCnt == 0 {
		delete(c.spans, spanSubscription.span.TableID)
	}
	log.Info("deregister acceptor", zap.Uint64("clusterID", c.tidbClusterID), zap.Any("acceptorID", id))
}

// Store the progress of the dispatcher, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this dispatcher.
type dispatcherStat struct {
	info             DispatcherInfo
	filter           filter.Filter
	spanSubscription *spanSubscription
	// The watermark of the events that have been sent to the dispatcher.
	watermark atomic.Uint64

	nextTaskStartTs atomic.Uint64

	metricSorterOutputEventCountKV        prometheus.Counter
	metricEventServiceSendKvCount         prometheus.Counter
	metricEventServiceSendDDLCount        prometheus.Counter
	metricEventServiceSendResolvedTsCount prometheus.Counter
}

func newDispatcherStat(
	startTs uint64, info DispatcherInfo,
	subscription *spanSubscription, filter filter.Filter,
	dispatcherIdx int,
) *dispatcherStat {
	namespace, id := info.GetChangefeedID()
	dispStat := &dispatcherStat{
		info:             info,
		filter:           filter,
		spanSubscription: subscription,

		metricSorterOutputEventCountKV:        metrics.SorterOutputEventCount.WithLabelValues(namespace, id, "kv"),
		metricEventServiceSendKvCount:         metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "kv"),
		metricEventServiceSendDDLCount:        metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "ddl"),
		metricEventServiceSendResolvedTsCount: metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "resolved_ts"),
	}
	dispStat.watermark.Store(startTs)
	dispStat.nextTaskStartTs.Store(startTs)

	subscription.addDispatcher(dispStat)
	return dispStat
}

// spanSubscription store the latest progress of the table span in the event store.
// And it also stores the dispatchers that want to listen to the events of this table span.
type spanSubscription struct {
	span *heartbeatpb.TableSpan
	// map dispatcherID -> dispatcherStat
	dispatchers struct {
		sync.RWMutex
		m map[common.DispatcherID]*dispatcherStat
	}
	// The watermark of the events that have been stored in the event store.
	watermark  atomic.Uint64
	lastUpdate atomic.Value
}

func newSpanSubscription(span *heartbeatpb.TableSpan, startTs uint64) *spanSubscription {
	s := &spanSubscription{
		span:       span,
		watermark:  atomic.Uint64{},
		lastUpdate: atomic.Value{},
	}
	s.watermark.Store(startTs)
	s.lastUpdate.Store(time.Now())
	return s
}

func (s *spanSubscription) addDispatcher(stat *dispatcherStat) {
	s.dispatchers.Lock()
	defer s.dispatchers.Unlock()
	if s.dispatchers.m == nil {
		s.dispatchers.m = make(map[common.DispatcherID]*dispatcherStat)
	}
	s.dispatchers.m[stat.info.GetID()] = stat
}

func (s *spanSubscription) removeDispatcher(id common.DispatcherID) int {
	s.dispatchers.Lock()
	defer s.dispatchers.Unlock()
	delete(s.dispatchers.m, id)
	return len(s.dispatchers.m)
}

type dispatcherQueue struct {
	sync.Mutex
	dipatchers []common.DispatcherID
	nextIndex  int
}

func newDispatcherQueue() *dispatcherQueue {
	return &dispatcherQueue{
		dipatchers: make([]common.DispatcherID, 0),
		nextIndex:  0,
	}
}

func (d *dispatcherQueue) add(id common.DispatcherID) {
	d.Lock()
	defer d.Unlock()
	d.dipatchers = append(d.dipatchers, id)
}

func (d *dispatcherQueue) next() (common.DispatcherID, bool) {
	d.Lock()
	defer d.Unlock()
	if len(d.dipatchers) == 0 {
		return common.DispatcherID{}, false
	}
	if d.nextIndex >= len(d.dipatchers) {
		d.nextIndex = 0
	}
	id := d.dipatchers[d.nextIndex]
	d.nextIndex++
	return id, true
}

func (d *dispatcherQueue) remove(id common.DispatcherID) {
	d.Lock()
	defer d.Unlock()
	for i, v := range d.dipatchers {
		if v == id {
			d.dipatchers = append(d.dipatchers[:i], d.dipatchers[i+1:]...)
			return
		}
	}
}

type scanTaskType int

const (
	scanTaskTypeResolvedTs scanTaskType = iota
	scanTaskTypeData
)

type scanTask struct {
	taskType       scanTaskType
	dataRange      common.DataRange
	dispatcherStat *dispatcherStat
	createTime     time.Time
}

func newScanTask(taskType scanTaskType, dataRange common.DataRange, dispatcherStat *dispatcherStat) scanTask {
	return scanTask{
		taskType:       taskType,
		dataRange:      dataRange,
		dispatcherStat: dispatcherStat,
		createTime:     time.Now(),
	}
}

func (t *scanTask) handle() {
	metricScanTaskQueueDuration.Observe(float64(time.Since(t.createTime).Milliseconds()))
}

type scanTaskPool struct {
	// pendingTaskQueue is used to store the tasks that are waiting to be handled by the scan workers.
	// The length of the pendingTaskQueue is equal to the number of the scan workers.
	pendingTaskQueue chan scanTask
}

func newScanTaskPool() *scanTaskPool {
	return &scanTaskPool{
		pendingTaskQueue: make(chan scanTask, defaultChannelSize),
	}
}

// pushTask pushes a task to the pool,
// and merge the task if the task is overlapped with the existing tasks.
func (p *scanTaskPool) pushTask(task scanTask) bool {
	select {
	case p.pendingTaskQueue <- task:
		return true
	default:
		metricEventBrokerDropTaskCount.Inc()
		// If the queue is full, we just drop the task
		return false
	}
}

func (p *scanTaskPool) popTask(chanIndex int) <-chan scanTask {
	return p.pendingTaskQueue
}

type wrapEvent struct {
	serverID node.ID
	e        messaging.IOTypeT
	msgType  int
}

func newWrapTxnEvent(serverID node.ID, e *common.DMLEvent) wrapEvent {
	return wrapEvent{
		serverID: serverID,
		e:        e,
		msgType:  common.TypeDMLEvent,
	}
}

func newWrapResolvedEvent(serverID node.ID, e common.ResolvedEvent) wrapEvent {
	return wrapEvent{
		serverID: serverID,
		e:        &e,
		msgType:  common.TypeResolvedEvent,
	}
}

func newWrapDDLEvent(serverID node.ID, e *common.DDLEvent) wrapEvent {
	return wrapEvent{
		serverID: serverID,
		e:        e,
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
