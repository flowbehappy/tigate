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
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	"go.uber.org/zap"
)

const (
	resolvedTsCacheSize = 8192
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
		tidbClusterID:           id,
		eventStore:              eventStore,
		mounter:                 mounter.NewMounter(tz),
		schemaStore:             schemaStore,
		notifyCh:                make(chan *spanSubscription, defaultChannelSize*16),
		dispatchers:             sync.Map{},
		tableTriggerDispatchers: sync.Map{},
		spans:                   make(map[common.TableID]*spanSubscription),
		msgSender:               mc,
		taskPool:                newScanTaskPool(),
		scanWorkerCount:         defaultScanWorkerCount,
		ds:                      ds,
		messageCh:               make(chan wrapEvent, defaultChannelSize),
		resolvedTsCaches:        make(map[node.ID]*resolvedTsCache),
		cancel:                  cancel,
		wg:                      wg,

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
			ResolvedTs:   watermark})
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
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case s := <-c.notifyCh:
				s.dispatchers.RLock()
				for _, stat := range s.dispatchers.m {
					// Only send the task to the dispatcher that is running.
					if stat.isRunning.Load() {
						c.ds.In() <- newScanTask(stat)
					}
				}
				s.dispatchers.RUnlock()
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

// checkNeedScan checks if the dispatcher needs to scan the event store.
// If the dispatcher needs to scan the event store, it returns true.
// If the dispatcher does not need to scan the event store, it
// 1. send the watermark to the dispatcher
// 2. push the task to the task pool
func (c *eventBroker) checkNeedScan(ctx context.Context, task scanTask) (bool, common.DataRange, []common.DDLEvent) {
	dataRange, needScan := task.dispatcherStat.getDataRange()
	if !needScan {
		return false, dataRange, nil
	}
	ddlEvents, endTs, err := c.schemaStore.FetchTableDDLEvents(dataRange.Span.TableID, task.dispatcherStat.filter, dataRange.StartTs, dataRange.EndTs)
	if err != nil {
		log.Panic("get ddl events failed", zap.Error(err))
	}
	if endTs < dataRange.EndTs {
		dataRange.EndTs = endTs
	}

	if dataRange.EndTs <= dataRange.StartTs {
		return false, dataRange, nil
	}

	// The dispatcher has no new events. In such case, we don't need to scan the event store.
	// We just send the watermark to the dispatcher.
	if dataRange.StartTs >= task.dispatcherStat.spanSubscription.maxEventCommitTs.Load() {
		remoteID := node.ID(task.dispatcherStat.info.GetServerID())
		dispatcherID := task.dispatcherStat.info.GetID()
		// send the watermark to the dispatcher
		for _, e := range ddlEvents {
			c.sendDDL(ctx, remoteID, e, task.dispatcherStat)
		}
		c.sendWatermark(remoteID, dispatcherID, dataRange.EndTs, task.dispatcherStat.metricEventServiceSendResolvedTsCount)
		task.dispatcherStat.watermark.Store(dataRange.EndTs)
		return false, dataRange, nil
	}

	return true, dataRange, ddlEvents
}

// TODO: handle error properly.
func (c *eventBroker) doScan(ctx context.Context, task scanTask) {
	task.handle()
	start := time.Now()
	needScan, dataRange, ddlEvents := c.checkNeedScan(ctx, task)
	if !needScan {
		return
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
	ticker := time.NewTicker(time.Second * 10)
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

func (c *eventBroker) onNotify(s *spanSubscription, watermark uint64) {
	s.onSubscriptionWatermark(watermark)
	c.notifyCh <- s
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
	c.eventStore.RegisterDispatcher(
		id,
		span,
		info.GetStartTs(),
		dispatcher.spanSubscription.onNewEvent,
		func(watermark uint64) { c.onNotify(dispatcher.spanSubscription, watermark) },
	)
	c.schemaStore.RegisterTable(span.GetTableID(), info.GetStartTs())
	eventStoreRegisterDuration := time.Since(start)
	c.ds.AddPath(id, c)

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

func (c *eventBroker) pauseDispatcher(dispatcherInfo DispatcherInfo) {
	stat, ok := c.dispatchers.Load(dispatcherInfo.GetID())
	if !ok {
		return
	}
	stat.(*dispatcherStat).isRunning.Store(false)
}

func (c *eventBroker) resumeDispatcher(dispatcherInfo DispatcherInfo) {
	stat, ok := c.dispatchers.Load(dispatcherInfo.GetID())
	if !ok {
		return
	}
	dispStat := stat.(*dispatcherStat)
	// Reset the watermark to the startTs of the dispatcherInfo.
	dispStat.watermark.Store(dispStat.info.GetStartTs())
	dispStat.isRunning.Store(true)
}

// Store the progress of the dispatcher, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this dispatcher.
type dispatcherStat struct {
	info             DispatcherInfo
	filter           filter.Filter
	spanSubscription *spanSubscription
	// The watermark of the events that have been sent to the dispatcher.
	watermark                             atomic.Uint64
	isRunning                             atomic.Bool
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
		info:                                  info,
		filter:                                filter,
		spanSubscription:                      subscription,
		metricSorterOutputEventCountKV:        metrics.SorterOutputEventCount.WithLabelValues(namespace, id, "kv"),
		metricEventServiceSendKvCount:         metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "kv"),
		metricEventServiceSendDDLCount:        metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "ddl"),
		metricEventServiceSendResolvedTsCount: metrics.EventServiceSendEventCount.WithLabelValues(namespace, id, "resolved_ts"),
	}
	dispStat.watermark.Store(startTs)
	dispStat.isRunning.Store(true)
	subscription.addDispatcher(dispStat)
	return dispStat
}

func (a *dispatcherStat) getDataRange() (common.DataRange, bool) {
	if a.watermark.Load() >= a.spanSubscription.watermark.Load() {
		return common.DataRange{}, false
	}
	r := common.DataRange{
		Span:    a.info.GetTableSpan(),
		StartTs: a.watermark.Load(),
		EndTs:   a.spanSubscription.watermark.Load(),
	}
	return r, true
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
	// The commitTs of the latest kv event that has been stored in the event store.
	maxEventCommitTs atomic.Uint64
}

func newSpanSubscription(span *heartbeatpb.TableSpan, startTs uint64) *spanSubscription {
	s := &spanSubscription{
		span:             span,
		watermark:        atomic.Uint64{},
		lastUpdate:       atomic.Value{},
		maxEventCommitTs: atomic.Uint64{},
	}
	s.watermark.Store(startTs)
	s.maxEventCommitTs.Store(startTs)
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

// onSubscriptionWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (s *spanSubscription) onSubscriptionWatermark(watermark uint64) {
	if watermark < s.watermark.Load() {
		return
	}
	s.watermark.Store(watermark)
	s.lastUpdate.Store(time.Now())
}

// onNewEvent is used to track whether there are new events in the event store, so that
// we can skip some unnecessary scan tasks.
// TODO: consider to use a better way to reduce the contention of the lock, maybe it is
// not necessary to update the maxEventCommitTs for every event.
func (s *spanSubscription) onNewEvent(raw *common.RawKVEntry) {
	if raw == nil {
		return
	}
	util.MustCompareAndMonotonicIncrease(&s.maxEventCommitTs, raw.CRTs)
}

type scanTask struct {
	dispatcherStat *dispatcherStat
	createTime     time.Time
}

func newScanTask(dispatcherStat *dispatcherStat) scanTask {
	return scanTask{
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
