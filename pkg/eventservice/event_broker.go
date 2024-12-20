package eventservice

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"

	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

const (
	resolvedTsCacheSize = 512
	basicChannelSize    = 2048
)

var (
	metricEventServiceSendEventDuration   = metrics.EventServiceSendEventDuration.WithLabelValues("txn")
	metricEventBrokerScanTaskCount        = metrics.EventServiceScanTaskCount
	metricEventBrokerPendingScanTaskCount = metrics.EventServicePendingScanTaskCount
	metricEventStoreOutputKv              = metrics.EventStoreOutputEventCount.WithLabelValues("kv")
	metricEventStoreOutputResolved        = metrics.EventStoreOutputEventCount.WithLabelValues("resolved")

	metricEventServiceSendKvCount         = metrics.EventServiceSendEventCount.WithLabelValues("kv")
	metricEventServiceSendResolvedTsCount = metrics.EventServiceSendEventCount.WithLabelValues("resolved_ts")
	metricEventServiceSendDDLCount        = metrics.EventServiceSendEventCount.WithLabelValues("ddl")
	metricEventServiceSendCommandCount    = metrics.EventServiceSendEventCount.WithLabelValues("command")
)

// eventBroker get event from the eventStore, and send the event to the dispatchers.
// Every TiDB cluster has a eventBroker.
// All span subscriptions and dispatchers of the TiDB cluster are managed by the eventBroker.
type eventBroker struct {
	// tidbClusterID is the ID of the TiDB cluster this eventStore belongs to.
	tidbClusterID uint64
	// eventStore is the source of the events, eventBroker get the events from the eventStore.
	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	mounter     pevent.Mounter
	// msgSender is used to send the events to the dispatchers.
	msgSender messaging.MessageSender

	// All the dispatchers that register to the eventBroker.
	dispatchers sync.Map
	// dispatcherID -> dispatcherStat map, track all table trigger dispatchers.
	tableTriggerDispatchers sync.Map
	// taskChan is used to send the scan tasks to the scan workers.
	taskChan chan scanTask

	// sendMessageWorkerCount is the number of the send message workers to spawn.
	sendMessageWorkerCount int
	// scanWorkerCount is the number of the scan workers to spawn.
	scanWorkerCount int

	// messageCh is used to receive message from the scanWorker,
	// and a goroutine is responsible for sending the message to the dispatchers.
	messageCh []chan *wrapEvent

	// wg is used to spawn the goroutines.
	wg *sync.WaitGroup
	// cancel is used to cancel the goroutines spawned by the eventBroker.
	cancel context.CancelFunc

	metricDispatcherCount                prometheus.Gauge
	metricEventServiceReceivedResolvedTs prometheus.Gauge
	metricEventServiceSentResolvedTs     prometheus.Gauge
	metricEventServiceResolvedTsLag      prometheus.Gauge
	metricScanEventDuration              prometheus.Observer
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
	// These numbers are define by real test result.
	// We noted that:
	// 1. When the number of send message workers is too small, the lag of the resolvedTs keep in a high level.
	// 2. When the number of send message workers is too large, the lag of the resolvedTs has spikes.
	// And when the number of send message workers is x, the lag of the resolvedTs is stable.
	sendMessageWorkerCount := config.DefaultBasicEventHandlerConcurrency
	scanWorkerCount := config.DefaultBasicEventHandlerConcurrency * 4

	conf := config.GetGlobalServerConfig().Debug.EventService

	c := &eventBroker{
		tidbClusterID:           id,
		eventStore:              eventStore,
		mounter:                 pevent.NewMounter(tz),
		schemaStore:             schemaStore,
		dispatchers:             sync.Map{},
		tableTriggerDispatchers: sync.Map{},
		msgSender:               mc,
		taskChan:                make(chan scanTask, conf.ScanTaskQueueSize),
		sendMessageWorkerCount:  sendMessageWorkerCount,
		messageCh:               make([]chan *wrapEvent, sendMessageWorkerCount),
		scanWorkerCount:         scanWorkerCount,
		cancel:                  cancel,
		wg:                      wg,

		metricDispatcherCount:                metrics.EventServiceDispatcherGauge.WithLabelValues(strconv.FormatUint(id, 10)),
		metricEventServiceReceivedResolvedTs: metrics.EventServiceResolvedTsGauge,
		metricEventServiceResolvedTsLag:      metrics.EventServiceResolvedTsLagGauge.WithLabelValues("received"),
		metricEventServiceSentResolvedTs:     metrics.EventServiceResolvedTsLagGauge.WithLabelValues("sent"),
		metricScanEventDuration:              metrics.EventServiceScanDuration,
	}

	for i := 0; i < c.sendMessageWorkerCount; i++ {
		c.messageCh[i] = make(chan *wrapEvent, basicChannelSize*4)
	}

	c.runScanWorker(ctx)
	c.tickTableTriggerDispatchers(ctx)
	c.logUnresetDispatchers(ctx)
	c.reportDispatcherStatToStore(ctx)
	c.updateMetrics(ctx)

	for i := 0; i < c.sendMessageWorkerCount; i++ {
		c.runSendMessageWorker(ctx, i)
	}
	log.Info("new event broker created", zap.Uint64("id", id))
	return c
}

func (c *eventBroker) sendWatermark(
	server node.ID,
	d *dispatcherStat,
	watermark uint64,
) {
	c.emitSyncPointEventIfNeeded(watermark, d, server)
	re := pevent.NewResolvedEvent(watermark, d.id)
	resolvedEvent := newWrapResolvedEvent(
		server,
		re,
		d.getEventSenderState())
	c.getMessageCh(d.workerIndex) <- resolvedEvent
	metricEventServiceSendResolvedTsCount.Inc()
}

func (c *eventBroker) sendReadyEvent(
	server node.ID,
	d *dispatcherStat,
) {
	event := pevent.NewReadyEvent(d.info.GetID())
	wrapEvent := newWrapReadyEvent(server, event)
	c.getMessageCh(d.workerIndex) <- wrapEvent
	metricEventServiceSendCommandCount.Inc()
}

func (c *eventBroker) sendNotReusableEvent(
	server node.ID,
	d *dispatcherStat,
) {
	event := pevent.NewNotReusableEvent(d.info.GetID())
	wrapEvent := newWrapNotReusableEvent(server, event)

	// must success unless we can do retry later
	c.getMessageCh(d.workerIndex) <- wrapEvent
	metricEventServiceSendCommandCount.Inc()
}

func (c *eventBroker) getMessageCh(workerIndex int) chan *wrapEvent {
	return c.messageCh[workerIndex]
}

func (c *eventBroker) runScanWorker(ctx context.Context) {
	c.wg.Add(c.scanWorkerCount)
	for i := 0; i < c.scanWorkerCount; i++ {
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-c.taskChan:
					c.doScan(ctx, task)
				}
			}
		}()
	}
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
					dispatcherStat := value.(*dispatcherStat)
					if dispatcherStat.resetTs.Load() == 0 {
						c.sendReadyEvent(node.ID(dispatcherStat.info.GetServerID()), dispatcherStat)
						return true
					}
					if !dispatcherStat.isInitialized.Load() {
						dispatcherStat.seq.Store(0)
						e := pevent.NewHandshakeEvent(
							dispatcherStat.id,
							dispatcherStat.startTs,
							dispatcherStat.seq.Add(1),
							nil)
						wrapE := &wrapEvent{
							serverID: node.ID(dispatcherStat.info.GetServerID()),
							e:        e,
							msgType:  pevent.TypeHandshakeEvent,
							postSendFunc: func() {
								dispatcherStat.isInitialized.Store(true)
							},
						}
						c.getMessageCh(dispatcherStat.workerIndex) <- wrapE
						return true
					}

					startTs := dispatcherStat.sentResolvedTs.Load()
					remoteID := node.ID(dispatcherStat.info.GetServerID())
					// TODO: maybe limit 1 is enough.
					ddlEvents, endTs, err := c.schemaStore.FetchTableTriggerDDLEvents(dispatcherStat.filter, startTs, 100)
					if err != nil {
						log.Panic("get table trigger events failed", zap.Error(err))
					}
					for _, e := range ddlEvents {
						c.sendDDL(ctx, remoteID, e, dispatcherStat)
					}
					if endTs > startTs {
						// After all the events are sent, we send the watermark to the dispatcher.
						c.sendWatermark(remoteID, dispatcherStat, endTs)
						dispatcherStat.sentResolvedTs.Store(endTs)
					}
					return true
				})
			}
		}
	}()
}

func (c *eventBroker) logUnresetDispatchers(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.dispatchers.Range(func(key, value interface{}) bool {
					dispatcher := value.(*dispatcherStat)
					if dispatcher.resetTs.Load() == 0 {
						log.Info("dispatcher not reset", zap.Any("dispatcher", dispatcher.id))
					}
					return true
				})
			}
		}
	}()
}

func (c *eventBroker) sendDDL(ctx context.Context, remoteID node.ID, e pevent.DDLEvent, d *dispatcherStat) {
	c.emitSyncPointEventIfNeeded(e.FinishedTs, d, remoteID)
	e.DispatcherID = d.id
	e.Seq = d.seq.Add(1)
	log.Info("send ddl event to dispatcher", zap.Stringer("dispatcher", d.id), zap.String("query", e.Query), zap.Int64("table", e.TableID), zap.Uint64("commitTs", e.FinishedTs), zap.Uint64("seq", e.Seq))
	ddlEvent := newWrapDDLEvent(remoteID, &e, d.getEventSenderState())
	select {
	case <-ctx.Done():
		return
	case c.getMessageCh(d.workerIndex) <- ddlEvent:
		metricEventServiceSendDDLCount.Inc()
	}
}

// checkNeedScan checks if the dispatcher needs to scan the event store.
// If the dispatcher needs to scan the event store, it returns true.
// If the dispatcher does not need to scan the event store, it send the watermark to the dispatcher
func (c *eventBroker) checkNeedScan(task scanTask, mustCheck bool) (bool, common.DataRange) {
	if !mustCheck && task.taskScanning.Load() {
		return false, common.DataRange{}
	}
	if task.resetTs.Load() == 0 {
		remoteID := node.ID(task.info.GetServerID())
		c.sendReadyEvent(remoteID, task)
		//log.Info("Send ready event to dispatcher", zap.Stringer("dispatcher", task.id))
		return false, common.DataRange{}
	}

	c.checkAndInitDispatcher(task)
	// 1. Get the data range of the dispatcher.
	dataRange, needScan := task.getDataRange()
	if !needScan {
		return false, dataRange
	}

	// 2. Constrain the data range by the ddl state of the table.
	ddlState := c.schemaStore.GetTableDDLEventState(task.info.GetTableSpan().TableID)
	if ddlState.ResolvedTs < dataRange.EndTs {
		dataRange.EndTs = ddlState.ResolvedTs
	}

	if dataRange.EndTs <= dataRange.StartTs {
		return false, dataRange
	}

	// target ts range: (dataRange.StartTs, dataRange.EndTs]
	if dataRange.StartTs >= task.latestCommitTs.Load() &&
		dataRange.StartTs >= ddlState.MaxEventCommitTs {
		// The dispatcher has no new events. In such case, we don't need to scan the event store.
		// We just send the watermark to the dispatcher.
		remoteID := node.ID(task.info.GetServerID())
		c.sendWatermark(remoteID, task, dataRange.EndTs)
		task.sentResolvedTs.Store(dataRange.EndTs)
		return false, dataRange
	}

	// Only scan when the dispatcher is running.
	if !task.isRunning.Load() {
		// If the dispatcher is not running, we also need to send the watermark to the dispatcher.
		// And the resolvedTs should be the last sent watermark.
		resolvedTs := task.sentResolvedTs.Load()
		remoteID := node.ID(task.info.GetServerID())
		c.sendWatermark(remoteID, task, resolvedTs)
		return false, dataRange
	}

	return true, dataRange
}

func (c *eventBroker) checkAndInitDispatcher(task scanTask) {
	if task.isInitialized.Load() {
		return
	}
	// Always reset the seq of the dispatcher to 0 before sending a handshake event.
	task.seq.Store(0)
	wrapE := &wrapEvent{
		serverID: node.ID(task.info.GetServerID()),
		e: pevent.NewHandshakeEvent(
			task.id,
			task.sentResolvedTs.Load(),
			task.seq.Add(1),
			task.startTableInfo.Load()),
		msgType: pevent.TypeHandshakeEvent,
		postSendFunc: func() {
			task.isInitialized.Store(true)
		},
	}
	//log.Info("Send handshake event to dispatcher", zap.Uint64("seq", wrapE.e.(*pevent.HandshakeEvent).Seq), zap.Stringer("dispatcher", task.id))
	c.getMessageCh(task.workerIndex) <- wrapE
	metricEventServiceSendCommandCount.Inc()
}

// emitSyncPointEventIfNeeded emits a sync point event if the current ts is greater than the next sync point, and updates the next sync point.
// We need call this function every time we send a event(whether dml/ddl/resolvedTs),
// thus to ensure the sync point event is in correct order for each dispatcher.
func (c *eventBroker) emitSyncPointEventIfNeeded(ts uint64, d *dispatcherStat, remoteID node.ID) {
	if d.enableSyncPoint && ts > d.nextSyncPoint {
		// Send the sync point event.
		syncPointEvent := newWrapSyncPointEvent(
			remoteID,
			&pevent.SyncPointEvent{
				DispatcherID: d.id,
				CommitTs:     ts},
			d.getEventSenderState())
		c.getMessageCh(d.workerIndex) <- syncPointEvent
		d.nextSyncPoint = oracle.GoTimeToTS(oracle.GetTimeFromTS(d.nextSyncPoint).Add(d.syncPointInterval))
	}
}

// TODO: handle error properly.
func (c *eventBroker) doScan(ctx context.Context, task scanTask) {
	task.handle()
	start := time.Now()
	remoteID := node.ID(task.info.GetServerID())
	dispatcherID := task.id

	defer func() {
		task.taskScanning.Store(false)
	}()

	// If the target is not ready to send, we don't need to scan the event store.
	// To avoid the useless scan task.
	if !c.msgSender.IsReadyToSend(remoteID) {
		log.Info("The remote target is not ready, skip scan", zap.Stringer("dispatcher", task.id), zap.Stringer("remote", remoteID))
		return
	}

	needScan, dataRange := c.checkNeedScan(task, true)
	if !needScan {
		return
	}

	// TODO: distinguish only dml or only ddl scenario
	ddlEvents, err := c.schemaStore.FetchTableDDLEvents(dataRange.Span.TableID, task.filter, dataRange.StartTs, dataRange.EndTs)
	if err != nil {
		log.Panic("get ddl events failed", zap.Error(err))
	}

	// After all the events are sent, we need to
	// drain the ddlEvents and wake up the dispatcher.
	defer func() {
		for _, e := range ddlEvents {
			c.sendDDL(ctx, remoteID, e, task)
		}
		task.sentResolvedTs.Store(dataRange.EndTs)
		// After all the events are sent, we send the watermark to the dispatcher.
		c.sendWatermark(remoteID,
			task,
			dataRange.EndTs)
	}()

	//2. Get event iterator from eventStore.
	iter, err := c.eventStore.GetIterator(dispatcherID, dataRange)
	if err != nil {
		log.Panic("read events failed", zap.Error(err))
	}
	// TODO: use error to indicate the dispatcher is removed
	if iter == nil {
		return
	}

	defer func() {
		eventCount, _ := iter.Close()
		if eventCount != 0 {
			metricEventStoreOutputKv.Add(float64(eventCount))
		}
		metricEventBrokerScanTaskCount.Inc()
	}()

	sendDML := func(dml *pevent.DMLEvent) {
		if dml == nil {
			return
		}

		for len(ddlEvents) > 0 && dml.CommitTs > ddlEvents[0].FinishedTs {
			c.sendDDL(ctx, remoteID, ddlEvents[0], task)
			ddlEvents = ddlEvents[1:]
		}
		dml.Seq = task.seq.Add(1)
		c.emitSyncPointEventIfNeeded(dml.CommitTs, task, remoteID)
		c.getMessageCh(task.workerIndex) <- newWrapDMLEvent(remoteID, dml, task.getEventSenderState())
		metricEventServiceSendKvCount.Add(float64(dml.Len()))
	}

	// 3. Send the events to the dispatcher.
	var dml *pevent.DMLEvent
	for {
		//Node: The first event of the txn must return isNewTxn as true.
		e, isNewTxn, err := iter.Next()
		if err != nil {
			log.Panic("read events failed", zap.Error(err))
		}
		if e == nil {
			// Send the last dml to the dispatcher.
			sendDML(dml)
			c.metricScanEventDuration.Observe(time.Since(start).Seconds())
			return
		}
		if e.CRTs < dataRange.StartTs {
			// If the commitTs of the event is less than the startTs of the data range,
			// there are some bugs in the eventStore.
			log.Panic("should never Happen", zap.Uint64("commitTs", e.CRTs), zap.Uint64("dataRangeStartTs", dataRange.StartTs))
		}

		if isNewTxn {
			sendDML(dml)
			tableID := task.info.GetTableSpan().TableID
			tableInfo, err := c.schemaStore.GetTableInfo(tableID, e.CRTs-1)
			if err != nil {
				if task.isRemoved.Load() {
					log.Warn("get table info failed, since the dispatcher is removed", zap.Error(err))
					return
				}
				log.Panic("get table info failed, unknown reason", zap.Error(err))
			}
			dml = pevent.NewDMLEvent(dispatcherID, tableID, e.StartTs, e.CRTs, tableInfo)
		}
		dml.AppendRow(e, c.mounter.DecodeToChunk)
	}
}

func (c *eventBroker) runSendMessageWorker(ctx context.Context, workerIndex int) {
	c.wg.Add(1)
	flushResolvedTsTicker := time.NewTicker(time.Millisecond * 25)
	resolvedTsCacheMap := make(map[node.ID]*resolvedTsCache)
	messageCh := c.messageCh[workerIndex]
	tickCh := flushResolvedTsTicker.C

	maxBatchSize := 128
	batchM := make([]*wrapEvent, 0, maxBatchSize)

	go func() {
		defer c.wg.Done()
		defer flushResolvedTsTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-messageCh:
				batchM = append(batchM, m)

			LOOP:
				for {
					select {
					case moreM := <-messageCh:
						batchM = append(batchM, moreM)
						if len(batchM) > maxBatchSize {
							break LOOP
						}
					default:
						break LOOP
					}
				}

				for _, m := range batchM {
					if m.msgType == pevent.TypeResolvedEvent {
						c.handleResolvedTs(ctx, resolvedTsCacheMap, m, workerIndex)
						continue
					}
					// Check if the dispatcher is initialized, if so, ignore the handshake event.
					if m.msgType == pevent.TypeHandshakeEvent {
						// If the message is a handshake event, we need to reset the dispatcher.
						d, ok := c.getDispatcher(m.getDispatcherID())
						if !ok {
							log.Warn("Get dispatcher failed", zap.Any("dispatcherID", m.getDispatcherID()))
							continue
						} else if d.isInitialized.Load() {
							log.Info("Ignore handshake event since the dispatcher is initialized", zap.Any("dispatcherID", m.getDispatcherID()))
							continue
						}
					}
					tMsg := messaging.NewSingleTargetMessage(
						m.serverID,
						messaging.EventCollectorTopic,
						m.e,
						uint64(workerIndex),
					)
					// Note: we need to flush the resolvedTs cache before sending the message
					// to keep the order of the resolvedTs and the message.
					c.flushResolvedTs(ctx, resolvedTsCacheMap[m.serverID], m.serverID, workerIndex)
					c.sendMsg(ctx, tMsg, m.postSendFunc)
					m.reset()
				}
				batchM = batchM[:0]

			case <-tickCh:
				for serverID, cache := range resolvedTsCacheMap {
					c.flushResolvedTs(ctx, cache, serverID, workerIndex)
				}
			}
		}
	}()
}

func (c *eventBroker) handleResolvedTs(ctx context.Context, cacheMap map[node.ID]*resolvedTsCache, m *wrapEvent, workerIndex int) {
	defer m.reset()
	cache, ok := cacheMap[m.serverID]
	if !ok {
		cache = newResolvedTsCache(resolvedTsCacheSize)
		cacheMap[m.serverID] = cache
	}
	cache.add(m.resolvedTsEvent)
	if cache.isFull() {
		c.flushResolvedTs(ctx, cache, m.serverID, workerIndex)
	}
}

func (c *eventBroker) flushResolvedTs(ctx context.Context, cache *resolvedTsCache, serverID node.ID, workerIndex int) {
	if cache == nil || cache.len == 0 {
		return
	}
	msg := &pevent.BatchResolvedEvent{}
	msg.Events = append(msg.Events, cache.getAll()...)
	tMsg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventCollectorTopic,
		msg,
		uint64(workerIndex),
	)
	c.sendMsg(ctx, tMsg, nil)
}

func (c *eventBroker) sendMsg(ctx context.Context, tMsg *messaging.TargetMessage, postSendMsg func()) {
	start := time.Now()
	congestedRetryInterval := time.Millisecond * 10
	// Send the message to messageCenter. Retry if to send failed.
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Send the message to the dispatcher.
		err := c.msgSender.SendEvent(tMsg)
		if err != nil {
			appErr, ok := err.(*apperror.AppError)
			if ok && appErr.Type == apperror.ErrorTypeMessageCongested {
				log.Debug("send message failed since the message is congested, retry it laster", zap.Error(err))
				// Wait for a while and retry to avoid the dropped message flood.
				time.Sleep(congestedRetryInterval)
				continue
			} else {
				// Drop the message, and return.
				// If the dispatcher finds the events are not continuous, it will send a reset message.
				// And the broker will send the missed events to the dispatcher again.
				return
			}
		}
		if postSendMsg != nil {
			postSendMsg()
		}
		metricEventServiceSendEventDuration.Observe(time.Since(start).Seconds())
		return
	}
}

func (c *eventBroker) updateMetrics(ctx context.Context) {
	c.wg.Add(1)
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		defer c.wg.Done()
		log.Info("update metrics goroutine is started")
		for {
			select {
			case <-ctx.Done():
				log.Info("update metrics goroutine is closing")
				return
			case <-ticker.C:
				receivedMinResolvedTs := uint64(0)
				sentMinWaterMark := uint64(0)
				c.dispatchers.Range(func(key, value interface{}) bool {
					dispatcher := value.(*dispatcherStat)
					resolvedTs := dispatcher.eventStoreResolvedTs.Load()
					if receivedMinResolvedTs == 0 || resolvedTs < receivedMinResolvedTs {
						receivedMinResolvedTs = resolvedTs
					}
					watermark := dispatcher.sentResolvedTs.Load()
					if sentMinWaterMark == 0 || watermark < sentMinWaterMark {
						sentMinWaterMark = watermark
					}
					return true
				})
				if receivedMinResolvedTs == 0 {
					continue
				}
				phyResolvedTs := oracle.ExtractPhysical(receivedMinResolvedTs)
				lag := float64(oracle.GetPhysical(time.Now())-phyResolvedTs) / 1e3
				c.metricEventServiceReceivedResolvedTs.Set(float64(phyResolvedTs))
				c.metricEventServiceResolvedTsLag.Set(lag)
				lag = float64(oracle.GetPhysical(time.Now())-oracle.ExtractPhysical(sentMinWaterMark)) / 1e3
				c.metricEventServiceSentResolvedTs.Set(lag)

				metricEventBrokerPendingScanTaskCount.Set(float64(len(c.taskChan)))

			}
		}
	}()
}

// updateDispatcherSendTs updates the sendTs of the dispatcher periodically.
// The eventStore need to know this to GC the stale data.
func (c *eventBroker) reportDispatcherStatToStore(ctx context.Context) {
	c.wg.Add(1)
	ticker := time.NewTicker(time.Second * 120)
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
					// FIXME: use checkpointTs instead after checkpointTs is correctly updated
					checkpointTs := dispatcher.sentResolvedTs.Load()
					// TODO: when use checkpointTs, this check can be removed
					if checkpointTs > 0 {
						c.eventStore.UpdateDispatcherCheckpointTs(dispatcher.id, checkpointTs)
					}
					return true
				})
			}
		}
	}()
}

func (c *eventBroker) close() {
	c.cancel()
	c.wg.Wait()
}

func (c *eventBroker) onNotify(d *dispatcherStat, resolvedTs uint64, latestCommitTs uint64) {
	if d.onResolvedTs(resolvedTs) {
		metricEventStoreOutputResolved.Inc()
		d.onLatestCommitTs(latestCommitTs)
		needScan, _ := c.checkNeedScan(d, false)
		if needScan {
			d.taskScanning.Store(true)
			c.taskChan <- d
		}
	}
}

func (c *eventBroker) getDispatcher(id common.DispatcherID) (*dispatcherStat, bool) {
	stat, ok := c.dispatchers.Load(id)
	if !ok {
		stat, ok = c.tableTriggerDispatchers.Load(id)
	}
	if !ok {
		return nil, false
	}
	return stat.(*dispatcherStat), true
}

func (c *eventBroker) addDispatcher(info DispatcherInfo) {
	defer c.metricDispatcherCount.Inc()
	filter := info.GetFilter()

	start := time.Now()
	id := info.GetID()
	span := info.GetTableSpan()
	startTs := info.GetStartTs()
	workerIndex := int((common.GID)(id).Hash(uint64(c.sendMessageWorkerCount)))
	dispatcher := newDispatcherStat(startTs, info, filter, workerIndex)
	if span.Equal(heartbeatpb.DDLSpan) {
		c.tableTriggerDispatchers.Store(id, dispatcher)
		log.Info("table trigger dispatcher register dispatcher", zap.Uint64("clusterID", c.tidbClusterID),
			zap.Any("dispatcherID", id), zap.Int64("tableID", span.TableID),
			zap.Uint64("startTs", startTs), zap.Duration("brokerRegisterDuration", time.Since(start)))
		return
	}

	brokerRegisterDuration := time.Since(start)

	start = time.Now()
	success, err := c.eventStore.RegisterDispatcher(
		id,
		span,
		info.GetStartTs(),
		func(resolvedTs uint64, latestCommitTs uint64) { c.onNotify(dispatcher, resolvedTs, latestCommitTs) },
		info.IsOnlyReuse(),
	)
	if err != nil {
		log.Panic("register dispatcher to eventStore failed", zap.Error(err), zap.Any("dispatcherInfo", info))
	}
	if !success {
		if !info.IsOnlyReuse() {
			log.Panic("register dispatcher to eventStore failed",
				zap.Error(err),
				zap.Any("dispatcherInfo", info))
		}
		c.sendNotReusableEvent(node.ID(info.GetServerID()), dispatcher)
		return
	}

	err = c.schemaStore.RegisterTable(span.GetTableID(), info.GetStartTs())
	if err != nil {
		log.Panic("register table to schemaStore failed", zap.Error(err), zap.Int64("tableID", span.TableID), zap.Uint64("startTs", info.GetStartTs()))
	}
	tableInfo, err := c.schemaStore.GetTableInfo(span.GetTableID(), info.GetStartTs())
	if err != nil {
		log.Panic("get table info from schemaStore failed", zap.Error(err), zap.Int64("tableID", span.TableID), zap.Uint64("startTs", info.GetStartTs()))
	}
	dispatcher.updateTableInfo(tableInfo)
	eventStoreRegisterDuration := time.Since(start)

	c.dispatchers.Store(id, dispatcher)

	log.Info("register dispatcher", zap.Uint64("clusterID", c.tidbClusterID),
		zap.Any("dispatcherID", id), zap.Int64("tableID", span.TableID),
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
	stat.(*dispatcherStat).isRemoved.Store(true)
	c.eventStore.UnregisterDispatcher(id)
	c.schemaStore.UnregisterTable(dispatcherInfo.GetTableSpan().TableID)
	c.dispatchers.Delete(id)
	log.Info("deregister acceptor", zap.Uint64("clusterID", c.tidbClusterID), zap.Any("acceptorID", id))
}

func (c *eventBroker) pauseDispatcher(dispatcherInfo DispatcherInfo) {
	stat, ok := c.getDispatcher(dispatcherInfo.GetID())
	if !ok {
		return
	}
	log.Info("pause dispatcher", zap.Any("dispatcher", stat.id))
	stat.isRunning.Store(false)
}

func (c *eventBroker) resumeDispatcher(dispatcherInfo DispatcherInfo) {
	stat, ok := c.getDispatcher(dispatcherInfo.GetID())
	if !ok {
		return
	}
	log.Info("resume dispatcher", zap.Any("dispatcher", stat.id), zap.Uint64("checkpointTs", stat.sentResolvedTs.Load()), zap.Uint64("seq", stat.seq.Load()))
	// Reset the watermark to the startTs of the dispatcherInfo.
	stat.isRunning.Store(true)
}

func (c *eventBroker) resetDispatcher(dispatcherInfo DispatcherInfo) {
	stat, ok := c.getDispatcher(dispatcherInfo.GetID())
	if !ok {
		return
	}
	log.Info("reset dispatcher", zap.Any("dispatcher", stat.id), zap.Uint64("startTs", stat.info.GetStartTs()))
	stat.resetTs.Store(dispatcherInfo.GetStartTs())
	stat.isInitialized.Store(false)
	stat.taskScanning.Store(false)
}
