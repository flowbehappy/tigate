package worker

import (
	"context"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/eventrouter"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/topicmanager"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec/encoder"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/metrics/mq"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type CheckpointInfo struct {
	Ts         uint64
	TableNames []*common.SchemaTableName
}

// worker will send messages to the DML producer on a batch basis.
type KafkaDDLWorker struct {
	// changeFeedID indicates this sink belongs to which processor(changefeed).
	changeFeedID model.ChangeFeedID
	// protocol indicates the protocol used by this sink.
	protocol           config.Protocol
	ddlEventChan       chan *common.DDLEvent
	checkpointInfoChan chan *CheckpointInfo
	// ticker used to force flush the batched messages when the interval is reached.
	ticker *time.Ticker

	encoder encoder.EventEncoder
	// eventRouter used to route events to the right topic and partition.
	eventRouter *eventrouter.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager topicmanager.TopicManager

	// producer is used to send the messages to the Kafka broker.
	producer ddlproducer.DDLProducer

	// metricMQWorkerSendMessageDuration tracks the time duration cost on send messages.
	metricMQWorkerSendMessageDuration prometheus.Observer
	// metricMQWorkerBatchSize tracks each batch's size.
	metricMQWorkerBatchSize prometheus.Observer
	// metricMQWorkerBatchDuration tracks the time duration cost on batch messages.
	metricMQWorkerBatchDuration prometheus.Observer
	// statistics is used to record DML metrics.
	statistics    *metrics.Statistics
	partitionRule DDLDispatchRule
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// DDLDispatchRule is the dispatch rule for DDL event.
type DDLDispatchRule int

const (
	// PartitionZero means the DDL event will be dispatched to partition 0.
	// NOTICE: Only for canal and canal-json protocol.
	PartitionZero DDLDispatchRule = iota
	// PartitionAll means the DDL event will be broadcast to all the partitions.
	PartitionAll
)

func getDDLDispatchRule(protocol config.Protocol) DDLDispatchRule {
	switch protocol {
	case config.ProtocolCanal, config.ProtocolCanalJSON:
		return PartitionZero
	default:
	}
	return PartitionAll
}

// newWorker creates a new flush worker.
func NewKafkaDDLWorker(
	id model.ChangeFeedID,
	protocol config.Protocol,
	producer ddlproducer.DDLProducer,
	encoder encoder.EventEncoder,
	eventRouter *eventrouter.EventRouter,
	topicManager topicmanager.TopicManager,
	statistics *metrics.Statistics,
) *KafkaDDLWorker {
	ctx, cancel := context.WithCancel(context.Background())
	w := &KafkaDDLWorker{
		ctx:                               ctx,
		changeFeedID:                      id,
		protocol:                          protocol,
		ddlEventChan:                      make(chan *common.DDLEvent, 16),
		ticker:                            time.NewTicker(batchInterval),
		encoder:                           encoder,
		producer:                          producer,
		eventRouter:                       eventRouter,
		topicManager:                      topicManager,
		metricMQWorkerSendMessageDuration: mq.WorkerSendMessageDuration.WithLabelValues(id.Namespace, id.ID),
		metricMQWorkerBatchSize:           mq.WorkerBatchSize.WithLabelValues(id.Namespace, id.ID),
		metricMQWorkerBatchDuration:       mq.WorkerBatchDuration.WithLabelValues(id.Namespace, id.ID),
		statistics:                        statistics,
		cancel:                            cancel,
		partitionRule:                     getDDLDispatchRule(protocol),
	}

	w.wg.Add(1)
	go w.encodeAndSendDDLEvents()
	go w.encodeAndSendCheckpointEvents()
	return w
}

func (w *KafkaDDLWorker) GetDDLEventChan() chan<- *common.DDLEvent {
	return w.ddlEventChan
}

func (w *KafkaDDLWorker) GetCheckpointInfoChan() chan<- *CheckpointInfo {
	return w.checkpointInfoChan
}

func (w *KafkaDDLWorker) encodeAndSendDDLEvents() error {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return errors.Trace(w.ctx.Err())
		case event, ok := <-w.ddlEventChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID))
				return nil
			}
			message, err := w.encoder.EncodeDDLEvent(event)
			if err != nil {
				log.Error("Failed to encode ddl event",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID),
					zap.Error(err))
				continue
			}

			topic := w.eventRouter.GetTopicForDDL(event)
			partitionNum, err := w.topicManager.GetPartitionNum(w.ctx, topic)
			if err != nil {
				log.Error("failed to get partition number for topic", zap.String("topic", topic), zap.Error(err))
				continue
			}

			if w.partitionRule == PartitionAll {
				err = w.statistics.RecordDDLExecution(func() error {
					return w.producer.SyncBroadcastMessage(w.ctx, topic, partitionNum, message)
				})
				return errors.Trace(err)
			}
			err = w.statistics.RecordDDLExecution(func() error {
				return w.producer.SyncSendMessage(w.ctx, topic, 0, message)
			})

			if err != nil {
				log.Error("Failed to RecordDDLExecution",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID),
					zap.Error(err))
				continue
			}

		}
	}
}

func (w *KafkaDDLWorker) encodeAndSendCheckpointEvents() error {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return errors.Trace(w.ctx.Err())
		case checkpointInfo, ok := <-w.checkpointInfoChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID))
				return nil
			}
			ts := checkpointInfo.Ts
			tableNames := checkpointInfo.TableNames
			msg, err := w.encoder.EncodeCheckpointEvent(ts)
			if err != nil {
				return errors.Trace(err)
			}
			// NOTICE: When there are no tables to replicate,
			// we need to send checkpoint ts to the default topic.
			// This will be compatible with the old behavior.
			if len(tableNames) == 0 {
				topic := w.eventRouter.GetDefaultTopic()
				partitionNum, err := w.topicManager.GetPartitionNum(w.ctx, topic)
				if err != nil {
					return errors.Trace(err)
				}
				log.Debug("Emit checkpointTs to default topic",
					zap.String("topic", topic), zap.Uint64("checkpointTs", ts))
				err = w.producer.SyncBroadcastMessage(w.ctx, topic, partitionNum, msg)
				return errors.Trace(err)
			}

			topics := w.eventRouter.GetActiveTopics(tableNames)
			for _, topic := range topics {
				partitionNum, err := w.topicManager.GetPartitionNum(w.ctx, topic)
				if err != nil {
					return errors.Trace(err)
				}
				err = w.producer.SyncBroadcastMessage(w.ctx, topic, partitionNum, msg)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}
