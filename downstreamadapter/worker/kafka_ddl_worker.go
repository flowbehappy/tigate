package worker

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/topicmanager"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec/encoder"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// worker will send messages to the DML producer on a batch basis.
type KafkaDDLWorker struct {
	// changeFeedID indicates this sink belongs to which processor(changefeed).
	changeFeedID common.ChangeFeedID
	// protocol indicates the protocol used by this sink.
	protocol         config.Protocol
	checkpointTsChan chan uint64

	encoder encoder.EventEncoder
	// eventRouter used to route events to the right topic and partition.
	eventRouter *eventrouter.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager topicmanager.TopicManager

	// producer is used to send the messages to the Kafka broker.
	producer ddlproducer.DDLProducer

	tableSchemaStore *util.TableSchemaStore

	statistics    *metrics.Statistics
	partitionRule DDLDispatchRule
	ctx           context.Context
	cancel        context.CancelFunc
	errGroup      *errgroup.Group
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
	ctx context.Context,
	id common.ChangeFeedID,
	protocol config.Protocol,
	producer ddlproducer.DDLProducer,
	encoder encoder.EventEncoder,
	eventRouter *eventrouter.EventRouter,
	topicManager topicmanager.TopicManager,
	statistics *metrics.Statistics,
	errGroup *errgroup.Group,
) *KafkaDDLWorker {
	ctx, cancel := context.WithCancel(ctx)
	return &KafkaDDLWorker{
		ctx:              ctx,
		changeFeedID:     id,
		protocol:         protocol,
		encoder:          encoder,
		producer:         producer,
		eventRouter:      eventRouter,
		topicManager:     topicManager,
		statistics:       statistics,
		partitionRule:    getDDLDispatchRule(protocol),
		checkpointTsChan: make(chan uint64, 16),
		cancel:           cancel,
		errGroup:         errGroup,
	}
}

func (w *KafkaDDLWorker) Run() {
	w.errGroup.Go(func() error {
		return w.encodeAndSendCheckpointEvents()
	})
}

func (w *KafkaDDLWorker) GetCheckpointTsChan() chan<- uint64 {
	return w.checkpointTsChan
}

func (w *KafkaDDLWorker) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	w.tableSchemaStore = tableSchemaStore
}

func (w *KafkaDDLWorker) WriteBlockEvent(event *event.DDLEvent) error {
	messages := make([]*ticommon.Message, 0)
	topics := make([]string, 0)

	// Some ddl event may be multi-events, we need to split it into multiple messages.
	// Such as rename table test.table1 to test.table10, test.table2 to test.table20
	if event.IsMultiEvents() {
		subEvents := event.GetSubEvents()
		for _, subEvent := range subEvents {
			message, err := w.encoder.EncodeDDLEvent(&subEvent)
			if err != nil {
				return errors.Trace(err)
			}
			topic := w.eventRouter.GetTopicForDDL(&subEvent)
			messages = append(messages, message)
			topics = append(topics, topic)
		}
	} else {
		message, err := w.encoder.EncodeDDLEvent(event)
		if err != nil {
			return errors.Trace(err)
		}
		topic := w.eventRouter.GetTopicForDDL(event)
		messages = append(messages, message)
		topics = append(topics, topic)
	}

	for i, message := range messages {
		topic := topics[i]
		partitionNum, err := w.topicManager.GetPartitionNum(w.ctx, topic)
		if err != nil {
			return errors.Trace(err)
		}

		if w.partitionRule == PartitionAll {
			err = w.statistics.RecordDDLExecution(func() error {
				return w.producer.SyncBroadcastMessage(w.ctx, topic, partitionNum, message)
			})
		} else {
			err = w.statistics.RecordDDLExecution(func() error {
				return w.producer.SyncSendMessage(w.ctx, topic, 0, message)
			})
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	// after flush all the ddl event, we call the callback function.
	event.PostFlush()
	return nil
}

func (w *KafkaDDLWorker) encodeAndSendCheckpointEvents() error {
	checkpointTsMessageDuration := metrics.CheckpointTsMessageDuration.WithLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())
	checkpointTsMessageCount := metrics.CheckpointTsMessageCount.WithLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())

	defer func() {
		metrics.CheckpointTsMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())
		metrics.CheckpointTsMessageCount.DeleteLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())
	}()

	for {
		select {
		case <-w.ctx.Done():
			return errors.Trace(w.ctx.Err())
		case ts, ok := <-w.checkpointTsChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.changeFeedID.Namespace()),
					zap.String("changefeed", w.changeFeedID.Name()))
				return nil
			}
			start := time.Now()

			msg, err := w.encoder.EncodeCheckpointEvent(ts)
			if err != nil {
				return errors.Trace(err)
			}
			tableNames := w.tableSchemaStore.GetAllTableNames(ts)
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
					zap.String("topic", topic), zap.Uint64("checkpointTs", ts), zap.Any("partitionNum", partitionNum))
				err = w.producer.SyncBroadcastMessage(w.ctx, topic, partitionNum, msg)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
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

			checkpointTsMessageCount.Inc()
			checkpointTsMessageDuration.Observe(time.Since(start).Seconds())
		}
	}
}

func (w *KafkaDDLWorker) Close() error {
	w.cancel()
	w.producer.Close()

	return nil
}
