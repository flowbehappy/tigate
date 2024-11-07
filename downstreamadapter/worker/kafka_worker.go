// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package worker

import (
	"context"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/topicmanager"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"go.uber.org/zap"
)

const (
	// batchSize is the maximum size of the number of messages in a batch.
	batchSize = 2048
	// batchInterval is the interval of the worker to collect a batch of messages.
	// It shouldn't be too large, otherwise it will lead to a high latency.
	batchInterval = 15 * time.Millisecond
)

// worker will send messages to the DML producer on a batch basis.
type KafkaDMLWorker struct {
	changeFeedID common.ChangeFeedID
	protocol     config.Protocol

	eventChan chan *commonEvent.DMLEvent
	rowChan   chan *commonEvent.MQRowEvent
	// ticker used to force flush the batched messages when the interval is reached.
	ticker *time.Ticker

	columnSelector *columnselector.ColumnSelectors
	// eventRouter used to route events to the right topic and partition.
	eventRouter *eventrouter.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager topicmanager.TopicManager
	encoderGroup codec.EncoderGroup

	// producer is used to send the messages to the Kafka broker.
	producer dmlproducer.DMLProducer

	// statistics is used to record DML metrics.
	statistics *metrics.Statistics

	ctx      context.Context
	cancel   context.CancelFunc
	errGroup *errgroup.Group
}

// NewKafkaWorker creates a dml flush worker for kafka
func NewKafkaWorker(
	ctx context.Context,
	id common.ChangeFeedID,
	protocol config.Protocol,
	producer dmlproducer.DMLProducer,
	encoderGroup codec.EncoderGroup,
	columnSelector *columnselector.ColumnSelectors,
	eventRouter *eventrouter.EventRouter,
	topicManager topicmanager.TopicManager,
	statistics *metrics.Statistics,
	errGroup *errgroup.Group,
) *KafkaDMLWorker {
	ctx, cancel := context.WithCancel(ctx)
	return &KafkaDMLWorker{
		ctx:            ctx,
		changeFeedID:   id,
		protocol:       protocol,
		eventChan:      make(chan *commonEvent.DMLEvent, 32),
		rowChan:        make(chan *commonEvent.MQRowEvent, 32),
		ticker:         time.NewTicker(batchInterval),
		encoderGroup:   encoderGroup,
		columnSelector: columnSelector,
		eventRouter:    eventRouter,
		topicManager:   topicManager,
		producer:       producer,
		statistics:     statistics,
		cancel:         cancel,
		errGroup:       errGroup,
	}
}

func (w *KafkaDMLWorker) Run() {
	w.errGroup.Go(func() error {
		return w.calculateKeyPartitions()
	})

	w.errGroup.Go(func() error {
		return w.encoderGroup.Run(w.ctx)
	})

	w.errGroup.Go(func() error {
		if w.protocol.IsBatchEncode() {
			return w.batchEncodeRun()
		}
		return w.nonBatchEncodeRun()
	})

	w.errGroup.Go(func() error {
		return w.sendMessages()
	})
}

func (w *KafkaDMLWorker) calculateKeyPartitions() error {
	for {
		select {
		case <-w.ctx.Done():
			return errors.Trace(w.ctx.Err())
		case event := <-w.eventChan:
			topic := w.eventRouter.GetTopicForRowChange(event.TableInfo)
			partitionNum, err := w.topicManager.GetPartitionNum(w.ctx, topic)
			if err != nil {
				return errors.Trace(err)
			}
			partitonGenerator := w.eventRouter.GetPartitionGeneratorForRowChange(event.TableInfo)
			selector := w.columnSelector.GetSelector(event.TableInfo.TableName.Schema, event.TableInfo.TableName.Table)
			toRowCallback := func(postTxnFlushed []func(), totalCount uint64) func() {
				var calledCount atomic.Uint64
				// The callback of the last row will trigger the callback of the txn.
				return func() {
					if calledCount.Inc() == totalCount {
						for _, callback := range postTxnFlushed {
							callback()
						}
					}
				}
			}

			rowsCount := uint64(event.Len())
			rowCallback := toRowCallback(event.PostTxnFlushed, rowsCount)

			for {
				row, ok := event.GetNextRow()
				if !ok {
					break
				}

				index, key, err := partitonGenerator.GeneratePartitionIndexAndKey(&row, partitionNum, event.TableInfo, event.CommitTs)
				if err != nil {
					return errors.Trace(err)
				}

				w.rowChan <- &commonEvent.MQRowEvent{
					Key: model.TopicPartitionKey{
						Topic:          topic,
						Partition:      index,
						PartitionKey:   key,
						TotalPartition: partitionNum,
					},
					RowEvent: commonEvent.RowEvent{
						TableInfo:      event.TableInfo,
						CommitTs:       event.CommitTs,
						Event:          row,
						Callback:       rowCallback,
						ColumnSelector: selector,
					},
				}
			}
		}
	}
}

func (w *KafkaDMLWorker) GetEventChan() chan<- *commonEvent.DMLEvent {
	return w.eventChan
}

// nonBatchEncodeRun add events to the encoder group immediately.
func (w *KafkaDMLWorker) nonBatchEncodeRun() error {
	log.Info("MQ sink non batch worker started",
		zap.String("namespace", w.changeFeedID.Namespace()),
		zap.String("changefeed", w.changeFeedID.Name()),
		zap.String("protocol", w.protocol.String()),
	)
	for {
		select {
		case <-w.ctx.Done():
			return errors.Trace(w.ctx.Err())
		case event, ok := <-w.rowChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.changeFeedID.Namespace()),
					zap.String("changefeed", w.changeFeedID.Name()))
				return nil
			}
			if err := w.encoderGroup.AddEvents(w.ctx, event.Key, &event.RowEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batchEncodeRun collect messages into batch and add them to the encoder group.
func (w *KafkaDMLWorker) batchEncodeRun() error {
	log.Info("MQ sink batch worker started",
		zap.String("namespace", w.changeFeedID.Namespace()),
		zap.String("changefeed", w.changeFeedID.Name()),
		zap.String("protocol", w.protocol.String()),
	)

	namespace, changefeed := w.changeFeedID.Namespace(), w.changeFeedID.Name()
	metricBatchDuration := metrics.WorkerBatchDuration.WithLabelValues(namespace, changefeed)
	metricBatchSize := metrics.WorkerBatchSize.WithLabelValues(namespace, changefeed)
	defer func() {
		metrics.WorkerBatchDuration.DeleteLabelValues(namespace, changefeed)
		metrics.WorkerBatchSize.DeleteLabelValues(namespace, changefeed)
	}()

	msgsBuf := make([]*commonEvent.MQRowEvent, batchSize)
	for {
		start := time.Now()
		msgCount, err := w.batch(msgsBuf, batchInterval)
		if err != nil {
			log.Error("kafka dml worker batch failed",
				zap.String("namespace", w.changeFeedID.Namespace()),
				zap.String("changefeed", w.changeFeedID.Name()),
				zap.Error(err))
		}
		if msgCount == 0 {
			continue
		}

		metricBatchSize.Observe(float64(msgCount))
		metricBatchDuration.Observe(time.Since(start).Seconds())

		msgs := msgsBuf[:msgCount]
		// Group messages by its TopicPartitionKey before adding them to the encoder group.
		groupedMsgs := w.group(msgs)
		for key, msg := range groupedMsgs {
			if err := w.encoderGroup.AddEvents(w.ctx, key, msg...); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batch collects a batch of messages from w.msgChan into buffer.
// It returns the number of messages collected.
// Note: It will block until at least one message is received.
func (w *KafkaDMLWorker) batch(buffer []*commonEvent.MQRowEvent, flushInterval time.Duration) (int, error) {
	msgCount := 0
	maxBatchSize := len(buffer)
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-w.ctx.Done():
		return msgCount, w.ctx.Err()
	case msg, ok := <-w.rowChan:
		if !ok {
			log.Warn("MQ sink flush worker channel closed")
			return msgCount, nil
		}

		buffer[msgCount] = msg
		msgCount++
	}

	// Reset the ticker to start a new batching.
	// We need to stop batching when the interval is reached.
	w.ticker.Reset(flushInterval)
	for {
		select {
		case <-w.ctx.Done():
			return msgCount, w.ctx.Err()
		case msg, ok := <-w.rowChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed")
				return msgCount, nil
			}

			buffer[msgCount] = msg
			msgCount++

			if msgCount >= maxBatchSize {
				return msgCount, nil
			}
		case <-w.ticker.C:
			return msgCount, nil
		}
	}
}

// group groups messages by its key.
func (w *KafkaDMLWorker) group(msgs []*commonEvent.MQRowEvent) map[model.TopicPartitionKey][]*commonEvent.RowEvent {
	groupedMsgs := make(map[model.TopicPartitionKey][]*commonEvent.RowEvent)
	for _, msg := range msgs {
		if _, ok := groupedMsgs[msg.Key]; !ok {
			groupedMsgs[msg.Key] = make([]*commonEvent.RowEvent, 0)
		}
		groupedMsgs[msg.Key] = append(groupedMsgs[msg.Key], &msg.RowEvent)
	}
	return groupedMsgs
}

func (w *KafkaDMLWorker) sendMessages() error {
	metricSendMessageDuration := metrics.WorkerSendMessageDuration.WithLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())
	defer metrics.WorkerSendMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())

	var err error
	outCh := w.encoderGroup.Output()
	for {
		select {
		case <-w.ctx.Done():
			return errors.Trace(w.ctx.Err())
		case future, ok := <-outCh:
			if !ok {
				log.Warn("MQ sink encoder's output channel closed",
					zap.String("namespace", w.changeFeedID.Namespace()),
					zap.String("changefeed", w.changeFeedID.Name()))
				return nil
			}
			if err = future.Ready(w.ctx); err != nil {
				return errors.Trace(err)
			}
			for _, message := range future.Messages {
				start := time.Now()
				if err = w.statistics.RecordBatchExecution(func() (int, int64, error) {
					message.SetPartitionKey(future.Key.PartitionKey)
					if err := w.producer.AsyncSendMessage(
						w.ctx,
						future.Key.Topic,
						future.Key.Partition,
						message); err != nil {
						return 0, 0, err
					}
					return message.GetRowsCount(), int64(message.Length()), nil
				}); err != nil {
					return errors.Trace(err)
				}
				metricSendMessageDuration.Observe(time.Since(start).Seconds())
			}
		}
	}
}

func (w *KafkaDMLWorker) Close() error {
	w.ticker.Stop()
	w.cancel()
	w.producer.Close()

	return nil
}
