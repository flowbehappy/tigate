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
	"sync"
	"time"

	"github.com/flowbehappy/tigate/pkg/config"
	"go.uber.org/atomic"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/eventrouter"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/topicmanager"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/pkg/sink/codec"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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
type KafkaWorker struct {
	// changeFeedID indicates this sink belongs to which processor(changefeed).
	changeFeedID model.ChangeFeedID
	// protocol indicates the protocol used by this sink.
	protocol config.Protocol

	eventChan chan *commonEvent.DMLEvent
	rowChan   chan *commonEvent.MQRowEvent
	// ticker used to force flush the batched messages when the interval is reached.
	ticker *time.Ticker

	columnSelector *common.ColumnSelectors
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

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newWorker creates a new flush worker.
func NewKafkaWorker(
	id model.ChangeFeedID,
	protocol config.Protocol,
	producer dmlproducer.DMLProducer,
	encoderGroup codec.EncoderGroup,
	columnSelector *common.ColumnSelectors,
	eventRouter *eventrouter.EventRouter,
	topicManager topicmanager.TopicManager,
	statistics *metrics.Statistics,
) *KafkaWorker {
	ctx, cancel := context.WithCancel(context.Background())
	w := &KafkaWorker{
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
	}

	w.wg.Add(4)
	go w.calculateKeyPartitions(ctx)

	go func() error {
		defer w.wg.Done()
		return w.encoderGroup.Run(ctx)
	}()
	go func() error {
		defer w.wg.Done()
		if w.protocol.IsBatchEncode() {
			return w.batchEncodeRun(ctx)
		}
		return w.nonBatchEncodeRun(ctx)
	}()
	go w.sendMessages(ctx)
	return w
}

func (w *KafkaWorker) calculateKeyPartitions(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-w.eventChan:
			topic := w.eventRouter.GetTopicForRowChange(event.TableInfo)
			partitionNum, err := w.topicManager.GetPartitionNum(ctx, topic)
			if err != nil {
				log.Error("failed to get partition number for topic", zap.String("topic", topic), zap.Error(err))
				return
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
					log.Error("failed to generate partition index and key for row", zap.Error(err))
					return
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

func (w *KafkaWorker) GetEventChan() chan<- *commonEvent.DMLEvent {
	return w.eventChan
}

// nonBatchEncodeRun add events to the encoder group immediately.
func (w *KafkaWorker) nonBatchEncodeRun(ctx context.Context) error {
	log.Info("MQ sink non batch worker started",
		zap.String("namespace", w.changeFeedID.Namespace),
		zap.String("changefeed", w.changeFeedID.ID),
		zap.String("protocol", w.protocol.String()),
	)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event, ok := <-w.rowChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID))
				return nil
			}
			if err := w.encoderGroup.AddEvents(ctx, event.Key, &event.RowEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batchEncodeRun collect messages into batch and add them to the encoder group.
func (w *KafkaWorker) batchEncodeRun(ctx context.Context) (retErr error) {
	log.Info("MQ sink batch worker started",
		zap.String("namespace", w.changeFeedID.Namespace),
		zap.String("changefeed", w.changeFeedID.ID),
		zap.String("protocol", w.protocol.String()),
	)

	metricBatchDuration := metrics.WorkerBatchDuration.WithLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	metricBatchSize := metrics.WorkerBatchSize.WithLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	defer func() {
		metrics.WorkerBatchDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
		metrics.WorkerBatchSize.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	}()

	msgsBuf := make([]*commonEvent.MQRowEvent, batchSize)
	for {
		start := time.Now()
		msgCount, err := w.batch(ctx, msgsBuf, batchInterval)
		if err != nil {
			return errors.Trace(err)
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
			if err := w.encoderGroup.AddEvents(ctx, key, msg...); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batch collects a batch of messages from w.msgChan into buffer.
// It returns the number of messages collected.
// Note: It will block until at least one message is received.
func (w *KafkaWorker) batch(ctx context.Context, buffer []*commonEvent.MQRowEvent, flushInterval time.Duration) (int, error) {
	msgCount := 0
	maxBatchSize := len(buffer)
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-ctx.Done():
		return msgCount, ctx.Err()
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
		case <-ctx.Done():
			return msgCount, ctx.Err()
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
func (w *KafkaWorker) group(msgs []*commonEvent.MQRowEvent) map[model.TopicPartitionKey][]*commonEvent.RowEvent {
	groupedMsgs := make(map[model.TopicPartitionKey][]*commonEvent.RowEvent)
	for _, msg := range msgs {
		if _, ok := groupedMsgs[msg.Key]; !ok {
			groupedMsgs[msg.Key] = make([]*commonEvent.RowEvent, 0)
		}
		groupedMsgs[msg.Key] = append(groupedMsgs[msg.Key], &msg.RowEvent)
	}
	return groupedMsgs
}

func (w *KafkaWorker) sendMessages(ctx context.Context) error {
	defer w.wg.Done()
	metricSendMessageDuration := metrics.WorkerSendMessageDuration.WithLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	defer metrics.WorkerSendMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)

	var err error
	outCh := w.encoderGroup.Output()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case future, ok := <-outCh:
			if !ok {
				log.Warn("MQ sink encoder's output channel closed",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID))
				return nil
			}
			if err = future.Ready(ctx); err != nil {
				return errors.Trace(err)
			}
			for _, message := range future.Messages {
				start := time.Now()
				if err = w.statistics.RecordBatchExecution(func() (int, int64, error) {
					message.SetPartitionKey(future.Key.PartitionKey)
					if err := w.producer.AsyncSendMessage(
						ctx,
						future.Key.Topic,
						future.Key.Partition,
						message); err != nil {
						return 0, 0, err
					}
					if err != nil {
						log.Error("Async Send Message failed", zap.Any("error", err))
					}
					return message.GetRowsCount(), int64(message.Length()), nil
				}); err != nil {
					return err
				}
				metricSendMessageDuration.Observe(time.Since(start).Seconds())
			}
		}
	}
}

func (w *KafkaWorker) close() {
	w.producer.Close()
}
