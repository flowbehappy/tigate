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

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/metrics/mq"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
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
	// msgChan caches the messages to be sent.
	// It is an unbounded channel.
	msgChan *chann.DrainableChann[*common.MQRowEvent]
	// ticker used to force flush the batched messages when the interval is reached.
	ticker *time.Ticker

	encoderGroup codec.EncoderGroup

	// producer is used to send the messages to the Kafka broker.
	producer dmlproducer.DMLProducer

	// metricMQWorkerSendMessageDuration tracks the time duration cost on send messages.
	metricMQWorkerSendMessageDuration prometheus.Observer
	// metricMQWorkerBatchSize tracks each batch's size.
	metricMQWorkerBatchSize prometheus.Observer
	// metricMQWorkerBatchDuration tracks the time duration cost on batch messages.
	metricMQWorkerBatchDuration prometheus.Observer
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
	statistics *metrics.Statistics,
) *KafkaWorker {
	ctx, cancel := context.WithCancel(context.Background())
	w := &KafkaWorker{
		changeFeedID:                      id,
		protocol:                          protocol,
		msgChan:                           chann.NewAutoDrainChann[*common.MQRowEvent](),
		ticker:                            time.NewTicker(batchInterval),
		encoderGroup:                      encoderGroup,
		producer:                          producer,
		metricMQWorkerSendMessageDuration: mq.WorkerSendMessageDuration.WithLabelValues(id.Namespace, id.ID),
		metricMQWorkerBatchSize:           mq.WorkerBatchSize.WithLabelValues(id.Namespace, id.ID),
		metricMQWorkerBatchDuration:       mq.WorkerBatchDuration.WithLabelValues(id.Namespace, id.ID),
		statistics:                        statistics,
		cancel:                            cancel,
	}

	w.wg.Add(3)
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
	go func() error {
		defer w.wg.Done()
		return w.sendMessages(ctx)
	}()
	return w
}

func (w *KafkaWorker) GetEventChan() chan<- *common.MQRowEvent {
	return w.msgChan.In()
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
		case event, ok := <-w.msgChan.Out():
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

	msgsBuf := make([]*common.MQRowEvent, batchSize)
	for {
		start := time.Now()
		msgCount, err := w.batch(ctx, msgsBuf, batchInterval)
		if err != nil {
			return errors.Trace(err)
		}
		if msgCount == 0 {
			continue
		}

		w.metricMQWorkerBatchSize.Observe(float64(msgCount))
		w.metricMQWorkerBatchDuration.Observe(time.Since(start).Seconds())

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
func (w *KafkaWorker) batch(ctx context.Context, buffer []*common.MQRowEvent, flushInterval time.Duration) (int, error) {
	msgCount := 0
	maxBatchSize := len(buffer)
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-ctx.Done():
		return msgCount, ctx.Err()
	case msg, ok := <-w.msgChan.Out():
		if !ok {
			log.Warn("MQ sink flush worker channel closed")
			return msgCount, nil
		}
		//if msg.RowEvent != nil {
		//w.statistics.ObserveRows(msg.RowEvent)
		buffer[msgCount] = msg
		msgCount++
		// }
	}

	// Reset the ticker to start a new batching.
	// We need to stop batching when the interval is reached.
	w.ticker.Reset(flushInterval)
	for {
		select {
		case <-ctx.Done():
			return msgCount, ctx.Err()
		case msg, ok := <-w.msgChan.Out():
			if !ok {
				log.Warn("MQ sink flush worker channel closed")
				return msgCount, nil
			}

			//if msg.RowEvent != nil {
			//w.statistics.ObserveRows(msg.rowEvent.Event)
			buffer[msgCount] = msg
			msgCount++
			//}

			if msgCount >= maxBatchSize {
				return msgCount, nil
			}
		case <-w.ticker.C:
			return msgCount, nil
		}
	}
}

// group groups messages by its key.
func (w *KafkaWorker) group(msgs []*common.MQRowEvent) map[model.TopicPartitionKey][]*common.RowEvent {
	groupedMsgs := make(map[model.TopicPartitionKey][]*common.RowEvent)
	for _, msg := range msgs {
		if _, ok := groupedMsgs[msg.Key]; !ok {
			groupedMsgs[msg.Key] = make([]*common.RowEvent, 0)
		}
		groupedMsgs[msg.Key] = append(groupedMsgs[msg.Key], &msg.RowEvent)
	}
	return groupedMsgs
}

func (w *KafkaWorker) sendMessages(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	metric := codec.EncoderGroupOutputChanSizeGauge.
		WithLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	defer func() {
		ticker.Stop()
		codec.EncoderGroupOutputChanSizeGauge.
			DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	}()

	var err error
	outCh := w.encoderGroup.Output()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			metric.Set(float64(len(outCh)))
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
					return message.GetRowsCount(), int64(message.Length()), nil
				}); err != nil {
					return err
				}
				w.metricMQWorkerSendMessageDuration.Observe(time.Since(start).Seconds())
			}
		}
	}
}

func (w *KafkaWorker) close() {
	w.msgChan.CloseAndDrain()
	w.producer.Close()
	mq.WorkerSendMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	mq.WorkerBatchSize.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	mq.WorkerBatchDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
}
