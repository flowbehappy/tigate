// Copyright 2024 PingCAP, Inc.
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

package sink

import (
	"context"
	"net/url"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/columnselector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/eventrouter"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/topicmanager"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/downstreamadapter/worker"
	"github.com/flowbehappy/tigate/downstreamadapter/worker/dmlproducer"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec"
	"github.com/flowbehappy/tigate/pkg/sink/codec/builder"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	timetrics "github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	v2 "github.com/pingcap/tiflow/pkg/sink/kafka/v2"
	utils "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type KafkaSink struct {
	changefeedID model.ChangeFeedID

	protocol config.Protocol

	columnSelector *columnselector.ColumnSelector
	// eventRouter used to route events to the right topic and partition.
	eventRouter *eventrouter.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager topicmanager.TopicManager

	worker      *worker.KafkaWorker
	adminClient kafka.ClusterAdminClient
	scheme      string

	ctx    context.Context
	cancel context.CancelCauseFunc // todo?
}

func NewKafkaSink(changefeedID model.ChangeFeedID, sinkURI *url.URL, replicaConfig *config.ReplicaConfig) (*KafkaSink, error) {
	ctx, cancel := context.WithCancelCause(context.Background())
	topic, err := util.GetTopic(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	scheme := sink.GetScheme(sinkURI)
	protocol, err := util.GetProtocol(utils.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		return nil, errors.Trace(err)
	}

	options := kafka.NewOptions()
	if err := options.Apply(changefeedID, sinkURI, replicaConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	// todo
	factoryCreator := kafka.NewSaramaFactory
	if utils.GetOrZero(replicaConfig.Sink.EnableKafkaSinkV2) {
		factoryCreator = v2.NewFactory
	}

	factory, err := factoryCreator(options, changefeedID)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	adminClient, err := factory.AdminClient(ctx)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	eventRouter, err := eventrouter.NewEventRouter(replicaConfig, protocol, topic, scheme)
	if err != nil {
		return nil, errors.Trace(err)
	}

	columnSelector, err := columnselector.New(replicaConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, replicaConfig, options.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	encoderBuilder, err := builder.NewRowEventEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(ctx, failpointCh)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	metricsCollector := factory.MetricsCollector(utils.RoleProcessor, adminClient)
	dmlProducer := dmlproducer.NewKafkaDMLProducer(ctx, changefeedID, asyncProducer, metricsCollector)
	encoderGroup := codec.NewEncoderGroup(replicaConfig.Sink, encoderBuilder, changefeedID)

	statistics := timetrics.NewStatistics(changefeedID, sink.RowSink)
	worker := worker.NewKafkaWorker(changefeedID, protocol, dmlProducer, encoderGroup, statistics)

	topicManager, err := topicmanager.GetTopicManagerAndTryCreateTopic(
		ctx,
		changefeedID,
		topic,
		options.DeriveTopicConfig(),
		adminClient,
	)

	if err != nil {
		if adminClient != nil {
			adminClient.Close()
		}
		return nil, err
	}
	return &KafkaSink{
		ctx:            ctx,
		cancel:         cancel,
		topicManager:   topicManager,
		eventRouter:    eventRouter,
		columnSelector: columnSelector,
		worker:         worker,
	}, nil
}

func (s *KafkaSink) AddDMLEvent(event *common.TxnEvent, tableProgress *types.TableProgress) {
	if len(event.GetRows()) == 0 {
		return
	}
	tableProgress.Add(event)

	firstRow := event.GetRows()[0]
	topic := s.eventRouter.GetTopicForRowChange(firstRow)
	partitionNum, err := s.topicManager.GetPartitionNum(s.ctx, topic)
	if err != nil {
		s.cancel(err)
		log.Error("failed to get partition number for topic", zap.String("topic", topic), zap.Error(err))
		return
	}
	partitonGenerator := s.eventRouter.GetPartitionGeneratorForRowChange(firstRow)

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

	rowsCount := uint64(len(event.GetRows()))
	rowCallback := toRowCallback(event.PostTxnFlushed, rowsCount)

	for _, row := range event.GetRows() {
		err = s.columnSelector.Apply(row)
		if err != nil {
			s.cancel(err)
			log.Error("failed to do column selector for row", zap.Error(err))
			return
		}

		index, key, err := partitonGenerator.GeneratePartitionIndexAndKey(row, partitionNum)
		if err != nil {
			s.cancel(err)
			log.Error("failed to generate partition index and key for row", zap.Error(err))
			return
		}

		s.worker.GetEventChan() <- common.MQRowEvent{
			Key: model.TopicPartitionKey{
				Topic:          topic,
				Partition:      index,
				PartitionKey:   key,
				TotalPartition: partitionNum,
			},
			RowEvent: common.RowEvent{
				Event:    row,
				Callback: rowCallback,
			},
		}
	}

}

func (s *KafkaSink) PassDDLAndSyncPointEvent(event *common.TxnEvent, tableProgress *types.TableProgress) {
	tableProgress.Pass(event)
}

func (s *KafkaSink) AddDDLAndSyncPointEvent(event *common.TxnEvent, tableProgress *types.TableProgress) {
}

func (s *KafkaSink) Close() {
}
