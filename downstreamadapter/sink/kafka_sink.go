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

	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/eventrouter"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/topicmanager"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/downstreamadapter/worker"
	"github.com/flowbehappy/tigate/downstreamadapter/worker/dmlproducer"
	"github.com/flowbehappy/tigate/pkg/common"
	ticonfig "github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/sink/codec"
	"github.com/flowbehappy/tigate/pkg/sink/kafka"
	v2 "github.com/flowbehappy/tigate/pkg/sink/kafka/v2"
	tiutils "github.com/flowbehappy/tigate/pkg/sink/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	timetrics "github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	tikafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	utils "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type KafkaSink struct {
	changefeedID model.ChangeFeedID

	protocol config.Protocol

	columnSelector *common.ColumnSelectors
	// eventRouter used to route events to the right topic and partition.
	eventRouter *eventrouter.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager topicmanager.TopicManager

	dmlWorker   *worker.KafkaWorker
	ddlWorker   *worker.KafkaDDLWorker
	adminClient tikafka.ClusterAdminClient
	scheme      string

	ctx    context.Context
	cancel context.CancelCauseFunc // todo?
}

func (s *KafkaSink) SinkType() SinkType {
	return KafkaSinkType
}

func NewKafkaSink(changefeedID model.ChangeFeedID, sinkURI *url.URL, sinkConfig *ticonfig.SinkConfig) (*KafkaSink, error) {
	ctx, cancel := context.WithCancelCause(context.Background())
	topic, err := util.GetTopic(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	scheme := sink.GetScheme(sinkURI)
	protocol, err := util.GetProtocol(utils.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return nil, errors.Trace(err)
	}

	options := kafka.NewOptions()
	if err := options.Apply(changefeedID, sinkURI, sinkConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	factoryCreator := kafka.NewSaramaFactory
	if utils.GetOrZero(sinkConfig.EnableKafkaSinkV2) {
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

	// adjust the option configuration before creating the kafka client
	if err = kafka.AdjustOptions(ctx, adminClient, options, topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

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

	eventRouter, err := eventrouter.NewEventRouter(sinkConfig, protocol, topic, scheme)
	if err != nil {
		return nil, errors.Trace(err)
	}

	columnSelector, err := common.NewColumnSelectors(sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := tiutils.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, options.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(ctx, failpointCh)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	metricsCollector := factory.MetricsCollector(utils.RoleProcessor, adminClient)
	dmlProducer := dmlproducer.NewKafkaDMLProducer(ctx, changefeedID, asyncProducer, metricsCollector)
	encoderGroup := codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, changefeedID)

	statistics := timetrics.NewStatistics(changefeedID, sink.RowSink)
	dmlWorker := worker.NewKafkaWorker(changefeedID, protocol, dmlProducer, encoderGroup, statistics)

	encoder, err := codec.NewEventEncoder(ctx, encoderConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncProducer, err := factory.SyncProducer(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlProducer := ddlproducer.NewKafkaDDLProducer(ctx, changefeedID, syncProducer)
	ddlWorker := worker.NewKafkaDDLWorker(changefeedID, protocol, ddlProducer, encoder, statistics)

	return &KafkaSink{
		ctx:            ctx,
		cancel:         cancel,
		topicManager:   topicManager,
		eventRouter:    eventRouter,
		columnSelector: columnSelector,
		dmlWorker:      dmlWorker,
		ddlWorker:      ddlWorker,
	}, nil
}

func (s *KafkaSink) AddDMLEvent(event *common.DMLEvent, tableProgress *types.TableProgress) {
	if event.Len() == 0 {
		return
	}
	tableProgress.Add(event)

	topic := s.eventRouter.GetTopicForRowChange(event.TableInfo)
	partitionNum, err := s.topicManager.GetPartitionNum(s.ctx, topic)
	if err != nil {
		s.cancel(err)
		log.Error("failed to get partition number for topic", zap.String("topic", topic), zap.Error(err))
		return
	}
	partitonGenerator := s.eventRouter.GetPartitionGeneratorForRowChange(event.TableInfo)
	selector := s.columnSelector.GetSelector(event.TableInfo.TableName.Schema, event.TableInfo.TableName.Table)
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
			s.cancel(err)
			log.Error("failed to generate partition index and key for row", zap.Error(err))
			return
		}

		s.dmlWorker.GetEventChan() <- &common.MQRowEvent{
			Key: model.TopicPartitionKey{
				Topic:          topic,
				Partition:      index,
				PartitionKey:   key,
				TotalPartition: partitionNum,
			},
			RowEvent: common.RowEvent{
				TableInfo:      event.TableInfo,
				CommitTs:       event.CommitTs,
				Event:          row,
				Callback:       rowCallback,
				ColumnSelector: selector,
			},
		}
	}

}

func (s *KafkaSink) PassDDLAndSyncPointEvent(event *common.DDLEvent, tableProgress *types.TableProgress) {
	tableProgress.Pass(event)
}

func (s *KafkaSink) AddDDLAndSyncPointEvent(event *common.DDLEvent, tableProgress *types.TableProgress) {
	tableProgress.Add(event)
	s.ddlWorker.GetEventChan() <- event
}

func (s *KafkaSink) AddCheckpointTs(ts uint64) {
	//s.ddlWorker.GetEventChan() <- &common.ResolvedEvent{ResolvedTs: ts}
}

func (s *KafkaSink) Close() {
}
