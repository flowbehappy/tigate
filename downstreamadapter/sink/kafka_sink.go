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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/topicmanager"
	"github.com/pingcap/ticdc/downstreamadapter/sink/types"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	ticonfig "github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	v2 "github.com/pingcap/ticdc/pkg/sink/kafka/v2"
	"github.com/pingcap/ticdc/pkg/sink/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	utils "github.com/pingcap/tiflow/pkg/util"
)

type KafkaSink struct {
	changefeedID common.ChangeFeedID

	dmlWorker *worker.KafkaWorker
	ddlWorker *worker.KafkaDDLWorker
}

func (s *KafkaSink) SinkType() SinkType {
	return KafkaSinkType
}

func NewKafkaSink(changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *ticonfig.SinkConfig) (*KafkaSink, error) {
	ctx := context.Background()
	topic, err := helper.GetTopic(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	scheme := sink.GetScheme(sinkURI)
	protocol, err := helper.GetProtocol(utils.GetOrZero(sinkConfig.Protocol))
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

	columnSelector, err := columnselector.NewColumnSelectors(sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, options.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(ctx, failpointCh)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	metricsCollector := factory.MetricsCollector(utils.RoleProcessor, adminClient)
	dmlProducer := producer.NewKafkaDMLProducer(ctx, changefeedID, asyncProducer, metricsCollector)
	encoderGroup := codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, changefeedID)

	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")
	dmlWorker := worker.NewKafkaWorker(changefeedID, protocol, dmlProducer, encoderGroup, columnSelector, eventRouter, topicManager, statistics)

	encoder, err := codec.NewEventEncoder(ctx, encoderConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncProducer, err := factory.SyncProducer(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlProducer := producer.NewKafkaDDLProducer(ctx, changefeedID, syncProducer)
	ddlWorker := worker.NewKafkaDDLWorker(changefeedID, protocol, ddlProducer, encoder, eventRouter, topicManager, statistics)

	return &KafkaSink{
		changefeedID: changefeedID,
		dmlWorker:    dmlWorker,
		ddlWorker:    ddlWorker,
	}, nil
}

func (s *KafkaSink) AddDMLEvent(event *commonEvent.DMLEvent, tableProgress *types.TableProgress) {
	if event.Len() == 0 {
		return
	}
	tableProgress.Add(event)
	s.dmlWorker.GetEventChan() <- event
}

func (s *KafkaSink) PassBlockEvent(event commonEvent.BlockEvent, tableProgress *types.TableProgress) {
	tableProgress.Pass(event)
}

func (s *KafkaSink) AddBlockEvent(event commonEvent.BlockEvent, tableProgress *types.TableProgress) {
	tableProgress.Add(event)
	switch event.(type) {
	case *commonEvent.DDLEvent:
		if event.(*commonEvent.DDLEvent).TiDBOnly {
			// run callback directly and return
			for _, cb := range event.(*commonEvent.DDLEvent).PostTxnFlushed {
				cb()
			}
			return
		}
		s.ddlWorker.GetDDLEventChan() <- event.(*commonEvent.DDLEvent)
	case *commonEvent.SyncPointEvent:
		log.Error("Kafkasink doesn't support Sync Point Event")
	}
}

func (s *KafkaSink) AddCheckpointTs(ts uint64) {
	s.ddlWorker.GetCheckpointTsChan() <- ts
}

func (s *KafkaSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}
func (s *KafkaSink) Close(removeDDLTsItem bool) error {
	return nil
}

func (s *KafkaSink) CheckStartTs(tableId int64, startTs uint64) (int64, error) {
	return int64(startTs), nil
}
