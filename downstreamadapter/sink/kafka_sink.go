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
	tikafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	utils "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type KafkaSink struct {
	changefeedID common.ChangeFeedID

	dmlWorker *worker.KafkaDMLWorker
	ddlWorker *worker.KafkaDDLWorker

	// the module used by dmlWorker and ddlWorker
	// KafkaSink need to close it when Close() is called
	adminClient  tikafka.ClusterAdminClient
	topicManager topicmanager.TopicManager
	statistics   *metrics.Statistics

	errgroup *errgroup.Group
	errCh    chan error
}

func (s *KafkaSink) SinkType() SinkType {
	return KafkaSinkType
}

func NewKafkaSink(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *ticonfig.SinkConfig, errCh chan error) (*KafkaSink, error) {
	errGroup, ctx := errgroup.WithContext(ctx)
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

	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")

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

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && adminClient != nil {
			adminClient.Close()
		}
	}()

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

	eventRouter, err := eventrouter.NewEventRouter(sinkConfig, protocol, topic, scheme)
	if err != nil {
		return nil, errors.Trace(err)
	}

	columnSelector, err := columnselector.NewColumnSelectors(sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for dml worker
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, options.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	failpointCh := make(chan error, 1)
	dmlAsyncProducer, err := factory.AsyncProducer(ctx, failpointCh)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	metricsCollector := factory.MetricsCollector(utils.RoleProcessor, adminClient)
	dmlProducer := producer.NewKafkaDMLProducer(ctx, changefeedID, dmlAsyncProducer, metricsCollector)
	encoderGroup := codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, changefeedID)

	dmlWorker := worker.NewKafkaWorker(ctx, changefeedID, protocol, dmlProducer, encoderGroup, columnSelector, eventRouter, topicManager, statistics, errGroup)

	// for ddl worker
	encoder, err := codec.NewEventEncoder(ctx, encoderConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ddlSyncProducer, err := factory.SyncProducer(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlProducer := producer.NewKafkaDDLProducer(ctx, changefeedID, ddlSyncProducer)
	ddlWorker := worker.NewKafkaDDLWorker(ctx, changefeedID, protocol, ddlProducer, encoder, eventRouter, topicManager, statistics, errGroup)

	sink := &KafkaSink{
		changefeedID: changefeedID,
		dmlWorker:    dmlWorker,
		ddlWorker:    ddlWorker,
		adminClient:  adminClient,
		topicManager: topicManager,
		statistics:   statistics,
		errgroup:     errGroup,
		errCh:        errCh,
	}
	go sink.run()
	return sink, nil
}

func (s *KafkaSink) run() {
	s.dmlWorker.Run()
	s.ddlWorker.Run()
	s.errCh <- s.errgroup.Wait()
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

func (s *KafkaSink) WriteBlockEvent(event commonEvent.BlockEvent, tableProgress *types.TableProgress) error {
	tableProgress.Add(event)
	switch event := event.(type) {
	case *commonEvent.DDLEvent:
		if event.TiDBOnly {
			// run callback directly and return
			for _, cb := range event.PostTxnFlushed {
				cb()
			}
			return nil
		}
		s.ddlWorker.GetDDLEventChan() <- event
	case *commonEvent.SyncPointEvent:
		log.Error("KafkaSink doesn't support Sync Point Event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", event))
	default:
		log.Error("KafkaSink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event type", event.GetType()))
	}
	return nil
}

func (s *KafkaSink) AddCheckpointTs(ts uint64) {
	s.ddlWorker.GetCheckpointTsChan() <- ts
}

func (s *KafkaSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}

func (s *KafkaSink) Close(removeDDLTsItem bool) error {
	err := s.ddlWorker.Close()
	if err != nil {
		return errors.Trace(err)
	}

	err = s.dmlWorker.Close()
	if err != nil {
		return errors.Trace(err)
	}

	s.adminClient.Close()
	s.topicManager.Close()
	s.statistics.Close()

	return nil
}

func (s *KafkaSink) CheckStartTsList(tableIds []int64, startTsList []int64) ([]int64, error) {
	return startTsList, nil
}
