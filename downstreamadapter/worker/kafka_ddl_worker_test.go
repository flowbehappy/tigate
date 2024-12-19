package worker

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// ddl | checkpoint ts

func kafkaDDLWorkerForTest(t *testing.T) *KafkaDDLWorker {
	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	openProtocol := "open-protocol"
	sinkConfig := &config.SinkConfig{Protocol: &openProtocol}
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	kafkaComponent, protocol, err := GetKafkaSinkComponentForTest(ctx, changefeedID, sinkURI, sinkConfig)
	require.NoError(t, err)

	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")
	errGroup, ctx := errgroup.WithContext(ctx)
	ddlMockProducer := producer.NewMockDDLProducer()

	ddlWorker := NewKafkaDDLWorker(ctx, changefeedID, protocol, ddlMockProducer,
		kafkaComponent.Encoder, kafkaComponent.EventRouter, kafkaComponent.TopicManager,
		statistics, errGroup)
	return ddlWorker
}

func TestWriteDDLEvents(t *testing.T) {
	count = 0

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 1,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count++ },
		},
	}

	ddlEvent2 := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count++ },
		},
	}

	ddlWorker := kafkaDDLWorkerForTest(t)
	err := ddlWorker.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	err = ddlWorker.WriteBlockEvent(ddlEvent2)
	require.NoError(t, err)

	// Wait for the events to be received by the worker.
	require.Len(t, ddlWorker.producer.(*producer.MockProducer).GetAllEvents(), 2)
	require.Equal(t, count, 2)
}

func TestWriteCheckpointTs(t *testing.T) {
	ddlWorker := kafkaDDLWorkerForTest(t)
	ddlWorker.Run()

	tableSchemaStore := util.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{}, common.KafkaSinkType)
	ddlWorker.SetTableSchemaStore(tableSchemaStore)

	ddlWorker.GetCheckpointTsChan() <- 1
	ddlWorker.GetCheckpointTsChan() <- 2

	time.Sleep(1 * time.Second)

	require.Len(t, ddlWorker.producer.(*producer.MockProducer).GetAllEvents(), 2)

}
