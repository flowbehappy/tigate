package producer

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
	tikafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func TestDDLSyncBroadcastMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions()
	options.MaxMessages = 1

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := common.ChangefeedID4Test("test", "test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	for i := 0; i < tikafka.DefaultMockPartitionNum; i++ {
		syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	}
	err = p.SyncBroadcastMessage(ctx, tikafka.DefaultMockTopicName,
		tikafka.DefaultMockPartitionNum, &ticommon.Message{Ts: 417318403368288260})
	require.NoError(t, err)

	p.Close()
	err = p.SyncBroadcastMessage(ctx, tikafka.DefaultMockTopicName,
		tikafka.DefaultMockPartitionNum, &ticommon.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestDDLSyncSendMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions()

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := common.ChangefeedID4Test("test", "test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	err = p.SyncSendMessage(ctx, tikafka.DefaultMockTopicName, 0, &ticommon.Message{Ts: 417318403368288260})
	require.NoError(t, err)

	p.Close()
	err = p.SyncSendMessage(ctx, tikafka.DefaultMockTopicName, 0, &ticommon.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestDDLProducerSendMsgFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	options := getOptions()
	options.MaxMessages = 1
	options.MaxMessageBytes = 1

	ctx = context.WithValue(ctx, "testing.T", t)

	// This will make the first send failed.
	changefeed := common.ChangefeedID4Test("test", "test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	defer p.Close()

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndFail(sarama.ErrMessageTooLarge)
	err = p.SyncSendMessage(ctx, tikafka.DefaultMockTopicName, 0, &ticommon.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, sarama.ErrMessageTooLarge)
}

func TestDDLProducerDoubleClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := getOptions()

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := common.ChangefeedID4Test("test", "test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	p.Close()
	p.Close()
}
