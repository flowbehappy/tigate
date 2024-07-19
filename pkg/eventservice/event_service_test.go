package eventservice

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/flowbehappy/tigate/downstreamadapter/dispatcher"
	"github.com/flowbehappy/tigate/downstreamadapter/eventcollector"
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/downstreamadapter/writer"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockMessageCenter is a mock implementation of the MessageCenter interface
type mockMessageCenter struct {
	messageCh chan *messaging.TargetMessage
}

func (m *mockMessageCenter) SendEvent(event ...*messaging.TargetMessage) error {
	for _, e := range event {
		m.messageCh <- e
	}
	return nil
}

func (m *mockMessageCenter) SendCommand(command ...*messaging.TargetMessage) error {
	for _, c := range command {
		m.messageCh <- c
	}
	return nil
}

func (m *mockMessageCenter) RegisterHandler(topic common.TopicType, handler messaging.MessageHandler) {
}

func (m *mockMessageCenter) DeRegisterHandler(topic common.TopicType) {
}

func (m *mockMessageCenter) AddTarget(id messaging.ServerId, epoch common.EpochType, addr common.AddressType) {
}

func (m *mockMessageCenter) RemoveTarget(id messaging.ServerId) {
}

func (m *mockMessageCenter) Close() {
}

// mockAcceptorInfo is a mock implementation of the AcceptorInfo interface
type mockAcceptorInfo struct {
	clusterID  uint64
	serverID   string
	id         string
	topic      common.TopicType
	span       *common.TableSpan
	startTs    uint64
	isRegister bool
}

func newMockAcceptorInfo() *mockAcceptorInfo {
	return &mockAcceptorInfo{
		clusterID: 1,
		serverID:  "server1",
		id:        "id1",
		topic:     "topic1",
		span: &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		}},
		startTs:    1,
		isRegister: true,
	}
}

func (m *mockAcceptorInfo) GetID() string {
	return m.id
}

func (m *mockAcceptorInfo) GetClusterID() uint64 {
	return m.clusterID
}

func (m *mockAcceptorInfo) GetTopic() common.TopicType {
	return m.topic
}

func (m *mockAcceptorInfo) GetServerID() string {
	return m.serverID
}

func (m *mockAcceptorInfo) GetTableSpan() *common.TableSpan {
	return m.span
}

func (m *mockAcceptorInfo) GetStartTs() uint64 {
	return m.startTs
}

func (m *mockAcceptorInfo) IsRegister() bool {
	return m.isRegister
}

type mockSpanStats struct {
	startTs       uint64
	watermark     uint64
	pendingEvents []*common.TxnEvent
	onUpdate      func(watermark uint64)
}

func (m *mockSpanStats) update(event []*common.TxnEvent, watermark uint64) {
	m.pendingEvents = append(m.pendingEvents, event...)
	m.watermark = watermark
	m.onUpdate(watermark)
}

// mockEventSource is a mock implementation of the EventSource interface
type mockEventSource struct {
	spans map[uint64]*mockSpanStats
}

func newMockEventSource() *mockEventSource {
	return &mockEventSource{
		spans: make(map[uint64]*mockSpanStats),
	}
}

func (m *mockEventSource) SubscribeTableSpan(span *common.TableSpan, startTs uint64, onSpanUpdate func(watermark uint64)) (uint64, error) {
	log.Info("subscribe table span", zap.Any("span", span), zap.Uint64("startTs", startTs))
	m.spans[span.TableID] = &mockSpanStats{
		startTs:       startTs,
		watermark:     startTs,
		onUpdate:      onSpanUpdate,
		pendingEvents: make([]*common.TxnEvent, 0),
	}
	return startTs, nil
}

func (m *mockEventSource) Read(dataRange ...*common.DataRange) ([][]*common.TxnEvent, error) {
	events := make([][]*common.TxnEvent, 0)
	for _, dr := range dataRange {
		events = append(events, m.spans[dr.Span.TableID].pendingEvents)
		m.spans[dr.Span.TableID].pendingEvents = make([]*common.TxnEvent, 0)
	}
	return events, nil
}

func TestEventServiceBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventSource := newMockEventSource()
	mc := &mockMessageCenter{
		messageCh: make(chan *messaging.TargetMessage, 100),
	}

	es := NewEventService(ctx, mc, eventSource)
	esImpl := es.(*eventService)
	go func() {
		err := es.Run()
		if err != nil {
			t.Errorf("EventService.Run() error = %v", err)
		}
	}()

	acceptorInfo := newMockAcceptorInfo()
	// register acceptor
	esImpl.acceptorInfoCh <- acceptorInfo
	// wait for eventService to process the acceptorInfo
	time.Sleep(time.Second * 2)

	require.Equal(t, 1, len(esImpl.brokers))
	require.NotNil(t, esImpl.brokers[acceptorInfo.GetClusterID()])
	require.Equal(t, 1, len(esImpl.brokers[acceptorInfo.GetClusterID()].spanStats))

	// add events to eventSource
	txnEvent := &common.TxnEvent{
		ClusterID: 1,
		Span:      acceptorInfo.span,
		StartTs:   1,
		CommitTs:  5,
		Rows: []*common.RowChangedEvent{
			{
				PhysicalTableID: 1,
			},
		},
	}

	sourceSpanStat, ok := eventSource.spans[acceptorInfo.span.TableID]
	require.True(t, ok)

	sourceSpanStat.update([]*common.TxnEvent{txnEvent}, txnEvent.CommitTs)

	// receive events from msg center
	msg := <-mc.messageCh
	require.NotNil(t, msg)
	require.Equal(t, acceptorInfo.GetTopic(), msg.Topic)
}

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

// The test mainly focus on the communication between dispatcher and event service.
// When dispatcher created and register in event service, event service need to send events to dispatcher.
func TestDispatcherCommunicateWithEventService(t *testing.T) {
	serverId := messaging.NewServerId()
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(serverId, watcher.TempEpoch, config.NewDefaultMessageCenterConfig()))
	appcontext.SetService(appcontext.EventCollector, eventcollector.NewEventCollector(100*1024*1024*1024, serverId)) // 100GB for demo

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventSource := newMockEventSource()
	eventService := NewEventService(ctx, appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter), eventSource)
	//esImpl := eventService.(*eventService)
	go func() {
		err := eventService.Run()
		if err != nil {
			t.Errorf("EventService.Run() error = %v", err)
		}
	}()

	db, _ := newTestMockDB(t)
	defer db.Close()

	mysqlSink := sink.NewMysqlSink(8, writer.NewMysqlConfig(), db)
	tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: 1, StartKey: nil, EndKey: nil}}
	startTs := uint64(1)

	tableEventDispatcher := dispatcher.NewTableEventDispatcher(tableSpan, mysqlSink, startTs, nil)
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RegisterDispatcher(tableEventDispatcher, startTs)

	time.Sleep(1 * time.Second)
	// add events to eventSource
	txnEvent := &common.TxnEvent{
		ClusterID: 1,
		Span:      tableSpan,
		StartTs:   1,
		CommitTs:  5,
		Rows: []*common.RowChangedEvent{
			{
				PhysicalTableID: 1,
			},
		},
	}

	sourceSpanStat, ok := eventSource.spans[tableSpan.TableID]
	require.True(t, ok)

	sourceSpanStat.update([]*common.TxnEvent{txnEvent}, txnEvent.CommitTs)

	<-tableEventDispatcher.GetEventChan()

}
