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
	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	tconfig "github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var _ messaging.MessageCenter = &mockMessageCenter{}

// mockMessageCenter is a mock implementation of the MessageCenter interface
type mockMessageCenter struct {
	messageCh chan *messaging.TargetMessage
}

func (m *mockMessageCenter) OnNodeChanges(nodeInfos map[string]*common.NodeInfo) {

}

func (m *mockMessageCenter) SendEvent(event *messaging.TargetMessage) error {
	m.messageCh <- event
	return nil
}

func (m *mockMessageCenter) SendCommand(command *messaging.TargetMessage) error {
	m.messageCh <- command
	return nil
}

func (m *mockMessageCenter) RegisterHandler(topic string, handler messaging.MessageHandler) {
}

func (m *mockMessageCenter) DeRegisterHandler(topic string) {
}

func (m *mockMessageCenter) AddTarget(id messaging.ServerId, epoch uint64, addr string) {
}

func (m *mockMessageCenter) RemoveTarget(id messaging.ServerId) {
}

func (m *mockMessageCenter) Close() {
}

// mockDispatcherInfo is a mock implementation of the AcceptorInfo interface
type mockDispatcherInfo struct {
	clusterID  uint64
	serverID   string
	id         common.DispatcherID
	topic      string
	span       *common.TableSpan
	startTs    uint64
	isRegister bool
}

func newMockAcceptorInfo(dispatcherID common.DispatcherID, tableID uint64) *mockDispatcherInfo {
	return &mockDispatcherInfo{
		clusterID: 1,
		serverID:  "server1",
		id:        dispatcherID,
		topic:     "topic1",
		span: &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		}},
		startTs:    1,
		isRegister: true,
	}
}

func (m *mockDispatcherInfo) GetID() common.DispatcherID {
	return m.id
}

func (m *mockDispatcherInfo) GetClusterID() uint64 {
	return m.clusterID
}

func (m *mockDispatcherInfo) GetTopic() string {
	return m.topic
}

func (m *mockDispatcherInfo) GetServerID() string {
	return m.serverID
}

func (m *mockDispatcherInfo) GetTableSpan() *common.TableSpan {
	return m.span
}

func (m *mockDispatcherInfo) GetStartTs() uint64 {
	return m.startTs
}

func (m *mockDispatcherInfo) IsRegister() bool {
	return m.isRegister
}

func (m *mockDispatcherInfo) GetChangefeedID() (namespace, id string) {
	return "default", "test"
}

func (m *mockDispatcherInfo) GetFilterConfig() *tconfig.FilterConfig {
	return nil
}

type mockSpanStats struct {
	startTs       uint64
	watermark     uint64
	pendingEvents []*common.TxnEvent
	onUpdate      func(watermark uint64)
	onEvent       func(event *common.RawKVEntry)
}

func (m *mockSpanStats) update(event []*common.TxnEvent, watermark uint64) {
	m.pendingEvents = append(m.pendingEvents, event...)
	m.watermark = watermark
	for _, e := range event {
		for range e.Rows {
			m.onEvent(&common.RawKVEntry{})
		}
	}
	m.onUpdate(watermark)

}

// mockEventStore is a mock implementation of the EventStore interface
type mockEventStore struct {
	spans map[uint64]*mockSpanStats
}

func newMockEventStore() *mockEventStore {
	return &mockEventStore{
		spans: make(map[uint64]*mockSpanStats),
	}
}

func (m *mockEventStore) Name() string {
	return "mockEventStore"
}

func (m *mockEventStore) Run(ctx context.Context) error {
	return nil
}

func (m *mockEventStore) Close(ctx context.Context) error {
	return nil
}

func (m *mockEventStore) RegisterDispatcher(
	dispatcherID common.DispatcherID,
	span *common.TableSpan,
	startTS common.Ts,
	observer eventstore.EventObserver,
	notifier eventstore.WatermarkNotifier,
) error {
	log.Info("subscribe table span", zap.Any("span", span), zap.Uint64("startTs", uint64(startTS)))
	m.spans[uint64(span.TableID)] = &mockSpanStats{
		startTs:       uint64(startTS),
		watermark:     uint64(startTS),
		onUpdate:      notifier,
		onEvent:       observer,
		pendingEvents: make([]*common.TxnEvent, 0),
	}
	return nil
}

func (m *mockEventStore) UpdateDispatcherSendTS(dispatcherID common.DispatcherID, gcTS uint64) error {
	return nil
}

func (m *mockEventStore) UnregisterDispatcher(dispatcherID common.DispatcherID) error {
	return nil
}

func (m *mockEventStore) GetIterator(dataRange *common.DataRange) (eventstore.EventIterator, error) {
	iter := &mockEventIterator{
		events: make([]*common.TxnEvent, 0),
	}

	for _, e := range m.spans[dataRange.Span.TableID].pendingEvents {
		if e.CommitTs > dataRange.StartTs && e.CommitTs <= dataRange.EndTs {
			iter.events = append(iter.events, e)
		}
	}

	return iter, nil
}

type mockEventIterator struct {
	events     []*common.TxnEvent
	currentTxn *common.TxnEvent
}

func (m *mockEventIterator) Next() (*common.RowChangedEvent, bool, error) {
	if len(m.events) == 0 && m.currentTxn == nil {
		return nil, false, nil
	}

	isNewTxn := false
	if m.currentTxn == nil {
		m.currentTxn = m.events[0]
		m.events = m.events[1:]
		isNewTxn = true
	}

	if len(m.currentTxn.Rows) == 0 {
		return nil, false, nil
	}

	row := m.currentTxn.Rows[0]
	m.currentTxn.Rows = m.currentTxn.Rows[1:]
	if len(m.currentTxn.Rows) == 0 {
		m.currentTxn = nil
	}

	return row, isNewTxn, nil
}

func (m *mockEventIterator) Close() (int64, error) {
	return 0, nil
}

// This test is to test the mockEventIterator works as expected.
func TestMockEventIterator(t *testing.T) {
	iter := &mockEventIterator{
		events: make([]*common.TxnEvent, 0),
	}

	// Case 1: empty iterator
	row, isNewTxn, err := iter.Next()
	require.Nil(t, err)
	require.False(t, isNewTxn)
	require.Nil(t, row)

	// Case 2: iterator with 2 txns that has 2 rows
	row = &common.RowChangedEvent{
		PhysicalTableID: 1,
		StartTs:         1,
		CommitTs:        5,
	}
	txnEvent1 := &common.TxnEvent{
		ClusterID: 1,
		StartTs:   1,
		Rows:      []*common.RowChangedEvent{row, row},
	}
	txnEvent2 := &common.TxnEvent{
		ClusterID: 1,
		StartTs:   1,
		Rows:      []*common.RowChangedEvent{row, row},
	}

	iter.events = append(iter.events, txnEvent1)
	iter.events = append(iter.events, txnEvent2)

	// txn-1, row-1
	row, isNewTxn, err = iter.Next()
	require.Nil(t, err)
	require.True(t, isNewTxn)
	require.NotNil(t, row)
	// txn-1, row-2
	row, isNewTxn, err = iter.Next()
	require.Nil(t, err)
	require.False(t, isNewTxn)
	require.NotNil(t, row)

	// txn-2, row1
	row, isNewTxn, err = iter.Next()
	require.Nil(t, err)
	require.True(t, isNewTxn)
	require.NotNil(t, row)
	// txn2, row2
	row, isNewTxn, err = iter.Next()
	require.Nil(t, err)
	require.False(t, isNewTxn)
	require.NotNil(t, row)
}

func TestEventServiceBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStore := newMockEventStore()
	mc := &mockMessageCenter{
		messageCh: make(chan *messaging.TargetMessage, 100),
	}

	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(appcontext.EventStore, mockStore)
	es := NewEventService()
	esImpl := es.(*eventService)
	go func() {
		err := es.Run(ctx)
		if err != nil {
			t.Errorf("EventService.Run() error = %v", err)
		}
	}()

	acceptorInfo := newMockAcceptorInfo(common.NewDispatcherID(), 1)
	// register acceptor
	esImpl.acceptorInfoCh <- acceptorInfo
	// wait for eventService to process the acceptorInfo
	time.Sleep(time.Second * 2)

	require.Equal(t, 1, len(esImpl.brokers))
	require.NotNil(t, esImpl.brokers[acceptorInfo.GetClusterID()])

	// add events to logpuller
	txnEvent := &common.TxnEvent{
		DispatcherID: acceptorInfo.GetID(),
		Span:         acceptorInfo.span,
		StartTs:      1,
		CommitTs:     5,
		Rows: []*common.RowChangedEvent{
			{
				PhysicalTableID: 1,
				StartTs:         1,
				CommitTs:        5,
			},
			{
				PhysicalTableID: 1,
				StartTs:         1,
				CommitTs:        5,
			},
		},
	}

	sourceSpanStat, ok := mockStore.spans[acceptorInfo.span.TableID]
	require.True(t, ok)

	sourceSpanStat.update([]*common.TxnEvent{txnEvent}, txnEvent.CommitTs)

	expectedEvent := &common.TxnEvent{
		DispatcherID: acceptorInfo.GetID(),
		StartTs:      1,
		CommitTs:     5,
		Rows: []*common.RowChangedEvent{
			{
				PhysicalTableID: 1,
				StartTs:         1,
				CommitTs:        5,
			},
			{
				PhysicalTableID: 1,
				StartTs:         1,
				CommitTs:        5,
			},
		},
	}

	// receive events from msg center
	for {
		msg := <-mc.messageCh
		txn := msg.Message[0].(*common.TxnEvent)
		if len(txn.Rows) == 0 {
			log.Info("received watermark", zap.Uint64("ts", txn.ResolvedTs))
			continue
		}
		require.NotNil(t, msg)
		require.Equal(t, acceptorInfo.GetTopic(), msg.Topic)
		require.Equal(t, expectedEvent, msg.Message[0].(*common.TxnEvent))
		return
	}
}

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

// The test mainly focus on the communication between dispatcher and event service.
// When dispatcher created and register in event service, event service need to send events to dispatcher.
func TestDispatcherCommunicateWithEventService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverId := messaging.NewServerId()
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(ctx, serverId, 1, config.NewDefaultMessageCenterConfig()))
	appcontext.SetService(appcontext.EventCollector, eventcollector.NewEventCollector(100*1024*1024*1024, serverId)) // 100GB for demo

	mockStore := newMockEventStore()
	appcontext.SetService(appcontext.EventStore, mockStore)
	eventService := NewEventService()
	go func() {
		err := eventService.Run(ctx)
		if err != nil {
			t.Errorf("EventService.Run() error = %v", err)
		}
	}()

	db, _ := newTestMockDB(t)
	defer db.Close()

	mysqlSink := sink.NewMysqlSink(model.DefaultChangeFeedID("test1"), 8, writer.NewMysqlConfig(), db)
	tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: 1, StartKey: nil, EndKey: nil}}
	startTs := uint64(1)
	id := common.NewDispatcherID()
	tableEventDispatcher := dispatcher.NewDispatcher(id, tableSpan, mysqlSink, startTs, nil, nil, 0)
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RegisterDispatcher(
		eventcollector.RegisterInfo{
			Dispatcher:   tableEventDispatcher,
			StartTs:      startTs,
			FilterConfig: nil,
		},
	)

	time.Sleep(1 * time.Second)
	// add events to logpuller
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

	sourceSpanStat, ok := mockStore.spans[tableSpan.TableID]
	require.True(t, ok)

	sourceSpanStat.update([]*common.TxnEvent{txnEvent}, txnEvent.CommitTs)

	// <-tableEventDispatcher.GetEventChan()
}
