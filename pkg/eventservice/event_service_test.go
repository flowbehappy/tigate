package eventservice

import (
	"context"
	"database/sql"
	"github.com/flowbehappy/tigate/pkg/node"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/mounter"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func initEventService(
	ctx context.Context, t *testing.T,
	mc messaging.MessageCenter, mockStore eventstore.EventStore,
) *eventService {
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(appcontext.EventStore, mockStore)
	appcontext.SetService(appcontext.SchemaStore, newMockSchemaStore())
	es := NewEventService()
	esImpl := es.(*eventService)
	go func() {
		err := esImpl.Run(ctx)
		if err != nil {
			t.Errorf("EventService.Run() error = %v", err)
		}
	}()
	return esImpl
}

func TestEventServiceBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStore := newMockEventStore()
	mc := &mockMessageCenter{
		messageCh: make(chan *messaging.TargetMessage, 100),
	}
	esImpl := initEventService(ctx, t, mc, mockStore)
	defer esImpl.Close(ctx)

	acceptorInfo := newMockAcceptorInfo(common.NewDispatcherID(), 1)
	// register acceptor
	esImpl.acceptorInfoCh <- acceptorInfo
	// wait for eventService to process the acceptorInfo
	time.Sleep(time.Second * 2)

	require.Equal(t, 1, len(esImpl.brokers))
	require.NotNil(t, esImpl.brokers[acceptorInfo.GetClusterID()])

	// add events to logpuller
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper, t, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
		`insert into test.t(id,c) values (2, "c2")`,
	}...)
	require.NotNil(t, kvEvents)

	sourceSpanStat, ok := mockStore.spans[acceptorInfo.span.TableID]
	require.True(t, ok)

	sourceSpanStat.update(kvEvents[0].CRTs, kvEvents...)
	schemastore := esImpl.schemaStore.(*mockSchemaStore)
	schemastore.AppendDDLEvent(acceptorInfo.span.TableID, ddlEvent)

	// receive events from msg center
	msgCnt := 0
	for {
		msg := <-mc.messageCh
		for _, m := range msg.Message {
			msgCnt++
			switch e := m.(type) {
			case *common.DMLEvent:
				require.NotNil(t, msg)
				require.Equal(t, "event-collector", msg.Topic)
				require.Equal(t, len(kvEvents), e.Len())
				require.Equal(t, kvEvents[0].CRTs, e.CommitTs)
			case *common.DDLEvent:
				require.NotNil(t, msg)
				require.Equal(t, "event-collector", msg.Topic)
				require.Equal(t, ddlEvent.CommitTS, e.CommitTS)
			case *common.BatchResolvedEvent:
				require.NotNil(t, msg)
				log.Info("received watermark", zap.Uint64("ts", e.Events[0].ResolvedTs))
			}
		}
		if msgCnt == 3 {
			break
		}
	}
}

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

var _ messaging.MessageCenter = &mockMessageCenter{}

// mockMessageCenter is a mock implementation of the MessageCenter interface
type mockMessageCenter struct {
	messageCh chan *messaging.TargetMessage
}

func (m *mockMessageCenter) OnNodeChanges(nodeInfos map[string]*node.Info) {

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

var _ eventstore.EventStore = &mockEventStore{}

// mockEventStore is a mock implementation of the EventStore interface
type mockEventStore struct {
	spans map[common.TableID]*mockSpanStats
}

func newMockEventStore() *mockEventStore {
	return &mockEventStore{
		spans: make(map[common.TableID]*mockSpanStats),
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
	span *heartbeatpb.TableSpan,
	startTS common.Ts,
	observer eventstore.EventObserver,
	notifier eventstore.WatermarkNotifier,
) error {
	log.Info("subscribe table span", zap.Any("span", span), zap.Uint64("startTs", uint64(startTS)))
	m.spans[span.TableID] = &mockSpanStats{
		startTs:       uint64(startTS),
		watermark:     uint64(startTS),
		onUpdate:      notifier,
		onEvent:       observer,
		pendingEvents: make([]*common.RawKVEntry, 0),
	}
	return nil
}

func (m *mockEventStore) UpdateDispatcherSendTS(dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan, gcTS uint64) error {
	return nil
}

func (m *mockEventStore) UnregisterDispatcher(dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan) error {
	return nil
}

func (m *mockEventStore) GetIterator(dispatcherID common.DispatcherID, dataRange *common.DataRange) (eventstore.EventIterator, error) {
	iter := &mockEventIterator{
		events: make([]*common.RawKVEntry, 0),
	}

	for _, e := range m.spans[dataRange.Span.TableID].pendingEvents {
		if e.CRTs > dataRange.StartTs && e.CRTs <= dataRange.EndTs {
			iter.events = append(iter.events, e)
		}
	}

	return iter, nil
}

type mockEventIterator struct {
	events       []*common.RawKVEntry
	prevStartTS  uint64
	prevCommitTS uint64
	rowCount     int
}

func (iter *mockEventIterator) Next() (*common.RawKVEntry, bool, error) {
	if len(iter.events) == 0 {
		return nil, false, nil
	}

	row := iter.events[0]
	iter.events = iter.events[1:]
	isNewTxn := false
	if iter.prevCommitTS == 0 || row.StartTs != iter.prevStartTS || row.CRTs != iter.prevCommitTS {
		isNewTxn = true
	}
	iter.prevStartTS = row.StartTs
	iter.prevCommitTS = row.CRTs
	iter.rowCount++
	return row, isNewTxn, nil
}

func (m *mockEventIterator) Close() (int64, error) {
	return 0, nil
}

// This test is to test the mockEventIterator works as expected.
func TestMockEventIterator(t *testing.T) {
	iter := &mockEventIterator{
		events: make([]*common.RawKVEntry, 0),
	}

	// Case 1: empty iterator
	row, isNewTxn, err := iter.Next()
	require.Nil(t, err)
	require.False(t, isNewTxn)
	require.Nil(t, row)

	// Case 2: iterator with 2 txns that has 2 rows
	row1 := &common.RawKVEntry{
		StartTs: 1,
		CRTs:    5,
	}
	row2 := &common.RawKVEntry{
		StartTs: 2,
		CRTs:    5,
	}

	iter.events = append(iter.events, row1, row1)
	iter.events = append(iter.events, row2, row2)

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

var _ schemastore.SchemaStore = &mockSchemaStore{}

type mockSchemaStore struct {
	schemastore.SchemaStore
	DDLEvents map[common.TableID][]common.DDLEvent
	TableInfo map[common.TableID][]*common.TableInfo

	dispatchers map[common.DispatcherID]common.TableID
	resolvedTs  uint64
}

func newMockSchemaStore() *mockSchemaStore {
	return &mockSchemaStore{
		DDLEvents:   make(map[common.TableID][]common.DDLEvent),
		TableInfo:   make(map[common.TableID][]*common.TableInfo),
		dispatchers: make(map[common.DispatcherID]common.TableID),
		resolvedTs:  0,
	}
}

func (m *mockSchemaStore) AppendDDLEvent(id common.TableID, ddls ...common.DDLEvent) {
	for _, ddl := range ddls {
		m.DDLEvents[id] = append(m.DDLEvents[id], ddl)
		job := ddl.Job
		m.TableInfo[id] = append(m.TableInfo[id], common.WrapTableInfo(
			job.SchemaID,
			job.SchemaName,
			job.BinlogInfo.TableInfo.UpdateTS,
			job.BinlogInfo.TableInfo))
	}
}

func (m *mockSchemaStore) GetTableInfo(tableID common.TableID, ts common.Ts) (*common.TableInfo, error) {
	infos := m.TableInfo[tableID]
	idx := sort.Search(len(infos), func(i int) bool {
		return infos[i].UpdateTS > uint64(ts)
	})
	return infos[idx-1], nil
}

func (m *mockSchemaStore) RegisterDispatcher(
	dispatcherID common.DispatcherID, span *heartbeatpb.TableSpan,
	startTS common.Ts, filter filter.Filter,
) error {
	m.dispatchers[dispatcherID] = common.TableID(span.TableID)
	return nil
}

func (m *mockSchemaStore) UnregisterDispatcher(dispatcherID common.DispatcherID) error {
	delete(m.dispatchers, dispatcherID)
	return nil
}

// GetNextDDLEvents returns the next ddl event which finishedTs is within the range (start, end]
func (m *mockSchemaStore) GetNextDDLEvents(id common.TableID, start, end common.Ts) ([]common.DDLEvent, common.Ts, error) {
	events := m.DDLEvents[id]
	if len(events) == 0 {
		return nil, end, nil
	}
	l := sort.Search(len(events), func(i int) bool {
		return events[i].CommitTS > start
	})
	if l == len(events) {
		return nil, end, nil
	}
	r := sort.Search(len(events), func(i int) bool {
		return events[i].CommitTS > end
	})
	m.DDLEvents[id] = events[r:]
	return events[l:r], end, nil
}

func (m *mockSchemaStore) GetNextTableTriggerEvents(f filter.Filter, start common.Ts, limit int) ([]common.DDLEvent, common.Ts, error) {
	return nil, m.resolvedTs, nil
}
