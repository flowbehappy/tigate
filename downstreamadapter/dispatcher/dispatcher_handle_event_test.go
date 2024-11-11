package dispatcher

import (
	"sync/atomic"
	"testing"

	psink "github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/types"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	sinkutil "github.com/pingcap/ticdc/pkg/sink/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"

	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// TODO: Merge this file into dispatcher_test.go after refactoring the dispatcher test.

type mockSink struct {
	blockEvents  []pevent.BlockEvent
	dmls         []*pevent.DMLEvent
	checkpointTs uint64
}

func (s *mockSink) AddDMLEvent(event *pevent.DMLEvent, tableProgress *types.TableProgress) {
	tableProgress.Add(event)
	s.dmls = append(s.dmls, event)
}

func (s *mockSink) AddBlockEvent(event pevent.BlockEvent, tableProgress *types.TableProgress) {
	tableProgress.Add(event)
	s.blockEvents = append(s.blockEvents, event)
}

func (s *mockSink) PassBlockEvent(event pevent.BlockEvent, tableProgress *types.TableProgress) {
}

func (s *mockSink) AddCheckpointTs(ts uint64) {
}

func (s *mockSink) SetTableSchemaStore(tableSchemaStore *sinkutil.TableSchemaStore) {
}

func (s *mockSink) Close(bool) error {
	return nil
}

func (s *mockSink) SinkType() psink.SinkType {
	return psink.MysqlSinkType
}

func (s *mockSink) CheckStartTs(startTs int64, checkpointTs uint64) (int64, error) {
	return startTs, nil
}

func (s *mockSink) flushDMLs() {
	for _, dml := range s.dmls {
		for _, postTxnFlushed := range dml.PostTxnFlushed {
			postTxnFlushed()
		}
		if dml.CommitTs > s.checkpointTs {
			s.checkpointTs = dml.CommitTs
		}
	}
	s.dmls = make([]*pevent.DMLEvent, 0)
}

func (s *mockSink) flushBlockEvents() {
	for _, blockEvent := range s.blockEvents {
		blockEvent.PostFlush()
		if blockEvent.GetCommitTs() > s.checkpointTs {
			s.checkpointTs = blockEvent.GetCommitTs()
		}
	}
	s.blockEvents = make([]pevent.BlockEvent, 0)
}

func newMockSink() *mockSink {
	return &mockSink{
		blockEvents: make([]pevent.BlockEvent, 0),
		dmls:        make([]*pevent.DMLEvent, 0),
	}
}

func TestDispatcherHandleEvents(t *testing.T) {
	helper := pevent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)

	tableInfo := dmlEvent.TableInfo

	dispatcherID := common.NewDispatcherID()
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
	}

	sink := newMockSink()

	dispatcherActionChan := make(chan common.DispatcherAction, 128)
	blockStatusesChan := make(chan *heartbeatpb.TableSpanBlockStatus, 128)
	schemaIDToDispatchers := NewSchemaIDToDispatchers()
	startTs := common.Ts(0)
	dispatcher := NewDispatcher(
		common.NewChangefeedID(),
		dispatcherID,
		tableSpan,
		sink,
		startTs, // startTs
		dispatcherActionChan,
		blockStatusesChan,
		nil,
		1, // schemaID
		schemaIDToDispatchers,
		nil,
		100000, // memoryQuota
		nil,
	)
	// 1. Dispatcher is not ready, handle dml event, it will return immediately
	dispatcherEvent := NewDispatcherEvent(dmlEvent)
	dispatcher.HandleEvents([]DispatcherEvent{dispatcherEvent})
	require.Equal(t, 0, len(sink.dmls))
	require.Equal(t, 0, len(sink.blockEvents))
	require.False(t, dispatcher.isReady.Load())

	var seq atomic.Uint64
	seq.Store(0)
	// 2. Dispatcher is not ready, handle handshake event, it will become ready
	handshakeEvent := pevent.NewHandshakeEvent(
		dispatcherID,
		startTs,    // startTs
		seq.Add(1), // seq
		tableInfo,
	)
	dispatcherEvent = NewDispatcherEvent(handshakeEvent)
	dispatcher.HandleEvents([]DispatcherEvent{dispatcherEvent})
	require.True(t, dispatcher.isReady.Load())

	// 3. Dispatcher is ready, handle local dml event, it will be sent to sink
	dmlEvent.Seq = seq.Add(1)
	dispatcherEvent = NewDispatcherEvent(dmlEvent)
	dispatcher.HandleEvents([]DispatcherEvent{dispatcherEvent})
	require.Equal(t, 1, len(sink.dmls))
	require.Equal(t, 0, len(sink.blockEvents))
	require.Equal(t, dmlEvent, sink.dmls[0])
	// Flush the dml events to wake the dynamic stream path of the dispatcher
	sink.flushDMLs()
	require.Equal(t, dispatcher.GetCheckpointTs(), dmlEvent.CommitTs-1)

	// 4. Dispatcher is ready, resolve event will be handled
	resolvedEvent := pevent.ResolvedEvent{
		Version:      pevent.ResolvedEventVersion,
		ResolvedTs:   dmlEvent.CommitTs + 1,
		DispatcherID: dispatcherID,
	}
	dispatcherEvent = NewDispatcherEvent(resolvedEvent)
	dispatcher.HandleEvents([]DispatcherEvent{dispatcherEvent})
	require.Equal(t, dispatcher.GetResolvedTs(), resolvedEvent.ResolvedTs)

	// 5. Dispatcher is ready, handle ddl event, it will be set as a blockingEvent
	ddlEvent := &pevent.DDLEvent{
		Version:      pevent.DDLEventVersion,
		DispatcherID: dispatcherID,
		Seq:          seq.Add(1),
		Type:         byte(timodel.ActionCreateTable),
		SchemaID:     tableInfo.SchemaID,
		TableID:      tableInfo.ID,
		SchemaName:   "test",
		TableName:    "t",
		Query:        "create table t(id int primary key, v int)",
		TableInfo:    tableInfo,
		FinishedTs:   resolvedEvent.ResolvedTs + 2,
		BlockedTables: &pevent.InfluencedTables{
			InfluenceType: pevent.InfluenceTypeNormal,
			TableIDs:      []int64{tableInfo.ID},
		},
		NeedAddedTables: []pevent.Table{
			{
				SchemaID: tableInfo.SchemaID,
				TableID:  tableInfo.ID,
			},
		},
	}
	dispatcherEvent = NewDispatcherEvent(ddlEvent)
	dispatcher.HandleEvents([]DispatcherEvent{dispatcherEvent})
	require.Equal(t, ddlEvent, dispatcher.blockStatus.blockPendingEvent)
	require.Equal(t, 1, len(sink.blockEvents))

	// 6. Dispatcher is ready, handle action event
	actionEvent := &heartbeatpb.DispatcherStatus{
		InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			DispatcherIDs: []*heartbeatpb.DispatcherID{dispatcherID.ToPB()},
		},
		Action: &heartbeatpb.DispatcherAction{
			Action:      heartbeatpb.Action_Write,
			CommitTs:    ddlEvent.FinishedTs,
			IsSyncPoint: false,
		},
	}

	dispatcher.HandleDispatcherStatus(actionEvent)
	sink.flushBlockEvents()
	require.Equal(t, dispatcher.GetCheckpointTs(), ddlEvent.FinishedTs-1)
}
