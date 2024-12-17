package dispatcher

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	sinkutil "github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tiflow/pkg/spanz"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// TODO: Merge this file into dispatcher_test.go after refactoring the dispatcher test.

type mockSink struct {
	dmls     []*commonEvent.DMLEvent
	isNormal bool
	sinkType common.SinkType
}

func (s *mockSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmls = append(s.dmls, event)
}

func (s *mockSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	event.PostFlush()
	return nil
}

func (s *mockSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *mockSink) AddCheckpointTs(ts uint64) {
}

func (s *mockSink) SetTableSchemaStore(tableSchemaStore *sinkutil.TableSchemaStore) {
}

func (s *mockSink) Close(bool) error {
	return nil
}

func (s *mockSink) SinkType() common.SinkType {
	return s.sinkType
}

func (s *mockSink) IsNormal() bool {
	return s.isNormal
}

func (s *mockSink) flushDMLs() {
	for _, dml := range s.dmls {
		dml.PostFlush()
	}
	s.dmls = make([]*commonEvent.DMLEvent, 0)
}

func newMockSink(sinkType common.SinkType) *mockSink {
	return &mockSink{
		dmls:     make([]*commonEvent.DMLEvent, 0),
		isNormal: true,
		sinkType: sinkType,
	}
}

func getCompleteTableSpanWithTableID(tableID int64) *heartbeatpb.TableSpan {
	tableSpan := &heartbeatpb.TableSpan{
		TableID: tableID,
	}
	startKey, endKey := spanz.GetTableRange(tableSpan.TableID)
	tableSpan.StartKey = spanz.ToComparableKey(startKey)
	tableSpan.EndKey = spanz.ToComparableKey(endKey)
	return tableSpan
}

func getCompleteTableSpan() *heartbeatpb.TableSpan {
	return getCompleteTableSpanWithTableID(1)
}

func getUncompleteTableSpan() *heartbeatpb.TableSpan {
	return &heartbeatpb.TableSpan{
		TableID: 1,
	}
}

func newDispatcherForTest(sink sink.Sink, tableSpan *heartbeatpb.TableSpan) *Dispatcher {
	return NewDispatcher(
		common.NewChangefeedID(),
		common.NewDispatcherID(),
		tableSpan,
		sink,
		common.Ts(0), // startTs
		make(chan *heartbeatpb.TableSpanBlockStatus, 128),
		1, // schemaID
		NewSchemaIDToDispatchers(),
		&syncpoint.SyncPointConfig{
			SyncPointInterval:  time.Duration(5 * time.Second),
			SyncPointRetention: time.Duration(10 * time.Minute),
		}, // syncPointConfig
		nil,          //filterConfig
		common.Ts(0), //pdTs
		make(chan error, 1),
	)
}

var count = 0

func callback() {
	count++
}

// test different events can be correctly handled by the dispatcher
func TestDispatcherHandleEvents(t *testing.T) {
	count = 0
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	dmlEvent.CommitTs = 2
	dmlEvent.Length = 1

	tableInfo := dmlEvent.TableInfo

	sink := newMockSink(common.MysqlSinkType)
	tableSpan := getCompleteTableSpan()
	dispatcher := newDispatcherForTest(sink, tableSpan)
	dispatcher.SetInitialTableInfo(tableInfo)
	require.Equal(t, uint64(0), dispatcher.GetCheckpointTs())
	require.Equal(t, uint64(0), dispatcher.GetResolvedTs())
	tableProgress := dispatcher.tableProgress

	checkpointTs, isEmpty := tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(0), checkpointTs)

	// ===== dml event =====
	nodeID := node.NewID()
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, callback)
	require.Equal(t, true, block)
	require.Equal(t, 1, len(sink.dmls))

	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, false, isEmpty)
	require.Equal(t, uint64(1), checkpointTs)
	require.Equal(t, 0, count)

	// flush
	sink.flushDMLs()
	require.Equal(t, 0, len(sink.dmls))
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(1), checkpointTs)
	require.Equal(t, 1, count)

	// ===== ddl event =====
	// 1. non-block ddl event, and don't need to communicate with maintainer
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo: tableInfo,
	}

	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.dmls))
	// no pending event
	require.Nil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_NONE)

	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(1), checkpointTs)

	require.Equal(t, 2, count)

	// 2.1 non-block ddl event, but need to communicate with maintainer(drop table)
	ddlEvent21 := &commonEvent.DDLEvent{
		FinishedTs: 3,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedDroppedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{1},
		},
		TableInfo: tableInfo,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent21)}, callback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.dmls))
	// no pending event
	require.Nil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_NONE)

	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(2), checkpointTs)

	require.Equal(t, 3, count)

	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	// receive the ack info
	dispatcherStatus := &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent21.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())

	// 2.2 block ddl event, but need to communicate with maintainer(add table)
	ddlEvent2 := &commonEvent.DDLEvent{
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
			},
		},
		TableInfo: tableInfo,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent2)}, callback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.dmls))
	// no pending event
	require.Nil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_NONE)
	// but block table progress until ack
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, false, isEmpty)
	require.Equal(t, uint64(3), checkpointTs)

	require.Equal(t, 4, count)

	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	// receive the ack info
	// ack for previous ddl event, not cancel this task
	dispatcherStatusPrev := &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatusPrev)
	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent2.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())

	// clear the event in tableProgress when receive the ack
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(3), checkpointTs)

	// 3. block ddl event
	ddlEvent3 := &commonEvent.DDLEvent{
		FinishedTs: 5,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0, 1},
		},
		TableInfo: tableInfo,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent3)}, callback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.dmls))
	// pending event
	require.NotNil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_WAITING)

	// the ddl is not available for write to sink
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(3), checkpointTs)

	require.Equal(t, 4, count)

	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	// receive the ack info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent3.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())
	// pending event
	require.NotNil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_WAITING)

	// the ddl is still not available for write to sink
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(3), checkpointTs)

	// receive the action info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Action: &heartbeatpb.DispatcherAction{
			Action:      heartbeatpb.Action_Write,
			CommitTs:    ddlEvent3.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(4), checkpointTs)

	// clear pending event(TODO:add a check for the middle status)
	require.Nil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_NONE)

	require.Equal(t, 5, count)

	// ===== sync point event =====

	syncPointEvent := &commonEvent.SyncPointEvent{
		CommitTs: 6,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, syncPointEvent)}, callback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.dmls))
	// pending event
	require.NotNil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_WAITING)

	// not available for write to sink
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(4), checkpointTs)

	// receive the ack info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    syncPointEvent.CommitTs,
			IsSyncPoint: true,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())
	// pending event
	require.NotNil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_WAITING)

	// receive the action info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Action: &heartbeatpb.DispatcherAction{
			Action:      heartbeatpb.Action_Pass,
			CommitTs:    syncPointEvent.CommitTs,
			IsSyncPoint: true,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(5), checkpointTs)

	require.Equal(t, 6, count)

	// ===== resolved event =====
	checkpointTs = dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(5), checkpointTs)
	resolvedEvent := commonEvent.ResolvedEvent{
		ResolvedTs: 7,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, resolvedEvent)}, callback)
	require.Equal(t, false, block)
	require.Equal(t, 0, len(sink.dmls))
	require.Equal(t, uint64(7), dispatcher.GetResolvedTs())
	checkpointTs = dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(7), checkpointTs)
}

// test uncompelete table span can correctly handle the ddl events
func TestUncompeleteTableSpanDispatcherHandleEvents(t *testing.T) {
	count = 0
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	sink := newMockSink(common.MysqlSinkType)
	tableSpan := getUncompleteTableSpan()
	dispatcher := newDispatcherForTest(sink, tableSpan)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	tableInfo := dmlEvent.TableInfo

	// basic ddl event
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo: tableInfo,
	}

	nodeID := node.NewID()
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	// pending event
	require.NotNil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_WAITING)
	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	checkpointTs := dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(0), checkpointTs)
	require.Equal(t, 0, count)

	// receive the ack info
	dispatcherStatus := &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())
	// pending event
	require.NotNil(t, dispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, dispatcher.blockEventStatus.blockStage, heartbeatpb.BlockStage_WAITING)

	// the ddl is still not available for write to sink
	checkpointTs = dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(0), checkpointTs)
	require.Equal(t, 0, count)

	// receive the action info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Action: &heartbeatpb.DispatcherAction{
			Action:      heartbeatpb.Action_Write,
			CommitTs:    ddlEvent.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	checkpointTs = dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(1), checkpointTs)
	require.Equal(t, 1, count)

}

func TestTableTriggerEventDispatcherInMysql(t *testing.T) {
	count = 0

	ddlTableSpan := heartbeatpb.DDLSpan
	sink := newMockSink(common.MysqlSinkType)
	tableTriggerEventDispatcher := newDispatcherForTest(sink, ddlTableSpan)
	require.Nil(t, tableTriggerEventDispatcher.tableSchemaStore)

	err := tableTriggerEventDispatcher.InitalizeTableSchemaStore([]*heartbeatpb.SchemaInfo{})
	require.NoError(t, err)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	tableInfo := dmlEvent.TableInfo

	// basic ddl event(non-block)
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo: tableInfo,
	}

	nodeID := node.NewID()
	block := tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	// no pending event
	require.Nil(t, tableTriggerEventDispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, 1, count)

	tableIds := tableTriggerEventDispatcher.tableSchemaStore.GetAllTableIds()
	require.Equal(t, 1, len(tableIds))
	require.Equal(t, int64(0), tableIds[0])

	// ddl influences tableSchemaStore
	ddlEvent = &commonEvent.DDLEvent{
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
			},
		},
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test",
					TableName:  "t1",
				},
			},
		},
		TableInfo: tableInfo,
	}

	block = tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	// no pending event
	require.Nil(t, tableTriggerEventDispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, 2, count)

	tableIds = tableTriggerEventDispatcher.tableSchemaStore.GetAllTableIds()
	require.Equal(t, int(2), len(tableIds))
	require.Equal(t, int64(1), tableIds[0])
	require.Equal(t, int64(0), tableIds[1])
}

func TestTableTriggerEventDispatcherInKafka(t *testing.T) {
	count = 0

	ddlTableSpan := heartbeatpb.DDLSpan
	sink := newMockSink(common.KafkaSinkType)
	tableTriggerEventDispatcher := newDispatcherForTest(sink, ddlTableSpan)
	require.Nil(t, tableTriggerEventDispatcher.tableSchemaStore)

	err := tableTriggerEventDispatcher.InitalizeTableSchemaStore([]*heartbeatpb.SchemaInfo{})
	require.NoError(t, err)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	tableInfo := dmlEvent.TableInfo

	// basic ddl event(non-block)
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo: tableInfo,
	}

	nodeID := node.NewID()
	block := tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	// no pending event
	require.Nil(t, tableTriggerEventDispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, 1, count)

	tableNames := tableTriggerEventDispatcher.tableSchemaStore.GetAllTableNames(2)
	require.Equal(t, int(0), len(tableNames))

	// ddl influences tableSchemaStore
	ddlEvent = &commonEvent.DDLEvent{
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
			},
		},
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test",
					TableName:  "t1",
				},
			},
		},
		TableInfo: tableInfo,
	}

	block = tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	// no pending event
	require.Nil(t, tableTriggerEventDispatcher.blockEventStatus.blockPendingEvent)
	require.Equal(t, 2, count)

	tableNames = tableTriggerEventDispatcher.tableSchemaStore.GetAllTableNames(3)
	require.Equal(t, int(0), len(tableNames))
	tableNames = tableTriggerEventDispatcher.tableSchemaStore.GetAllTableNames(4)
	require.Equal(t, int(1), len(tableNames))
	require.Equal(t, commonEvent.SchemaTableName{SchemaName: "test", TableName: "t1"}, *tableNames[0])
}

// ensure the dispatcher will be closed when no dml events is in sink
func TestDispatcherClose(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	dmlEvent.CommitTs = 2
	dmlEvent.Length = 1

	tableInfo := dmlEvent.TableInfo

	{
		sink := newMockSink(common.MysqlSinkType)
		dispatcher := newDispatcherForTest(sink, getCompleteTableSpan())

		dispatcher.SetInitialTableInfo(tableInfo)

		// ===== dml event =====
		nodeID := node.NewID()
		dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, callback)

		_, ok := dispatcher.TryClose()
		require.Equal(t, false, ok)

		// flush
		sink.flushDMLs()

		watermark, ok := dispatcher.TryClose()
		require.Equal(t, true, ok)
		require.Equal(t, uint64(1), watermark.CheckpointTs)
		require.Equal(t, uint64(0), watermark.ResolvedTs)
	}

	// test sink is not normal
	{
		sink := newMockSink(common.MysqlSinkType)
		dispatcher := newDispatcherForTest(sink, getCompleteTableSpan())

		dispatcher.SetInitialTableInfo(tableInfo)

		// ===== dml event =====
		nodeID := node.NewID()
		dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, callback)

		_, ok := dispatcher.TryClose()
		require.Equal(t, false, ok)

		sink.isNormal = false

		watermark, ok := dispatcher.TryClose()
		require.Equal(t, true, ok)
		require.Equal(t, uint64(1), watermark.CheckpointTs)
		require.Equal(t, uint64(0), watermark.ResolvedTs)
	}
}
