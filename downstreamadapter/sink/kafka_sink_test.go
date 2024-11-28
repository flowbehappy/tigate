package sink

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/types"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// Test callback and tableProgress works as expected after AddDMLEvent
func TestKafkaSinkBasicFunctionality(t *testing.T) {
	sink, dmlProducer, ddlProducer, err := newKafkaSinkForTest()
	require.NoError(t, err)
	tableProgress := types.NewTableProgress()
	ts, isEmpty := tableProgress.GetCheckpointTs()
	require.NotEqual(t, ts, 0)
	require.Equal(t, isEmpty, true)

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

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count++ },
	}
	dmlEvent.CommitTs = 2

	err = sink.WriteBlockEvent(ddlEvent, tableProgress)
	require.NoError(t, err)

	sink.AddDMLEvent(dmlEvent, tableProgress)
	time.Sleep(1 * time.Second)

	sink.PassBlockEvent(ddlEvent2, tableProgress)

	require.Len(t, dmlProducer.(*producer.MockProducer).GetAllEvents(), 2)
	require.Len(t, ddlProducer.(*producer.MockProducer).GetAllEvents(), 1)

	ts, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, ts, uint64(3))
	require.Equal(t, isEmpty, true)

	require.Equal(t, count, 3)
}
