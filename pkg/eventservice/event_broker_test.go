package eventservice

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/mounter"
	"github.com/pingcap/log"
	tconfig "github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewDispatcherStat(t *testing.T) {
	startTs := uint64(123)

	info := &mockDispatcherInfo{
		id:        common.NewDispatcherID(),
		clusterID: 1,
		startTs:   startTs,
	}

	spanSubscription := newSpanSubscription(info.GetTableSpan(), startTs)
	stat := newDispatcherStat(startTs, info, spanSubscription, nil)
	require.Equal(t, info, stat.info)
	require.Equal(t, startTs, stat.watermark.Load())
	require.NotNil(t, stat.spanSubscription)
	require.Equal(t, startTs, stat.spanSubscription.watermark.Load())
	require.NotEmpty(t, stat.workerIndex)
	require.Nil(t, stat.filter)
}

func TestDispatcherStatUpdateWatermark(t *testing.T) {
	startTs := uint64(123)
	wg := &sync.WaitGroup{}
	info := &mockDispatcherInfo{
		id:        common.NewDispatcherID(),
		clusterID: 1,
		startTs:   startTs,
	}

	spanSubscription := newSpanSubscription(info.GetTableSpan(), startTs)
	stat := newDispatcherStat(startTs, info, spanSubscription, nil)

	sendNewEvent := func(maxTs uint64) {
		g := &sync.WaitGroup{}
		for i := 0; i < 64; i++ {
			ts := rand.Uint64() % maxTs
			if i == 10 {
				ts = maxTs
			}
			g.Add(1)
			go func() {
				defer g.Done()
				stat.spanSubscription.onNewEvent(&common.RawKVEntry{
					CRTs: ts,
				})
			}()
		}
		g.Wait()
	}

	// Case 1: no new events, only watermark change
	stat.spanSubscription.onSubscriptionWatermark(456)
	require.Equal(t, uint64(456), stat.spanSubscription.watermark.Load())
	log.Info("pass TestDispatcherStatUpdateWatermark case 1")

	// Case 2: new events, and watermark increase
	sendNewEvent(startTs)
	stat.spanSubscription.onSubscriptionWatermark(789)
	require.Equal(t, uint64(789), stat.spanSubscription.watermark.Load())
	require.Equal(t, startTs, stat.spanSubscription.maxEventCommitTs.Load())
	log.Info("pass TestDispatcherStatUpdateWatermark case 2")

	// Case 3: new events, and watermark decrease
	// watermark should not decrease and no notification
	sendNewEvent(360)
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		stat.spanSubscription.onSubscriptionWatermark(456)
		close(done)
	}()
	<-done
	require.Equal(t, uint64(789), stat.spanSubscription.watermark.Load())
	require.Equal(t, uint64(360), stat.spanSubscription.maxEventCommitTs.Load())
	log.Info("pass TestDispatcherStatUpdateWatermark case 3")

	wg.Wait()
}

func newTableSpan(tableID int64, start, end string) *heartbeatpb.TableSpan {
	return &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: []byte(start),
		EndKey:   []byte(end),
	}
}

func TestResolvedTsCache(t *testing.T) {
	rc := newResolvedTsCache(10)
	require.Equal(t, 0, rc.len)
	require.Equal(t, 10, len(rc.cache))
	require.Equal(t, 10, rc.limit)

	// Case 1: insert a new resolved ts
	rc.add(commonEvent.ResolvedEvent{
		DispatcherID: common.NewDispatcherID(),
		ResolvedTs:   100,
	})
	require.Equal(t, 1, rc.len)
	require.Equal(t, uint64(100), rc.cache[0].ResolvedTs)
	require.False(t, rc.isFull())

	// Case 2: add more resolved ts until full
	i := 1
	for !rc.isFull() {
		rc.add(commonEvent.ResolvedEvent{
			DispatcherID: common.NewDispatcherID(),
			ResolvedTs:   uint64(100 + i),
		})
		i++
	}
	require.Equal(t, 10, rc.len)
	require.Equal(t, uint64(100), rc.cache[0].ResolvedTs)
	require.Equal(t, uint64(109), rc.cache[9].ResolvedTs)
	require.True(t, rc.isFull())

	// Case 3: get all resolved ts
	res := rc.getAll()
	require.Equal(t, 10, len(res))
	require.Equal(t, 0, rc.len)
	require.Equal(t, uint64(100), res[0].ResolvedTs)
	require.Equal(t, uint64(109), res[9].ResolvedTs)
	require.False(t, rc.isFull())
}

func genEvents(helper *mounter.EventTestHelper, t *testing.T, ddl string, dmls ...string) (commonEvent.DDLEvent, []*common.RawKVEntry) {
	job := helper.DDL2Job(ddl)
	schema := job.SchemaName
	table := job.TableName
	kvEvents1 := helper.DML2RawKv(schema, table, dmls...)
	for _, e := range kvEvents1 {
		require.Equal(t, job.BinlogInfo.TableInfo.UpdateTS-1, e.StartTs)
		require.Equal(t, job.BinlogInfo.TableInfo.UpdateTS+1, e.CRTs)
	}
	return commonEvent.DDLEvent{
		FinishedTs: job.BinlogInfo.TableInfo.UpdateTS,
		Job:        job,
	}, kvEvents1
}

func TestSendEvents(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlEvent, kvEvents := genEvents(helper, t, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
		`insert into test.t(id,c) values (2, "c2")`,
	}...)
	require.NotNil(t, kvEvents)

	ddlEvent1, _ := genEvents(helper, t, `alter table test.t add column dummy int default 0`)
	require.Less(t, ddlEvent.FinishedTs, ddlEvent1.FinishedTs)

	ddlEvent2, kvEvents2 := genEvents(helper, t, `alter table test.t add column d int`, []string{
		`insert into test.t(id,c,d) values (10, "c10", 10)`,
		`insert into test.t(id,c,d) values (11, "c11", 11)`,
		`insert into test.t(id,c,d) values (12, "c12", 12)`,
	}...)
	require.NotNil(t, kvEvents2)
	require.Less(t, kvEvents[0].CRTs, kvEvents2[1].CRTs)
	require.Less(t, ddlEvent.FinishedTs, ddlEvent2.FinishedTs)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventStore := newMockEventStore()
	schemaStore := newMockSchemaStore()
	msgCh := make(chan *messaging.TargetMessage, 1024)
	mc := &mockMessageCenter{messageCh: msgCh}

	wg.Add(1)
	go func() {
		defer wg.Done()
		expected := []struct {
			t        int // type
			commitTs common.Ts
		}{
			{t: commonEvent.TypeDDLEvent, commitTs: ddlEvent.FinishedTs},
			{t: commonEvent.TypeDMLEvent, commitTs: kvEvents[0].CRTs},
			{t: commonEvent.TypeDDLEvent, commitTs: ddlEvent1.FinishedTs},
			{t: commonEvent.TypeDDLEvent, commitTs: ddlEvent2.FinishedTs},
			{t: commonEvent.TypeDMLEvent, commitTs: kvEvents2[0].CRTs},
			{t: commonEvent.TypeBatchResolvedEvent, commitTs: common.Ts(0)},
		}
		cnt := 0
		for {
			select {
			case <-ctx.Done():
				return
			case msgs := <-msgCh:
				for _, msg := range msgs.Message {
					event, ok := msg.(commonEvent.Event)
					require.True(t, ok)
					fmt.Printf("cnt: %d -> %+v\n", cnt, event)
					require.Equal(t, expected[cnt].t, event.GetType(), "cnt: %d, e: %+v", cnt, event)
					require.Equal(t, expected[cnt].commitTs, event.GetCommitTs(), "cnt: %d, e: %+v", cnt, event)
					cnt++

					if cnt == len(expected) {
						return
					}
				}
			}
		}
	}()

	s := newEventBroker(ctx, 1, eventStore, schemaStore, mc, time.Local)
	defer s.close()

	// Register the dispatcher
	tableID := ddlEvent.TableID
	info := newMockAcceptorInfo(common.NewDispatcherID(), tableID)
	s.addDispatcher(info)
	_, ok := s.spans[tableID]
	require.True(t, ok)

	schemaStore.AppendDDLEvent(tableID, ddlEvent, ddlEvent1, ddlEvent2)

	span, ok := eventStore.spans[tableID]
	require.True(t, ok)
	span.update(ddlEvent2.FinishedTs+1, append(kvEvents, kvEvents2...)...)

	wg.Wait()
}

// mockDispatcherInfo is a mock implementation of the AcceptorInfo interface
type mockDispatcherInfo struct {
	clusterID  uint64
	serverID   string
	id         common.DispatcherID
	topic      string
	span       *heartbeatpb.TableSpan
	startTs    uint64
	isRegister bool
}

func newMockAcceptorInfo(dispatcherID common.DispatcherID, tableID int64) *mockDispatcherInfo {
	return &mockDispatcherInfo{
		clusterID: 1,
		serverID:  "server1",
		id:        dispatcherID,
		topic:     "topic1",
		span: &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
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

func (m *mockDispatcherInfo) GetTableSpan() *heartbeatpb.TableSpan {
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
	return &tconfig.FilterConfig{
		Rules: []string{"*.*"},
	}
}

type mockSpanStats struct {
	startTs       uint64
	watermark     uint64
	pendingEvents []*common.RawKVEntry
	onUpdate      func(watermark uint64)
	onEvent       func(event *common.RawKVEntry)
}

func (m *mockSpanStats) update(watermark uint64, events ...*common.RawKVEntry) {
	m.pendingEvents = append(m.pendingEvents, events...)
	m.watermark = watermark
	for _, e := range events {
		m.onEvent(e)
	}
	m.onUpdate(watermark)
}
