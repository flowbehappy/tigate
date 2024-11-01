package eventservice

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
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
	require.Nil(t, stat.filter)
}

func TestDispatcherStatUpdateWatermark(t *testing.T) {
	startTs := uint64(123)
	info := &mockDispatcherInfo{
		id:        common.NewDispatcherID(),
		clusterID: 1,
		startTs:   startTs,
	}

	spanSubscription := newSpanSubscription(info.GetTableSpan(), startTs)
	stat := newDispatcherStat(startTs, info, spanSubscription, nil)

	// Case 1: no new events, only watermark change
	stat.spanSubscription.onSubscriptionWatermark(456)
	require.Equal(t, uint64(456), stat.spanSubscription.watermark.Load())
	log.Info("pass TestDispatcherStatUpdateWatermark case 1")

	// Case 2: new events, and watermark increase
	stat.spanSubscription.onSubscriptionWatermark(789)
	stat.spanSubscription.onNewCommitTs(360)
	require.Equal(t, uint64(360), stat.spanSubscription.maxEventCommitTs.Load())
	require.Equal(t, uint64(789), stat.spanSubscription.watermark.Load())
	log.Info("pass TestDispatcherStatUpdateWatermark case 2")

	// Case 3: new events, and watermark decrease
	// watermark should not decrease
	stat.spanSubscription.onSubscriptionWatermark(456)
	stat.spanSubscription.onNewCommitTs(800)
	require.Equal(t, uint64(789), stat.spanSubscription.watermark.Load())
	require.Equal(t, uint64(800), stat.spanSubscription.maxEventCommitTs.Load())
	log.Info("pass TestDispatcherStatUpdateWatermark case 3")

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
	rc.add(pevent.ResolvedEvent{
		DispatcherID: common.NewDispatcherID(),
		ResolvedTs:   100,
	})
	require.Equal(t, 1, rc.len)
	require.Equal(t, uint64(100), rc.cache[0].ResolvedTs)
	require.False(t, rc.isFull())

	// Case 2: add more resolved ts until full
	i := 1
	for !rc.isFull() {
		rc.add(pevent.ResolvedEvent{
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

func TestSendEvents(t *testing.T) {
	helper := pevent.NewEventTestHelper(t)
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

	eventStore := newMockEventStore(100)
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
			{t: pevent.TypeDDLEvent, commitTs: ddlEvent.FinishedTs},
			{t: pevent.TypeDMLEvent, commitTs: kvEvents[0].CRTs},
			{t: pevent.TypeDDLEvent, commitTs: ddlEvent1.FinishedTs},
			{t: pevent.TypeDDLEvent, commitTs: ddlEvent2.FinishedTs},
			{t: pevent.TypeDMLEvent, commitTs: kvEvents2[0].CRTs},
			{t: pevent.TypeBatchResolvedEvent, commitTs: common.Ts(0)},
		}
		cnt := 0
		for {
			select {
			case <-ctx.Done():
				return
			case msgs := <-msgCh:
				for _, msg := range msgs.Message {
					event, ok := msg.(pevent.Event)
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
	info := newMockDispatcherInfo(common.NewDispatcherID(), tableID, eventpb.ActionType_ACTION_TYPE_REGISTER)
	s.addDispatcher(info)
	_, ok := s.spans[tableID]
	require.True(t, ok)

	schemaStore.AppendDDLEvent(tableID, ddlEvent, ddlEvent1, ddlEvent2)

	v, ok := eventStore.spansMap.Load(tableID)
	require.True(t, ok)
	span := v.(*mockSpanStats)
	span.update(ddlEvent2.FinishedTs+1, append(kvEvents, kvEvents2...)...)

	wg.Wait()
}
