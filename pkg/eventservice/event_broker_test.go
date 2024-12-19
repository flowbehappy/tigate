package eventservice

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/stretchr/testify/require"
)

func newTableSpan(tableID int64, start, end string) *heartbeatpb.TableSpan {
	return &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: []byte(start),
		EndKey:   []byte(end),
	}
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

	eb := newEventBroker(ctx, 1, eventStore, schemaStore, mc, time.Local)
	defer eb.close()

	// Register the dispatcher
	tableID := ddlEvent.TableID
	info := newMockDispatcherInfo(t, common.NewDispatcherID(), tableID, eventpb.ActionType_ACTION_TYPE_REGISTER)
	eb.addDispatcher(info)

	schemaStore.AppendDDLEvent(tableID, ddlEvent, ddlEvent1, ddlEvent2)

	v, ok := eventStore.spansMap.Load(tableID)
	require.True(t, ok)
	span := v.(*mockSpanStats)
	span.update(ddlEvent2.FinishedTs+1, append(kvEvents, kvEvents2...)...)

	wg.Wait()
}
