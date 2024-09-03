package eventservice

import (
	"sync"
	"testing"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
)

func TestNewDispatcherStat(t *testing.T) {
	startTs := uint64(123)

	info := &mockDispatcherInfo{
		id:        common.NewDispatcherID(),
		clusterID: 1,
		startTs:   startTs,
	}

	stat := newDispatcherStat(startTs, info, func(c subscriptionChange) {}, nil)
	require.Equal(t, info, stat.info)
	require.Equal(t, startTs, stat.watermark.Load())
	require.NotNil(t, stat.spanSubscription)
	require.Equal(t, startTs, stat.spanSubscription.watermark.Load())
	require.Equal(t, 0, int(stat.spanSubscription.newEventCount.Load()))
	require.NotEmpty(t, stat.workerIndex)
}

func TestDispatcherStatUpdateWatermark(t *testing.T) {
	startTs := uint64(123)
	wg := &sync.WaitGroup{}
	info := &mockDispatcherInfo{
		id:        common.NewDispatcherID(),
		clusterID: 1,
		startTs:   startTs,
	}

	notify := make(chan subscriptionChange)

	stat := newDispatcherStat(startTs, info, func(c subscriptionChange) {
		select {
		case notify <- c:
		default:
		}
	}, nil)

	// Case 1: no new events, only watermark change
	wg.Add(1)
	go func() {
		defer wg.Done()
		stat.onSubscriptionWatermark(456)
	}()
	subChange := <-notify
	require.Equal(t, uint64(456), stat.spanSubscription.watermark.Load())
	require.Equal(t, info.id, subChange.dispatcherInfo.GetID())
	require.Equal(t, uint64(0), subChange.eventCount)
	log.Info("pass TestDispatcherStatUpdateWatermark case 1")

	// Case 2: new events, and watermark increase
	stat.onNewEvent(&common.RawKVEntry{})
	stat.onNewEvent(&common.RawKVEntry{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		stat.onSubscriptionWatermark(789)
	}()
	subChange = <-notify
	require.Equal(t, uint64(789), stat.spanSubscription.watermark.Load())
	require.Equal(t, info.id, subChange.dispatcherInfo.GetID())
	require.Equal(t, uint64(2), subChange.eventCount)
	require.Equal(t, 0, int(stat.spanSubscription.newEventCount.Load()))
	log.Info("pass TestDispatcherStatUpdateWatermark case 2")

	// Case 3: new events, and watermark decrease
	// watermark should not decrease and no notification
	stat.onNewEvent(&common.RawKVEntry{})
	stat.onNewEvent(&common.RawKVEntry{})
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		stat.onSubscriptionWatermark(456)
		close(done)
	}()
	<-done
	require.Equal(t, uint64(789), stat.spanSubscription.watermark.Load())
	require.Equal(t, 2, int(stat.spanSubscription.newEventCount.Load()))
	log.Info("pass TestDispatcherStatUpdateWatermark case 3")

	wg.Wait()
}
func TestScanTaskPool_PushTask(t *testing.T) {
	pool := newScanTaskPool()
	span := newTableSpan(1, "a", "b")
	dispatcherInfo := &mockDispatcherInfo{
		id:        common.NewDispatcherID(),
		clusterID: 1,
		startTs:   1000,
		span:      span,
	}
	dispatcherStat := newDispatcherStat(dispatcherInfo.startTs, dispatcherInfo, func(c subscriptionChange) {}, nil)
	// Create two tasks with overlapping data ranges
	task1 := &scanTask{
		dispatcherStat: dispatcherStat,
		dataRange: &common.DataRange{
			ClusterID: uint64(1),
			Span:      span,
			StartTs:   1000,
			EndTs:     2000,
		},
		eventCount: 10,
	}

	task2 := &scanTask{
		dispatcherStat: dispatcherStat,
		dataRange: &common.DataRange{
			ClusterID: 1,
			Span:      span,
			StartTs:   1500,
			EndTs:     2500,
		},
		eventCount: 5,
	}

	expectedTask := &scanTask{
		dispatcherStat: dispatcherStat,
		dataRange: &common.DataRange{
			ClusterID: 1,
			Span:      span,
			StartTs:   1000,
			EndTs:     2500,
		},
		eventCount: 15,
	}

	// make the pool contain the task1 already
	pool.taskSet[dispatcherInfo.GetID()] = task1

	// Verify that the task is in the taskSet
	task, ok := pool.taskSet[dispatcherInfo.id]
	require.True(t, ok)
	require.Equal(t, task1, task)

	// Push the second task
	pool.pushTask(task2)

	// Verify that the task is sent to corresponding pendingTaskQueue
	receivedTask := <-pool.pendingTaskQueue[dispatcherStat.workerIndex]
	require.Equal(t, expectedTask, receivedTask)

	// Verify that the task is set to nil in the taskSet
	task, ok = pool.taskSet[dispatcherInfo.GetID()]
	require.True(t, ok)
	require.Nil(t, task)

}

func newTableSpan(tableID uint64, start, end string) *common.TableSpan {
	res := &common.TableSpan{}
	res.TableSpan = &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: []byte(start),
		EndKey:   []byte(end),
	}
	return res
}

func TestResolvedTsCache(t *testing.T) {
	rc := newResolvedTsCache(10)
	require.Equal(t, 0, rc.len)
	require.Equal(t, 10, len(rc.cache))
	require.Equal(t, 10, rc.limit)

	// Case 1: insert a new resolved ts
	rc.add(common.ResolvedEvent{
		DispatcherID: common.NewDispatcherID(),
		ResolvedTs:   100,
	})
	require.Equal(t, 1, rc.len)
	require.Equal(t, uint64(100), rc.cache[0].ResolvedTs)
	require.False(t, rc.isFull())

	// Case 2: add more resolved ts until full
	i := 1
	for !rc.isFull() {
		rc.add(common.ResolvedEvent{
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
