package eventservice

import (
	"math/rand"
	"sync"
	"testing"
	"time"

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

	stat := newDispatcherStat(startTs, info, nil)
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

	stat := newDispatcherStat(startTs, info, nil)

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
				stat.onNewEvent(&common.RawKVEntry{
					CRTs: ts,
				})
			}()
		}
		g.Wait()
	}

	// Case 1: no new events, only watermark change
	stat.onSubscriptionWatermark(456)
	require.Equal(t, uint64(456), stat.spanSubscription.watermark.Load())
	log.Info("pass TestDispatcherStatUpdateWatermark case 1")

	// Case 2: new events, and watermark increase
	sendNewEvent(startTs)
	stat.onSubscriptionWatermark(789)
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
		stat.onSubscriptionWatermark(456)
		close(done)
	}()
	<-done
	require.Equal(t, uint64(789), stat.spanSubscription.watermark.Load())
	require.Equal(t, uint64(360), stat.spanSubscription.maxEventCommitTs.Load())
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
	dispatcherStat := newDispatcherStat(dispatcherInfo.startTs, dispatcherInfo, nil)

	now := time.Now()
	task1 := &scanTask{
		dispatcherStat: dispatcherStat,
		createTime:     now.Add(1 * time.Second),
	}

	task2 := &scanTask{
		dispatcherStat: dispatcherStat,
		createTime:     now.Add(2 * time.Second),
	}

	// make the pool contain the task1 already
	pool.taskSet[dispatcherInfo.GetID()] = task1

	// Verify that the task is in the taskSet
	task, ok := pool.taskSet[dispatcherInfo.id]
	require.True(t, ok)
	require.Equal(t, task1, task)

	// Push the second task
	pool.pushTask(task2)
	// Verify that the task1 is sent to corresponding pendingTaskQueue, task2 is dropped since duplicate.
	receivedTask := <-pool.pendingTaskQueue[dispatcherStat.workerIndex]
	require.Equal(t, task1, receivedTask)

	// Verify that the task is removed from taskSet
	task, ok = pool.taskSet[dispatcherInfo.GetID()]
	require.False(t, ok)
	require.Nil(t, task)
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
