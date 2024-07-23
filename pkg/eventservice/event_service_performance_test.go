package eventservice

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// This test is used to test how many time it takes to create 1 million tables
// and 10 round of resolvedTs events for each table.
// Result:
//  1. It takes about 10 seconds to register 1 million tables.
//  2. It takes about 300-500ms to update resolvedTs for 1 million tables. The call chain is:
//     -> dispatcherStat.onSubscriptionWatermark() -> dispatcherStat.notify() -> taskPool.pushTask(), merge task -> scanWorker new Msg -> messageCenter.SendMsg()
//     It should be note that some task of the same dispatcher are merged into one task, so the messageCenter.SendMsg() is not called for each dispatcherStat.onSubscriptionWatermark().
func TestEventServiceOneMillionTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	mockStore := newMockEventStore()
	tableNum := 1000000
	sendRound := 10
	mc := &mockMessageCenter{
		messageCh: make(chan *messaging.TargetMessage, 100),
	}
	var receivedMsgCount atomic.Uint64
	wg.Add(1)
	// drain the message channel
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Info("all message channel are consumed", zap.Uint64("messageCount", receivedMsgCount.Load()))
				return // exit
			case <-mc.messageCh:
				receivedMsgCount.Add(1)
				// Only drain the message channel
			}
		}
	}()

	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(appcontext.EventStore, mockStore)
	es := NewEventService(ctx)
	esImpl := es.(*eventService)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		if err != nil {
			t.Errorf("EventService.Run() error = %v", err)
		}
	}()

	start := time.Now()
	clusterID := uint64(1)
	dispatchers := make([]DispatcherInfo, 0, tableNum)
	// register 1000,000 tables
	for i := 0; i < tableNum; i++ {
		acceptorInfo := newMockAcceptorInfo(uuid.New().String(), uint64(i))
		dispatchers = append(dispatchers, acceptorInfo)
	}
	for _, dispatcher := range dispatchers {
		esImpl.registerDispatcher(dispatcher)
	}
	require.Len(t, esImpl.brokers[clusterID].dispatchers.m, tableNum)
	log.Info("register 1 million tables", zap.Duration("cost", time.Since(start)))

	// Update resolvedTs for each table 10 times
	start = time.Now()
	wg.Add(1)
	resolvedTsUpdateInterval := 500 * time.Millisecond
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(resolvedTsUpdateInterval)
		defer ticker.Stop()
		round := 0
		for {
			<-ticker.C
			sendStart := time.Now()
			for _, dispatcher := range dispatchers {
				sub, ok := mockStore.spans[dispatcher.GetTableSpan().TableID]
				if !ok {
					continue
				}
				sub.update(nil, sub.watermark+1)
			}
			log.Info("send resolvedTs events for 1 million tables", zap.Duration("cost", time.Since(sendStart)), zap.Any("round", round))
			round++
			if round == sendRound {
				time.Sleep(5 * time.Second)
				cancel()
				return
			}
		}
	}()

	wg.Wait()
	log.Info("send 10 rounds of resolvedTs events of 1 million tables finished", zap.Duration("cost", time.Since(start)))
}
