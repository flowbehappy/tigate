// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package conflictdetector

import (
	"sync"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// ConflictDetector implements a logic that dispatches transaction
// to different worker cache channels in a way that transactions
// modifying the same keys are never executed concurrently and
// have their original orders preserved. Transactions in different
// channels can be executed concurrently.
type ConflictDetector struct {
	// resolvedTxnCaches are used to cache resolved transactions.
	resolvedTxnCaches []txnCache

	// slots are used to find all unfinished transactions
	// conflicting with an incoming transactions.
	slots    *Slots
	numSlots uint64

	// nextCacheID is used to dispatch transactions round-robin.
	nextCacheID atomic.Int64

	closeCh chan struct{}

	notifiedNodes chan func()
	wg            sync.WaitGroup
}

// NewConflictDetector creates a new ConflictDetector.
func NewConflictDetector(
	numSlots uint64, opt TxnCacheOption,
) *ConflictDetector {
	ret := &ConflictDetector{
		resolvedTxnCaches: make([]txnCache, opt.Count),
		slots:             NewSlots(numSlots),
		numSlots:          numSlots,
		closeCh:           make(chan struct{}),
		notifiedNodes:     make(chan func(), 1000000000), // 需要想过如何设置 buffer 长度
	}
	for i := 0; i < opt.Count; i++ {
		ret.resolvedTxnCaches[i] = newTxnCache(opt)
	}

	// task := newNotifyTask(&ret.notifiedChan)
	// threadpool.GetTaskSchedulerInstance().SinkTaskScheduler.Submit(task, threadpool.CPUTask, time.Time{})
	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.runBackgroundTasks()
	}()

	return ret
}

func (d *ConflictDetector) runBackgroundTasks() {
	for {
		select {
		case <-d.closeCh:
			return
		case notifyCallback := <-d.notifiedNodes:
			if notifyCallback != nil {
				notifyCallback()
			}
		}
	}
}

// Add pushes a transaction to the ConflictDetector.
//
// NOTE: if multiple threads access this concurrently,
// ConflictKeys must be sorted by the slot index.
func (d *ConflictDetector) Add(txn *common.TxnEvent, tableProgress *types.TableProgress) {
	hashes := ConflictKeys(txn)
	node := d.slots.AllocNode(hashes)
	txn.PostTxnFlushed = func() { // flush 的时候被调用 这个写法后面要想一下，感觉不是特别好
		// After this transaction is executed, we can remove the node from the graph,
		// and resolve related dependencies for these transacitons which depend on this
		// executed transaction.
		d.slots.Remove(node)
		tableProgress.Remove(txn)
	}
	node.TrySendToTxnCache = func(cacheID int64) bool {
		// Try sending this txn to related cache as soon as all dependencies are resolved.
		return d.sendToCache(txn, cacheID)
	}
	node.RandCacheID = func() int64 { return d.nextCacheID.Add(1) % int64(len(d.resolvedTxnCaches)) }
	node.OnNotified = func(callback func()) { d.notifiedNodes <- callback }
	d.slots.Add(node)
}

// Close closes the ConflictDetector.
func (d *ConflictDetector) Close() {
	close(d.closeCh)
}

// sendToCache should not call txn.Callback if it returns an error.
func (d *ConflictDetector) sendToCache(txn *common.TxnEvent, id int64) bool {
	if id < 0 {
		log.Panic("must assign with a valid cacheID", zap.Int64("cacheID", id))
	}

	cache := d.resolvedTxnCaches[id]
	ok := cache.add(txn)
	return ok
}

// GetOutChByCacheID returns the output channel by cacheID.
// Note txns in single cache should be executed sequentially.
func (d *ConflictDetector) GetOutChByCacheID(id int64) <-chan *common.TxnEvent {
	if id < 0 {
		log.Panic("must assign with a valid cacheID", zap.Int64("cacheID", id))
	}
	return d.resolvedTxnCaches[id].out()
}
