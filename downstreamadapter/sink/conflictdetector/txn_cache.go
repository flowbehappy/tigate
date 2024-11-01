// Copyright 2024 PingCAP, Inc.
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
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

const (
	// BlockStrategyWaitAvailable means the cache will block until there is an available slot.
	BlockStrategyWaitAvailable BlockStrategy = "waitAvailable"
	// BlockStrategyWaitEmpty means the cache will block until all cached txns are consumed.
	BlockStrategyWaitEmpty = "waitEmpty"
	// TODO: maybe we can implement a strategy that can automatically adapt to different scenarios
)

// BlockStrategy is the strategy to handle the situation when the cache is full.
type BlockStrategy string

// TxnCacheOption is the option for creating a cache for resolved txns.
type TxnCacheOption struct {
	// Count controls the number of caches, txns in different caches could be executed concurrently.
	Count int
	// Size controls the max number of txns a cache can hold.
	Size int
	// BlockStrategy controls the strategy when the cache is full.
	BlockStrategy BlockStrategy
}

// In current implementation, the conflict detector will push txn to the txnCache.
type txnCache interface {
	// add adds a event to the Cache.
	add(txn *common.TxnEvent) bool
	// out returns a channel to receive events which are ready to be executed.
	out() <-chan *common.TxnEvent
}

func newTxnCache(opt TxnCacheOption) txnCache {
	log.Info("create new worker cache in conflict detector",
		zap.Int("cacheCount", opt.Count),
		zap.Int("cacheSize", opt.Size), zap.String("BlockStrategy", string(opt.BlockStrategy)))
	if opt.Size <= 0 {
		log.Panic("WorkerOption.CacheSize should be greater than 0, please report a bug")
	}

	switch opt.BlockStrategy {
	case BlockStrategyWaitAvailable:
		return &boundedTxnCache{ch: make(chan *common.TxnEvent, opt.Size)}
	case BlockStrategyWaitEmpty:
		return &boundedTxnCacheWithBlock{ch: make(chan *common.TxnEvent, opt.Size)}
	default:
		return nil
	}
}

// boundedTxnCache is a cache which has a limit on the number of txns it can hold.
//
//nolint:unused
type boundedTxnCache struct {
	ch chan *common.TxnEvent
}

//nolint:unused
func (w *boundedTxnCache) add(txn *common.TxnEvent) bool {
	select {
	case w.ch <- txn:
		return true
	default:
		return false
	}
}

//nolint:unused
func (w *boundedTxnCache) out() <-chan *common.TxnEvent {
	return w.ch
}

// boundedTxnCacheWithBlock is a special boundedWorker. Once the cache
// is full, it will block until all cached txns are consumed.
type boundedTxnCacheWithBlock struct {
	ch chan *common.TxnEvent
	//nolint:unused
	isBlocked atomic.Bool
}

//nolint:unused
func (w *boundedTxnCacheWithBlock) add(txn *common.TxnEvent) bool {
	if w.isBlocked.Load() && len(w.ch) <= 0 {
		w.isBlocked.Store(false)
	}

	if !w.isBlocked.Load() {
		select {
		case w.ch <- txn:
			return true
		default:
			w.isBlocked.CompareAndSwap(false, true)
		}
	}
	return false
}

//nolint:unused
func (w *boundedTxnCacheWithBlock) out() <-chan *common.TxnEvent {
	return w.ch
}
