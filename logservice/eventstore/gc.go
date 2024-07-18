package eventstore

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"go.uber.org/zap"
)

type gcRangeItem struct {
	span tablepb.Span
	// TODO: startCommitTS may be not needed now(just use 0 for every delete range maybe ok),
	// but after split table range, it may be essential?
	startCommitTS uint64
	endCommitTS   uint64
}

type gcManager struct {
	mu     sync.Mutex
	ranges []gcRangeItem
}

func newGCManager() *gcManager {
	return &gcManager{}
}

func (d *gcManager) addGCItem(span tablepb.Span, startCommitTS uint64, endCommitTS uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ranges = append(d.ranges, gcRangeItem{
		span:          span,
		startCommitTS: startCommitTS,
		endCommitTS:   endCommitTS,
	})
}

func (d *gcManager) fetchAllGCItems() []gcRangeItem {
	d.mu.Lock()
	defer d.mu.Unlock()
	ranges := d.ranges
	d.ranges = nil
	return ranges
}

type deleteFunc func(span tablepb.Span, startCommitTS uint64, endCommitTS uint64) error

func (d *gcManager) run(ctx context.Context, wg *sync.WaitGroup, deleteDataRange deleteFunc) {
	wg.Add(1)
	defer wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ranges := d.fetchAllGCItems()
			for _, r := range ranges {
				// TODO: delete in batch?
				err := deleteDataRange(r.span, r.startCommitTS, r.endCommitTS)
				if err != nil {
					// TODO: add the data range back?
					log.Fatal("delete fail", zap.Error(err))
				}
			}
		}
	}
}
