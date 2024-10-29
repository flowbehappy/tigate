package eventstore

import (
	"context"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/pkg/metrics"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type gcRangeItem struct {
	dbIndex     int
	uniqueKeyID uint64
	tableID     int64
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

// add an item to delete the data in range (startTS, endTS] for `tableID` with `uniqueID`.
func (d *gcManager) addGCItem(dbIndex int, uniqueKeyID uint64, tableID int64, startTS uint64, endTS uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ranges = append(d.ranges, gcRangeItem{
		dbIndex:       dbIndex,
		uniqueKeyID:   uniqueKeyID,
		tableID:       tableID,
		startCommitTS: startTS,
		endCommitTS:   endTS,
	})
}

func (d *gcManager) fetchAllGCItems() []gcRangeItem {
	d.mu.Lock()
	defer d.mu.Unlock()
	ranges := d.ranges
	d.ranges = nil
	return ranges
}

type deleteFunc func(dbIndex int, uniqueKeyID uint64, tableID int64, startCommitTS uint64, endCommitTS uint64) error

func (d *gcManager) run(ctx context.Context, deleteDataRange deleteFunc) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ranges := d.fetchAllGCItems()
			if len(ranges) == 0 {
				continue
			}
			for _, r := range ranges {
				// TODO: delete in batch?
				err := deleteDataRange(r.dbIndex, r.uniqueKeyID, r.tableID, r.startCommitTS, r.endCommitTS)
				if err != nil {
					// TODO: add the data range back?
					log.Fatal("delete fail", zap.Error(err))
					return err
				}
			}
			metrics.EventStoreDeleteRangeCount.Add(float64(len(ranges)))
		}
	}
}
