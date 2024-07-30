package schemastore

import (
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/google/btree"
)

type unsortedDDLCache struct {
	mutex sync.Mutex
	// ordered by commitTS
	// TODO: is commitTS unique?
	ddlEvents *btree.BTreeG[DDLEvent]
}

func ddlEventLess(a, b DDLEvent) bool {
	return a.CommitTS < b.CommitTS || (a.CommitTS == b.CommitTS && a.Job.BinlogInfo.SchemaVersion < b.Job.BinlogInfo.SchemaVersion)
}

func newUnSortedDDLCache() *unsortedDDLCache {
	return &unsortedDDLCache{
		ddlEvents: btree.NewG[DDLEvent](16, ddlEventLess),
	}
}

func (c *unsortedDDLCache) addDDLEvent(ddlEvent DDLEvent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	oldEvent, duplicated := c.ddlEvents.ReplaceOrInsert(ddlEvent)
	if duplicated {
		log.Warn("ignore duplicated DDL event",
			zap.Any("event", ddlEvent),
			zap.Any("oldEvent", oldEvent))
	}
}

func (c *unsortedDDLCache) fetchSortedDDLEventBeforeTS(ts common.Ts) []DDLEvent {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	events := make([]DDLEvent, 0)
	c.ddlEvents.Ascend(func(event DDLEvent) bool {
		if event.CommitTS <= ts {
			events = append(events, event)
			return true
		}
		return false
	})
	for _, event := range events {
		c.ddlEvents.Delete(event)
	}
	return events
}
