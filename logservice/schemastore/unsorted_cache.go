package schemastore

import (
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/google/btree"
)

type ddlCache struct {
	mutex sync.Mutex
	// ordered by commitTS
	// TODO: is commitTS unique?
	ddlEvents *btree.BTreeG[DDLEvent]
}

func ddlEventLess(a, b DDLEvent) bool {
	return a.CommitTS < b.CommitTS || (a.CommitTS == b.CommitTS && a.Job.BinlogInfo.SchemaVersion < b.Job.BinlogInfo.SchemaVersion)
}

func newDDLCache() *ddlCache {
	return &ddlCache{
		ddlEvents: btree.NewG[DDLEvent](16, ddlEventLess),
	}
}

func (c *ddlCache) addDDLEvent(ddlEvent DDLEvent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	oldEvent, duplicated := c.ddlEvents.ReplaceOrInsert(ddlEvent)
	if duplicated {
		log.Debug("ignore duplicated DDL event",
			zap.Any("event", ddlEvent),
			zap.Any("oldEvent", oldEvent))
	}
}

func (c *ddlCache) fetchSortedDDLEventBeforeTS(ts common.Ts) []DDLEvent {
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

type ddlListWithFilter struct {
	events []DDLEvent
	filter filter.Filter
}
