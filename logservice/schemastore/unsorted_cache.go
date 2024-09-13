package schemastore

import (
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/google/btree"
)

type ddlCache struct {
	mutex sync.Mutex
	// ordered by FinishedTs
	ddlEvents *btree.BTreeG[PersistedDDLEvent]
}

func ddlEventLess(a, b PersistedDDLEvent) bool {
	return a.FinishedTs < b.FinishedTs || (a.FinishedTs == b.FinishedTs && a.SchemaVersion < b.SchemaVersion)
}

func newDDLCache() *ddlCache {
	return &ddlCache{
		ddlEvents: btree.NewG[PersistedDDLEvent](16, ddlEventLess),
	}
}

func (c *ddlCache) addDDLEvent(ddlEvent PersistedDDLEvent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	oldEvent, duplicated := c.ddlEvents.ReplaceOrInsert(ddlEvent)
	if duplicated {
		log.Debug("ignore duplicated DDL event",
			zap.Any("event", ddlEvent),
			zap.Any("oldEvent", oldEvent))
	}
}

func (c *ddlCache) fetchSortedDDLEventBeforeTS(ts uint64) []PersistedDDLEvent {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	events := make([]PersistedDDLEvent, 0)
	c.ddlEvents.Ascend(func(event PersistedDDLEvent) bool {
		if event.FinishedTs <= ts {
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
