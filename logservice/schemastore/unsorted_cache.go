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
	ddlEvents *btree.BTreeG[DDLJobWithCommitTs]
}

func ddlEventLess(a, b DDLJobWithCommitTs) bool {
	// TODO: commitTs or finishedTs
	return a.CommitTs < b.CommitTs ||
		(a.CommitTs == b.CommitTs && a.Job.BinlogInfo.SchemaVersion < b.Job.BinlogInfo.SchemaVersion)
}

func newDDLCache() *ddlCache {
	return &ddlCache{
		ddlEvents: btree.NewG[DDLJobWithCommitTs](16, ddlEventLess),
	}
}

func (c *ddlCache) addDDLEvent(ddlEvent DDLJobWithCommitTs) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	oldEvent, duplicated := c.ddlEvents.ReplaceOrInsert(ddlEvent)
	if duplicated {
		log.Debug("ignore duplicated DDL event",
			zap.Any("event", ddlEvent),
			zap.Any("oldEvent", oldEvent))
	}
}

func (c *ddlCache) fetchSortedDDLEventBeforeTS(ts uint64) []DDLJobWithCommitTs {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	events := make([]DDLJobWithCommitTs, 0)
	c.ddlEvents.Ascend(func(event DDLJobWithCommitTs) bool {
		if event.CommitTs <= ts {
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
