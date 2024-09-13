package schemastore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	cache := newDDLCache()
	cache.addDDLEvent(PersistedDDLEvent{FinishedTs: 1})
	cache.addDDLEvent(PersistedDDLEvent{FinishedTs: 100})
	cache.addDDLEvent(PersistedDDLEvent{FinishedTs: 50})
	cache.addDDLEvent(PersistedDDLEvent{FinishedTs: 30})
	cache.addDDLEvent(PersistedDDLEvent{FinishedTs: 40})
	cache.addDDLEvent(PersistedDDLEvent{FinishedTs: 9})
	cache.addDDLEvent(PersistedDDLEvent{FinishedTs: 15})
	events := cache.fetchSortedDDLEventBeforeTS(30)
	require.Equal(t, len(events), 4)
	require.Equal(t, events[0].FinishedTs, uint64(1))
	require.Equal(t, events[1].FinishedTs, uint64(9))
	require.Equal(t, events[2].FinishedTs, uint64(15))
	require.Equal(t, events[3].FinishedTs, uint64(30))
	events = cache.fetchSortedDDLEventBeforeTS(50)
	require.Equal(t, len(events), 2)
	require.Equal(t, events[0].FinishedTs, uint64(40))
	require.Equal(t, events[1].FinishedTs, uint64(50))
}
