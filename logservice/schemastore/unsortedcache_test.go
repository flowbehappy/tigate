package schemastore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	cache := newUnSortedDDLCache()
	cache.addDDLEvent(DDLEvent{CommitTS: 1})
	cache.addDDLEvent(DDLEvent{CommitTS: 100})
	cache.addDDLEvent(DDLEvent{CommitTS: 50})
	cache.addDDLEvent(DDLEvent{CommitTS: 30})
	cache.addDDLEvent(DDLEvent{CommitTS: 40})
	cache.addDDLEvent(DDLEvent{CommitTS: 9})
	cache.addDDLEvent(DDLEvent{CommitTS: 15})
	events := cache.fetchSortedDDLEventBeforeTS(30)
	require.Equal(t, len(events), 4)
	require.Equal(t, events[0].CommitTS, Timestamp(1))
	require.Equal(t, events[1].CommitTS, Timestamp(9))
	require.Equal(t, events[2].CommitTS, Timestamp(15))
	require.Equal(t, events[3].CommitTS, Timestamp(30))
	events = cache.fetchSortedDDLEventBeforeTS(50)
	require.Equal(t, len(events), 2)
	require.Equal(t, events[0].CommitTS, Timestamp(40))
	require.Equal(t, events[1].CommitTS, Timestamp(50))
}
