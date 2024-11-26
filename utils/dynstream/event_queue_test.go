package dynstream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockDest = struct{}

var singleEventType = EventType{
	DataGroup: 1,
	Property:  NonBatchable,
}

var repeatedEventType = EventType{
	DataGroup: 2,
	Property:  BatchableData,
}

func TestPopEvents(t *testing.T) {
	handler := &simpleHandler{}
	option := NewOption()
	batchSize := 3
	option.BatchCount = batchSize
	q := newEventQueue[int, string, *simpleEvent, mockDest, *simpleHandler](option, handler)

	pathInfo1 := newPathInfo[int, string, *simpleEvent, mockDest, *simpleHandler](1, "path1", mockDest{})
	pathInfo2 := newPathInfo[int, string, *simpleEvent, mockDest, *simpleHandler](1, "path2", mockDest{})

	event1 := &simpleEvent{
		id:        1,
		path:      pathInfo1.path,
		eventType: singleEventType,
	}
	event2 := &simpleEvent{
		id:        2,
		path:      pathInfo2.path,
		eventType: repeatedEventType,
	}

	// 1. The first pop event.
	q.appendEvent(eventWrap[int, string, *simpleEvent, mockDest, *simpleHandler]{
		pathInfo:  pathInfo1,
		event:     event1,
		eventType: singleEventType,
		timestamp: 1,
		queueTime: time.Now(),
	})

	// 2. The second pop events in batch.
	q.appendEvent(eventWrap[int, string, *simpleEvent, mockDest, *simpleHandler]{
		pathInfo:  pathInfo2,
		event:     event2,
		eventType: repeatedEventType,
		timestamp: 2,
		queueTime: time.Now(),
	})
	q.appendEvent(eventWrap[int, string, *simpleEvent, mockDest, *simpleHandler]{
		pathInfo:  pathInfo2,
		event:     event2,
		eventType: repeatedEventType,
		timestamp: 3,
		queueTime: time.Now(),
	})
	q.appendEvent(eventWrap[int, string, *simpleEvent, mockDest, *simpleHandler]{
		pathInfo:  pathInfo2,
		event:     event2,
		eventType: repeatedEventType,
		timestamp: 4,
		queueTime: time.Now(),
	})
	// 3. The third pop event.
	q.appendEvent(eventWrap[int, string, *simpleEvent, mockDest, *simpleHandler]{
		pathInfo:  pathInfo2,
		event:     event2,
		eventType: repeatedEventType,
		timestamp: 5,
		queueTime: time.Now(),
	})
	// 4. The fourth pop event.
	q.appendEvent(eventWrap[int, string, *simpleEvent, mockDest, *simpleHandler]{
		pathInfo:  pathInfo1,
		event:     event1,
		eventType: singleEventType,
		timestamp: 6,
		queueTime: time.Now(),
	})

	require.Equal(t, int64(6), q.totalPendingLength.Load())

	buf := make([]*simpleEvent, 0, batchSize)

	// Case 1: Only one event in the buffer, since the first event is non-batchable.
	events, _, _ := q.popEvents(buf)
	require.Len(t, events, 1)
	require.Equal(t, events[0], event1)
	require.Equal(t, int64(5), q.totalPendingLength.Load())

	// Case 2: The buffer is full of the repeated event.
	buf = make([]*simpleEvent, 0, batchSize)
	events, pi, _ := q.popEvents(buf)
	require.Equal(t, pathInfo2.path, pi.path)
	require.Len(t, events, batchSize)
	require.Equal(t, int64(2), q.totalPendingLength.Load())
	require.Equal(t, events[0], event2)
	require.Equal(t, events[1], event2)
	require.Equal(t, events[2], event2)

	// Case 3: Only one event in the buffer, since the second event is non-batchable.
	buf = make([]*simpleEvent, 0, batchSize)
	events, _, _ = q.popEvents(buf)
	require.Equal(t, len(events), 1)
	require.Equal(t, events[0], event2)

	// Case 4: Only one event in the buffer, since the first event is non-batchable.
	buf = make([]*simpleEvent, 0, batchSize)
	events, _, _ = q.popEvents(buf)
	require.Equal(t, len(events), 1)
	require.Equal(t, events[0], event1)

	require.Equal(t, int64(0), q.totalPendingLength.Load())
}
