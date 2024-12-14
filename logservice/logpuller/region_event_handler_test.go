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

package logpuller

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

// For UPDATE SQL, its prewrite event has both value and old value.
// It is possible that TiDB prewrites multiple times for the same row when
// there are other transactions it conflicts with. For this case,
// if the value is not "short", only the first prewrite contains the value.
//
// TiKV may output events for the UPDATE SQL as following:
//
// TiDB: [Prwrite1]    [Prewrite2]      [Commit]
//
//	v             v                v                                   Time
//
// ---------------------------------------------------------------------------->
//
//	^            ^    ^           ^     ^       ^     ^          ^     ^
//
// TiKV:   [Scan Start] [Send Prewrite2] [Send Commit] [Send Prewrite1] [Send Init]
// TiCDC:                    [Recv Prewrite2]  [Recv Commit] [Recv Prewrite1] [Recv Init]
func TestHandleEventEntryEventOutOfOrder(t *testing.T) {
	// initialize
	option := dynstream.NewOption()
	ds := dynstream.NewParallelDynamicStream(func(subID SubscriptionID) uint64 { return uint64(subID) }, &regionEventHandler{}, option)
	ds.Start()

	span := heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: spanz.ToComparableKey([]byte{}), // TODO: remove spanz dependency
		EndKey:   spanz.ToComparableKey(spanz.UpperBoundKey),
	}
	subID := SubscriptionID(999)
	eventCh := make(chan common.RawKVEntry, 1000)
	consumeKVEvents := func(events []common.RawKVEntry, _ func()) bool {
		for _, e := range events {
			eventCh <- e
		}
		return false
	}
	advanceResolvedTs := func(ts uint64) {
		// not used
	}
	subSpan := &subscribedSpan{
		subID:             subID,
		span:              span,
		startTs:           1000, // not used
		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   0,
	}
	ds.AddPath(subID, subSpan, dynstream.AreaSettings{})

	region := newRegionInfo(
		tikv.RegionVerID{},
		span,
		&tikv.RPCContext{},
		subSpan,
	)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	state := newRegionFeedState(region, 1)
	state.start()

	// Receive prewrite2 with empty value.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{{
					StartTs:  1,
					Type:     cdcpb.Event_PREWRITE,
					OpType:   cdcpb.Event_Row_PUT,
					Key:      []byte("key"),
					Value:    nil,
					OldValue: []byte("oldvalue"),
				}},
			},
		}
		regionEvent := regionEvent{
			state:   state,
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Receive commit.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{{
					StartTs:  1,
					CommitTs: 2,
					Type:     cdcpb.Event_COMMIT,
					OpType:   cdcpb.Event_Row_PUT,
					Key:      []byte("key"),
				}},
			},
		}
		regionEvent := regionEvent{
			state:   state,
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must not output event.
	{
		select {
		case <-eventCh:
			require.True(t, false, "shouldn't get an event")
		case <-time.NewTimer(100 * time.Millisecond).C:
		}
	}

	// Receive prewrite1 with actual value.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{{
					StartTs:  1,
					Type:     cdcpb.Event_PREWRITE,
					OpType:   cdcpb.Event_Row_PUT,
					Key:      []byte("key"),
					Value:    []byte("value"),
					OldValue: []byte("oldvalue"),
				}},
			},
		}
		regionEvent := regionEvent{
			state:   state,
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must not output event.
	{
		select {
		case <-eventCh:
			require.True(t, false, "shouldn't get an event")
		case <-time.NewTimer(100 * time.Millisecond).C:
		}
	}

	// Receive initialized.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{
					{
						Type: cdcpb.Event_INITIALIZED,
					},
				},
			},
		}
		regionEvent := regionEvent{
			state:   state,
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must output event.
	{
		select {
		case event := <-eventCh:
			require.Equal(t, uint64(2), event.CRTs)
			require.Equal(t, uint64(1), event.StartTs)
			require.Equal(t, "value", string(event.Value))
			require.Equal(t, "oldvalue", string(event.OldValue))
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}
}

func TestHandleResolvedTs(t *testing.T) {
	// initialize
	option := dynstream.NewOption()
	ds := dynstream.NewParallelDynamicStream(func(subID SubscriptionID) uint64 { return uint64(subID) }, &regionEventHandler{}, option)
	ds.Start()

	consumeKVEvents := func(events []common.RawKVEntry, _ func()) bool { return false } // not used
	tsCh := make(chan uint64, 100)
	advanceResolvedTs := func(ts uint64) {
		tsCh <- ts
	}

	subID1 := SubscriptionID(1)

	state1 := newRegionFeedState(regionInfo{verID: tikv.NewRegionVerID(1, 1, 1)}, uint64(subID1))
	state1.start()
	{
		span := heartbeatpb.TableSpan{
			TableID:  100,
			StartKey: spanz.ToComparableKey([]byte{}), // TODO: remove spanz dependency
			EndKey:   spanz.ToComparableKey(spanz.UpperBoundKey),
		}
		subSpan := &subscribedSpan{
			subID:             subID1,
			span:              heartbeatpb.TableSpan{},
			rangeLock:         regionlock.NewRangeLock(uint64(subID1), span.StartKey, span.EndKey, 1),
			consumeKVEvents:   consumeKVEvents,
			advanceResolvedTs: advanceResolvedTs,
			advanceInterval:   0,
		}
		ds.AddPath(subID1, subSpan, dynstream.AreaSettings{})
		state1.region.subscribedSpan = subSpan
		state1.region.lockedRangeState = &regionlock.LockedRangeState{}
		state1.setInitialized()
		state1.updateResolvedTs(9)
	}

	subID2 := SubscriptionID(2)
	state2 := newRegionFeedState(regionInfo{verID: tikv.NewRegionVerID(2, 2, 2)}, uint64(subID2))
	state2.start()
	{
		span := heartbeatpb.TableSpan{
			TableID:  100,
			StartKey: spanz.ToComparableKey([]byte{}), // TODO: remove spanz dependency
			EndKey:   spanz.ToComparableKey(spanz.UpperBoundKey),
		}
		subSpan := &subscribedSpan{
			subID:             subID2,
			span:              span,
			rangeLock:         regionlock.NewRangeLock(uint64(subID2), span.StartKey, span.EndKey, 1),
			consumeKVEvents:   consumeKVEvents,
			advanceResolvedTs: advanceResolvedTs,
			advanceInterval:   0,
		}
		ds.AddPath(subID2, subSpan, dynstream.AreaSettings{})
		state2.region.subscribedSpan = subSpan
		state2.region.lockedRangeState = &regionlock.LockedRangeState{}
		state2.setInitialized()
		state2.updateResolvedTs(11)
	}

	subID3 := SubscriptionID(3)
	state3 := newRegionFeedState(regionInfo{verID: tikv.NewRegionVerID(3, 3, 3)}, uint64(subID3))
	state3.start()
	{
		span := heartbeatpb.TableSpan{
			TableID:  100,
			StartKey: spanz.ToComparableKey([]byte{}), // TODO: remove spanz dependency
			EndKey:   spanz.ToComparableKey(spanz.UpperBoundKey),
		}
		subSpan := &subscribedSpan{
			subID:             subID3,
			span:              span,
			rangeLock:         regionlock.NewRangeLock(uint64(subID3), span.StartKey, span.EndKey, 1),
			consumeKVEvents:   consumeKVEvents,
			advanceResolvedTs: advanceResolvedTs,
			advanceInterval:   0,
		}
		ds.AddPath(subID3, subSpan, dynstream.AreaSettings{})
		state3.region.subscribedSpan = subSpan
		state3.region.lockedRangeState = &regionlock.LockedRangeState{}
		state3.updateResolvedTs(8)
	}

	{
		regionEvent := regionEvent{
			state:      state1,
			resolvedTs: 10,
		}
		ds.Push(subID1, regionEvent)
	}
	{
		regionEvent := regionEvent{
			state:      state2,
			resolvedTs: 10,
		}
		ds.Push(subID2, regionEvent)
	}
	{
		regionEvent := regionEvent{
			state:      state3,
			resolvedTs: 10,
		}
		ds.Push(subID3, regionEvent)
	}

	// should only get one ts event
	{
		select {
		case <-tsCh:
			// the ts is from range lock, it is hard code in the test code
		case <-time.NewTimer(300 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}

		select {
		case <-tsCh:
			require.True(t, false, "shouldn't get an event")
		case <-time.NewTimer(300 * time.Millisecond).C:
		}
	}

	require.Equal(t, uint64(10), state1.getLastResolvedTs())
	require.Equal(t, uint64(11), state2.getLastResolvedTs())
	require.Equal(t, uint64(8), state3.getLastResolvedTs())
}
