package eventstore

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestMemorySorterBasic(t *testing.T) {
	ctx := context.Background()
	sorter := NewMemorySorter(ctx)
	defer sorter.Close()

	// Test AddSubscription
	subID := logpuller.SubscriptionID(1)
	startTs := common.Ts(100)
	sorter.AddSubscription(subID, startTs)

	// Prepare test data
	events := []kvEvent{
		{raw: &common.RawKVEntry{StartTs: 101, CRTs: 105}},
		{raw: &common.RawKVEntry{StartTs: 102, CRTs: 106}},
		// Add resolved event
		{raw: &common.RawKVEntry{StartTs: 0, CRTs: 110, OpType: common.OpTypeResolved}},
	}

	// Add events
	sorter.Add(subID, events...)

	// Test Fetch
	iter := sorter.Fetch(subID, 100, 110)
	require.False(t, iter.IsEmpty())

	// Verify events are returned in order
	evt1, isNewTxn1, err := iter.Next()
	require.NoError(t, err)
	require.True(t, isNewTxn1)
	require.Equal(t, common.Ts(105), evt1.CRTs)

	evt2, isNewTxn2, err := iter.Next()
	require.NoError(t, err)
	require.True(t, isNewTxn2)
	require.Equal(t, common.Ts(106), evt2.CRTs)

	// Verify iterator is exhausted
	require.False(t, iter.Valid())
}
