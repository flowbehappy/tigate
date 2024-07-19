package common

import (
	"testing"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func TestMergeDataRange(t *testing.T) {
	span1 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}
	dataRange1 := NewDataRange(1, span1, 10, 20)
	dataRange2 := NewDataRange(1, span1, 15, 25)

	expectedDataRange := NewDataRange(1, span1, 10, 25)
	// Case 1: Merge two data ranges with the same span , and intersecting timestamps.
	mergedDataRange := dataRange1.Merge(dataRange2)
	require.NotNil(t, mergedDataRange)
	require.Equal(t, expectedDataRange, mergedDataRange)

	// Case 2: Merge two data ranges with the same span, but non-intersecting timestamps.
	dataRange3 := NewDataRange(1, span1, 25, 30)
	expectedDataRange = NewDataRange(1, span1, 10, 30)
	mergedDataRange = dataRange1.Merge(dataRange3)
	require.NotNil(t, mergedDataRange)
	require.Equal(t, expectedDataRange, mergedDataRange)

	// Case 3: Merge two data ranges with different spans.
	span2 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("b"),
			EndKey:   []byte("y"),
		},
	}
	dataRange4 := NewDataRange(1, span2, 10, 20)
	mergedDataRange = dataRange1.Merge(dataRange4)
	require.Nil(t, mergedDataRange)
}
func TestDataRangeEqual(t *testing.T) {
	span1 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}
	span2 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}
	span3 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  2,
			StartKey: []byte("b"),
			EndKey:   []byte("y"),
		},
	}

	dataRange1 := NewDataRange(1, span1, 10, 20)
	dataRange2 := NewDataRange(1, span2, 10, 20)
	dataRange3 := NewDataRange(1, span1, 15, 25)
	dataRange4 := NewDataRange(2, span3, 10, 20)

	require.True(t, dataRange1.Equal(dataRange2))
	require.False(t, dataRange1.Equal(dataRange3))
	require.False(t, dataRange1.Equal(dataRange4))
}
func TestTableSpanLess(t *testing.T) {
	span1 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}
	span2 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  2,
			StartKey: []byte("b"),
			EndKey:   []byte("y"),
		},
	}
	span3 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  2,
			StartKey: []byte("c"),
			EndKey:   []byte("x"),
		},
	}

	require.True(t, span1.Less(span2))
	require.False(t, span2.Less(span1))
	require.True(t, span2.Less(span3))
}
func TestTableSpanEqual(t *testing.T) {
	span1 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}
	span2 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}
	span3 := &TableSpan{
		TableSpan: &heartbeatpb.TableSpan{
			TableID:  2,
			StartKey: []byte("b"),
			EndKey:   []byte("y"),
		},
	}

	require.True(t, span1.Equal(span2))
	require.False(t, span1.Equal(span3))
}
