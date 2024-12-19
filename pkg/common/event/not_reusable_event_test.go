package event

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestNotReusableEvent(t *testing.T) {
	// 1. Test new event
	dispatcherID := common.NewDispatcherID()
	event := NewNotReusableEvent(dispatcherID)
	require.Equal(t, event.GetType(), TypeNotReusableEvent)
	require.Equal(t, event.GetDispatcherID(), dispatcherID)

	// 2. Test encode and decode
	data, err := event.encode()
	require.NoError(t, err)
	reverseEvent := NotReusableEvent{}
	err = reverseEvent.decode(data)
	require.NoError(t, err)
	require.Equal(t, event, reverseEvent)

	// 3. Test Marshal and Unmarshal
	data, err = event.Marshal()
	require.NoError(t, err)
	reverseEvent = NotReusableEvent{}
	err = reverseEvent.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, event, reverseEvent)

	// 4. Test Other methods
	require.Equal(t, event.GetSize(), int64(len(data)))
	require.Equal(t, event.GetDispatcherID(), dispatcherID)
	require.Equal(t, event.GetCommitTs(), common.Ts(0))
	require.Equal(t, event.GetStartTs(), common.Ts(0))
	require.Equal(t, event.IsPaused(), false)
}
