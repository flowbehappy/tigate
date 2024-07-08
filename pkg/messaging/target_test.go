package messaging

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/config"
)

func newRemoteMessageTargetForTest(t *testing.T) *remoteMessageTarget {
	localId := NewServerId()
	remoteId := NewServerId()
	cfg := config.NewDefaultMessageCenterConfig()
	rt := newRemoteMessageTarget(localId, remoteId, 1, cfg)
	return rt
}

// func TestRemoteTargetNewMessage(t *testing.T) {
// 	rt := newRemoteMessageTargetForTest(t)
// 	defer rt.close()
// 	b := []byte{1, 2, 3, 4}
// 	bs := [][]byte{b}
// 	msg1 := rt.newMessage(TypeBytes, bs)
// 	require.Equal(t, TypeBytes, IOType(msg1.Type))
// 	require.Equal(t, rt.epoch, msg1.Epoch)
// 	require.Equal(t, uint64(1), rt.sequence.Load())
// 	require.Equal(t, rt.sequence.Load(), msg1.Seqnum)

// 	// Test the second message's sequence number is increased by 1.
// 	msg2 := rt.newMessage(TypeDDLEvent, bs)
// 	log.Info("msg2", zap.Any("msg2", msg2))
// 	require.Equal(t, TypeDDLEvent, IOType(msg2.Type))
// 	require.Equal(t, rt.epoch, msg2.Epoch)
// 	require.Equal(t, uint64(2), rt.sequence.Load())
// 	require.Equal(t, rt.sequence.Load(), msg2.Seqnum)
// }
