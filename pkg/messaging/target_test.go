package messaging

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newRemoteMessageTargetForTest(t *testing.T) *remoteMessageTarget {
	localId := NewServerId()
	remoteId := NewServerId()
	cfg := config.NewDefaultMessageCenterConfig()
	receivedMsgCh := make(chan *TargetMessage, 1)
	rt := newRemoteMessageTarget(localId, remoteId, 1, 1, "", receivedMsgCh, receivedMsgCh, cfg)
	return rt
}

func TestRemoteTargetNewMessage(t *testing.T) {
	rt := newRemoteMessageTargetForTest(t)
	defer rt.close()
	b := []byte{1, 2, 3, 4}
	bs := Bytes(b)

	msg := &TargetMessage{
		Type:     TypeBytes,
		Epoch:    rt.messageCenterEpoch,
		Sequence: rt.sendSequence.Load(),
		Message:  &bs,
	}
	msg1 := rt.newMessage(msg)
	require.Equal(t, TypeBytes, IOType(msg1.Type))
	require.Equal(t, rt.messageCenterEpoch, epochType(msg1.Epoch))
	require.Equal(t, uint64(1), rt.sendSequence.Load())
	require.Equal(t, rt.sendSequence.Load(), msg1.Seqnum)

	// Test the second message's sequence number is increased by 1.
	msg2 := rt.newMessage(msg)
	log.Info("msg2", zap.Any("msg2", msg2))
	require.Equal(t, TypeBytes, IOType(msg2.Type))
	require.Equal(t, rt.messageCenterEpoch, epochType(msg2.Epoch))
	require.Equal(t, uint64(2), rt.sendSequence.Load())
	require.Equal(t, rt.sendSequence.Load(), msg2.Seqnum)
}
