package messaging

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/stretchr/testify/require"
)

func newRemoteMessageTargetForTest(t *testing.T) *remoteMessageTarget {
	localId := NewServerId()
	remoteId := NewServerId()
	cfg := config.NewDefaultMessageServerConfig()
	rt := newRemoteMessageTarget(localId, remoteId, 1, cfg)
	return rt
}

func TestRemoteTargetNewMessage(t *testing.T) {
	rt := newRemoteMessageTargetForTest(t)
	defer rt.close()
	b := []byte{1, 2, 3, 4}
	bs := [][]byte{b}
	msg1 := rt.newMessage(TypeBytes, bs)
	require.Equal(t, TypeBytes, msg1.Type)
	require.Equal(t, rt.epoch, msg1.Seqnum)
	require.Equal(t, 1, rt.sequence.Load())
	require.Equal(t, rt.sequence.Load(), msg1.Seqnum)

	msg2 := rt.newMessage(TypeDDLEvent, bs)
	require.Equal(t, TypeBytes, msg2.Type)
	require.Equal(t, rt.epoch, msg2.Seqnum)
	require.Equal(t, 2, rt.sequence.Load())
	require.Equal(t, rt.sequence.Load(), msg2.Seqnum)

}
