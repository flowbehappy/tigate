package messaging

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/node"

	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newRemoteMessageTargetForTest() *remoteMessageTarget {
	localId := node.NewID()
	remoteId := node.NewID()
	cfg := config.NewDefaultMessageCenterConfig()
	receivedMsgCh := make(chan *TargetMessage, 1)
	rt := newRemoteMessageTarget(localId, remoteId, 1, 1, "", receivedMsgCh, receivedMsgCh, cfg)
	return rt
}

func TestRemoteTargetNewMessage(t *testing.T) {
	rt := newRemoteMessageTargetForTest()
	defer rt.close()

	msg := &TargetMessage{
		Type:  TypeMessageHandShake,
		Epoch: rt.messageCenterEpoch,
	}
	msg1 := rt.newMessage(msg)
	require.Equal(t, TypeMessageHandShake, IOType(msg1.Type))
	require.Equal(t, rt.messageCenterEpoch, uint64(msg1.Epoch))

	msg2 := rt.newMessage(msg)
	log.Info("msg2", zap.Any("msg2", msg2))
	require.Equal(t, TypeMessageHandShake, IOType(msg2.Type))
	require.Equal(t, rt.messageCenterEpoch, uint64(msg2.Epoch))
}
