package messaging

import (
	. "github.com/flowbehappy/tigate/apperror"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type localMessageTarget struct {
	localId ServerId

	// The gather channel from the message center.
	// We only need to push and pull the messages from those channel.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage
}

func newLocalMessageTarget(id ServerId,
	gatherRecvEventChan chan *TargetMessage,
	gatherRecvCmdChan chan *TargetMessage) *localMessageTarget {
	return &localMessageTarget{
		localId:     id,
		recvEventCh: gatherRecvEventChan,
		recvCmdCh:   gatherRecvCmdChan,
	}
}

func (s *localMessageTarget) SendEvent(mtype IOType, eventBytes [][]byte) error {
	return sendMsgToChan(s.localId, mtype, eventBytes, s.recvEventCh)
}

func (s *localMessageTarget) SendCommand(mtype IOType, commandBytes [][]byte) error {
	return sendMsgToChan(s.localId, mtype, commandBytes, s.recvCmdCh)
}

func sendMsgToChan(sid ServerId, mtype IOType, eventBytes [][]byte, ch chan *TargetMessage) error {
	for _, eventBytes := range eventBytes {
		m, err := decodeIOType(mtype, eventBytes)
		if err != nil {
			log.Panic("Deserialize message failed", zap.Error(err))
		}

		message := &TargetMessage{From: sid, To: sid, Type: mtype, Message: m}
		select {
		case ch <- message:
		default:
			return AppError{Type: ErrorTypeMessageCongested, Reason: "Send message is congested"}
		}
	}
	return nil
}
