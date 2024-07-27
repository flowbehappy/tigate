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

package maintainer

import (
	"context"
	"github.com/flowbehappy/tigate/pkg/messaging"
)

// MessageQueue is a size limited queue for caching rpc messages
type MessageQueue struct {
	msgCh chan *messaging.TargetMessage
}

func NewMessageQueue(bufSize int) *MessageQueue {
	return &MessageQueue{
		msgCh: make(chan *messaging.TargetMessage, bufSize),
	}
}

func (m *MessageQueue) Push(ctx context.Context, msg *messaging.TargetMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.msgCh <- msg:
	}
	return nil
}

func (m *MessageQueue) PopMessages(buf []*messaging.TargetMessage, maxSize int) int {
	idx := 0
	for {
		select {
		case msg := <-m.msgCh:
			buf[idx] = msg
			idx++
			if idx >= maxSize {
				return idx
			}
		default:
			return idx
		}
	}
}
