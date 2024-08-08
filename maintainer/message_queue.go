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
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
)

// MessageQueue is a size limited queue for caching rpc messages
type MessageQueue struct {
	msgCh  chan *messaging.TargetMessage
	msgBuf []*messaging.TargetMessage

	metricsDiscardMessageCounter prometheus.Counter
	metricsReceiveMessageCounter prometheus.Counter
}

func NewMessageQueue(cfID model.ChangeFeedID, bufSize int) *MessageQueue {
	return &MessageQueue{
		msgCh:  make(chan *messaging.TargetMessage, bufSize),
		msgBuf: make([]*messaging.TargetMessage, bufSize),
		metricsDiscardMessageCounter: metrics.HandleMaintainerRequsetCounter.WithLabelValues(
			cfID.Namespace, cfID.ID, "discard"),
		metricsReceiveMessageCounter: metrics.HandleMaintainerRequsetCounter.WithLabelValues(
			cfID.Namespace, cfID.ID, "receive"),
	}
}

func (m *MessageQueue) Push(ctx context.Context, msg *messaging.TargetMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.msgCh <- msg:
		m.metricsReceiveMessageCounter.Inc()
	default:
		m.metricsDiscardMessageCounter.Inc()
	}
	return nil
}

func (m *MessageQueue) PopMessages() (int, []*messaging.TargetMessage) {
	buf := m.msgBuf
	idx := 0
	for {
		select {
		case msg := <-m.msgCh:
			buf[idx] = msg
			idx++
			if idx >= len(buf) {
				return idx, buf
			}
		default:
			return idx, buf
		}
	}
}
