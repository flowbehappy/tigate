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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	MessagingSendMsgCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "send_message_counter",
			Help:      "The counter of messages sent by a message center",
		}, []string{"target", "type"}) // target: its addr, type: event, command

	MessagingReceiveMsgCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "receive_message_counter",
			Help:      "The counter of messages received by a message center",
		}, []string{"target", "type"}) // target: its addr, type: event, command

	MessagingDropMsgCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "drop_message_counter",
			Help:      "The counter of messages dropped by a message center",
		}, []string{"target", "type"}) // target: its addr, type: event, command

	MessagingErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "error_counter",
			Help:      "The counter of errors occurred in a message center",
		}, []string{"target", "type", "message"}) // target: its addr, type: event, command, message: error info
	MessagingStreamGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "stream_gauge",
			Help:      "The gauge of streams in a message center",
		}, []string{"from"}) // target: its addr
)

// InitMetrics registers all metrics used in owner
func InitMessagingMetrics(registry *prometheus.Registry) {
	registry.MustRegister(MessagingSendMsgCounter)
	registry.MustRegister(MessagingReceiveMsgCounter)
	registry.MustRegister(MessagingDropMsgCounter)
	registry.MustRegister(MessagingErrorCounter)
	registry.MustRegister(MessagingStreamGauge)
}
