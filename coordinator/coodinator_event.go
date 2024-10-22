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

package coordinator

import (
	"github.com/flowbehappy/tigate/pkg/messaging"
)

const (
	// EventMessage is triggered when a grpc message received
	EventMessage = iota
	// EventPeriod is triggered periodically, coordinator handle some task in the loop, like resend messages
	EventPeriod
)

type Event struct {
	eventType int
	message   *messaging.TargetMessage
}

func (e Event) IsBatchable() bool {
	return true
}

// StreamHandler implements the dynstream Handler, no real logic, just forward event
type StreamHandler struct {
}

func NewStreamHandler() *StreamHandler {
	return &StreamHandler{}
}

func (m *StreamHandler) Path(event *Event) string {
	return "coordinator"
}

func (m *StreamHandler) Handle(dest *Controller, events ...*Event) bool {
	if len(events) != 1 {
		// TODO: Support batch
		panic("unexpected event count")
	}
	event := events[0]
	return dest.HandleEvent(event)
}
