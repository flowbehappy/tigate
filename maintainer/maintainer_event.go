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
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
)

const (
	// EventInit initialize the changefeed maintainer
	EventInit = iota
	// EventMessage is triggered when a grpc message received
	EventMessage
	// EventPeriod is triggered periodically, maintainer handle some task in the loop, like resend messages
	EventPeriod
)

// Event identify the Event that maintainer will handle in event-driven loop
type Event struct {
	changefeedID common.ChangeFeedID
	eventType    int
	message      *messaging.TargetMessage
}

func (e Event) IsBatchable() bool {
	return true
}

// SubmitScheduledEvent submits a task to controller pool to send a future event
func SubmitScheduledEvent(
	scheduler threadpool.ThreadPool,
	stream dynstream.DynamicStream[int, common.GID, *Event, *Maintainer, *StreamHandler],
	event *Event,
	scheduleTime time.Time) {
	task := func() time.Time {
		stream.In() <- event
		return time.Time{}
	}
	scheduler.SubmitFunc(task, scheduleTime)
}

// StreamHandler implements the dynstream Handler, no real logic, just forward event to the maintainer
type StreamHandler struct {
}

func NewStreamHandler() *StreamHandler {
	return &StreamHandler{}
}

func (m *StreamHandler) Path(event *Event) common.GID {
	return event.changefeedID.Id
}

func (m *StreamHandler) Handle(dest *Maintainer, events ...*Event) (await bool) {
	if len(events) != 1 {
		// TODO: Support batch
		panic("unexpected event count")
	}
	event := events[0]
	return dest.HandleEvent(event)
}

func (m *StreamHandler) GetSize(event *Event) int                      { return 0 }
func (m *StreamHandler) GetArea(path common.GID, dest *Maintainer) int { return 0 }
func (m *StreamHandler) GetTimestamp(event *Event) dynstream.Timestamp { return 0 }
func (m *StreamHandler) GetType(event *Event) dynstream.EventType      { return dynstream.DefaultEventType }
func (m *StreamHandler) IsPaused(event *Event) bool                    { return false }
func (m *StreamHandler) OnDrop(event *Event)                           {}
