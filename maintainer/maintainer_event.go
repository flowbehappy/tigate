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

	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
)

const (
	// EventInit initialize the changefeed maintainer
	EventInit = iota
	// EventMessage is triggered when a grpc message received
	EventMessage
	// EventSchedule is triggered when manually schedule tasks
	EventSchedule
	// EventRemove is triggered when track the removing process, it will be triggered again if the removing is not completed
	EventRemove
	// EventPeriod is triggered periodically, maintainer handle some task in the loop, like resend messages
	EventPeriod
)

// Event identify the Event that maintainer will handle in event-driven loop
type Event struct {
	changefeedID string
	eventType    int
	message      *messaging.TargetMessage
}

// SubmitScheduledEvent submits a task to scheduler pool to send a future event
func SubmitScheduledEvent(
	scheduler threadpool.ThreadPool,
	stream dynstream.DynamicStream[string, *Event, *Maintainer],
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

func (m *StreamHandler) Path(event *Event) string {
	return event.changefeedID
}

func (m *StreamHandler) Handle(event *Event, dest *Maintainer) (await bool) {
	return dest.HandleEvent(event)
}
