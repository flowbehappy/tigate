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
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"time"
)

const (
	EventInit = iota
	EventMessage
	EventSchedule
	EventRemove
	EventPeriod
)

type Event struct {
	cfID      string
	eventType int
	message   *messaging.TargetMessage
}

type StreamHandler struct {
}

func (m *StreamHandler) Path(event *Event) string {
	return event.cfID
}

func (m *StreamHandler) Handle(event *Event, dest *Maintainer) (await bool) {
	return dest.Handle(event)
}

type ScheduleEventTask func() (threadpool.TaskStatus, time.Time)

func (f ScheduleEventTask) Execute() (threadpool.TaskStatus, time.Time) {
	return f()
}

func submitScheduledEvent(
	scheduler *threadpool.TaskScheduler,
	stream dynstream.DynamicStream[string, *Event, *Maintainer],
	event *Event,
	scheduleTime time.Time) {
	var task ScheduleEventTask = func() (threadpool.TaskStatus, time.Time) {
		stream.In() <- event
		return threadpool.Done, time.Time{}
	}
	scheduler.Submit(task, threadpool.CPUTask, scheduleTime)
}
