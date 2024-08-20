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
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	EventInit = iota
	EventRemove
	EventMessage
	EventSchedule
)

type Event struct {
	cfID      string
	eventType int
	message   *messaging.TargetMessage
}

type MaintainerStream struct {
	stream dynstream.DynamicStream[string, *Event, *Maintainer]
}

func NewMaintainerStream() *MaintainerStream {
	s := &MaintainerStream{}
	s.stream = dynstream.NewDynamicStreamDefault[string, *Event, *Maintainer](s)
	s.stream.Start()
	return s
}

func (m *MaintainerStream) Path(event *Event) string {
	return event.cfID
}

func (m *MaintainerStream) Handle(event *Event, dest *Maintainer) (await bool) {
	switch event.eventType {
	case EventInit:
		// async initialize the changefeed
		go func() {
			err := dest.initChangefeed()
			if err != nil {
				m.stream.RemovePath(event.cfID)
			}
			dest.state = heartbeatpb.ComponentState_Working
			m.stream.Wake() <- event.cfID
			// ok , let's schedule
			m.stream.In() <- &Event{cfID: event.cfID, eventType: EventSchedule}
		}()
		return true
	case EventMessage:
		if err := dest.onMessage(event.message); err != nil {
			log.Warn("maintainer handle message error",
				zap.Any("event", event),
				zap.Error(err))
		}
	case EventSchedule:
		if err := dest.scheduleTableSpan(); err != nil {
			log.Warn("maintainer schedule table span failed",
				zap.Error(err))
		}
		// reschedule
	}
	return false
}
