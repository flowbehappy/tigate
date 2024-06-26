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
	"bytes"
	"fmt"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
	"time"
)

type Maintainer struct {
	id           model.ChangeFeedID
	checkpointTs uint64
	status       scheduler.ComponentStatus

	isStopping bool

	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler
}

func NewMaintainer() *Maintainer {
	return &Maintainer{}
}

func (m *Maintainer) Stop() {
	if !m.isStopping {
		m.isStopping = true
		//todo async stop
	}
}

func (c *Maintainer) checkLiveness() ([]rpc.Message, error) {
	var msgs []rpc.Message
	c.supervisor.GetInferiors().Ascend(
		func(key scheduler.InferiorID, value *scheduler.StateMachine) bool {
			if !value.Inferior.IsAlive() {
				log.Info("found inactive inferior", zap.Any("ID", key))
				c.supervisor.GetInferiors().Delete(key)
				// clean messages
				// trigger schedule task
			}
			return true
		})
	return msgs, nil
}

func (m *Maintainer) scheduleDispatcher() ([]rpc.Message, error) {
	if m.supervisor.CheckAllCaptureInitialized() {
		return nil, nil
	}

	// get all captures
	captures := m.supervisor.GetAllCaptures()
	// get allTableSpans
	var tables []uint64 = nil
	for _, tbl := range tables {
		println(tbl)
	}

	tasks := m.scheduler.Schedule(
		nil,
		captures,
		m.supervisor.GetInferiors(),
	)
	return m.supervisor.HandleScheduleTasks(tasks)
}

func (m *Maintainer) handleMessages() ([]rpc.Message, error) {
	var status []scheduler.InferiorStatus
	m.supervisor.UpdateCaptureStatus("", status)
	m.supervisor.HandleCaptureChanges(nil)
	if m.supervisor.CheckAllCaptureInitialized() {
		m.supervisor.HandleStatus("", status)
	}
	return nil, nil
}

type TableSpan struct {
	TableID  uint64
	StartKey []byte
	EndKey   []byte
}

func (c *TableSpan) String() string {
	return fmt.Sprintf("%d", c.TableID)
}

func (c *TableSpan) Less(b scheduler.InferiorID) bool {
	other := b.(*TableSpan)
	if c.TableID < other.TableID {
		return true
	}
	if bytes.Compare(c.StartKey, other.StartKey) < 0 {
		return true
	}
	return false
}

func (c *TableSpan) Equal(id scheduler.InferiorID) bool {
	other := id.(*TableSpan)
	return c.TableID == other.TableID &&
		bytes.Equal(c.StartKey, other.StartKey) &&
		bytes.Equal(c.EndKey, other.EndKey)
}

type dispatcher struct {
	ID    *TableSpan
	State *TableSpanStatus

	lastHeartBeat time.Time
}

func (c *dispatcher) UpdateStatus(status scheduler.InferiorStatus) {
	c.State = status.(*TableSpanStatus)
	c.lastHeartBeat = time.Now()
}

func (c *dispatcher) GetID() scheduler.InferiorID {
	return c.ID
}

func (c *dispatcher) NewInferiorStatus(status scheduler.ComponentStatus) scheduler.InferiorStatus {
	return &TableSpanStatus{
		ID:     c.ID,
		Status: status,
	}
}

func (c *dispatcher) IsAlive() bool {
	return time.Now().Sub(c.lastHeartBeat) < 10*time.Second
}

func (c *dispatcher) NewAddInferiorMessage(capture model.CaptureID, secondary bool) rpc.Message {
	return nil
}

func (c *dispatcher) NewRemoveInferiorMessage(capture model.CaptureID) rpc.Message {
	return nil
}

type TableSpanStatus struct {
	ID     *TableSpan
	Status scheduler.ComponentStatus
}

func (c *TableSpanStatus) GetInferiorID() scheduler.InferiorID {
	return scheduler.InferiorID(c.ID)
}

func (c *TableSpanStatus) GetInferiorState() scheduler.ComponentStatus {
	return c.Status
}
