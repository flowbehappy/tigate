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
	"fmt"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/tiflow/cdc/model"
	"time"
)

type Maintainer struct {
	id           model.ChangeFeedID
	checkpointTs uint64
	status       scheduler.ComponentStatus
}

func NewMaintainer() *Maintainer {
	return &Maintainer{}
}

func (m *Maintainer) Stop() {

}

type TRMaintainerID int

func (c TRMaintainerID) String() string {
	return fmt.Sprintf("%d", int(c))
}
func (c TRMaintainerID) Equal(id scheduler.InferiorID) bool {
	return int(c) == int(id.(TRMaintainerID))
}

func (c TRMaintainerID) Less(id scheduler.InferiorID) bool {
	return int(c) < int(id.(TRMaintainerID))
}

type trMaintainer struct {
	ID    int
	State *TrMaintainerStatus

	Info   *model.ChangeFeedInfo
	Status *model.ChangeFeedStatus

	lastHeartBeat time.Time
}

func (c *trMaintainer) UpdateStatus(status scheduler.InferiorStatus) {
	c.State = status.(*TrMaintainerStatus)
	c.lastHeartBeat = time.Now()
}

func (c *trMaintainer) GetID() scheduler.InferiorID {
	return TRMaintainerID(c.ID)
}

func (c *trMaintainer) NewInferiorStatus(status scheduler.ComponentStatus) scheduler.InferiorStatus {
	return &TrMaintainerStatus{
		ID:     TRMaintainerID(c.ID),
		Status: status,
	}
}

func (c *trMaintainer) IsAlive() bool {
	return time.Now().Sub(c.lastHeartBeat) < 10*time.Second
}

func (c *trMaintainer) NewAddInferiorMessage(capture model.CaptureID, secondary bool) rpc.Message {
	return nil
}

func (c *trMaintainer) NewRemoveInferiorMessage(capture model.CaptureID) rpc.Message {
	return nil
}

type TrMaintainerStatus struct {
	ID     TRMaintainerID
	Status scheduler.ComponentStatus
}

func (c *TrMaintainerStatus) GetInferiorID() scheduler.InferiorID {
	return scheduler.InferiorID(c.ID)
}

func (c *TrMaintainerStatus) GetInferiorState() scheduler.ComponentStatus {
	return c.Status
}
