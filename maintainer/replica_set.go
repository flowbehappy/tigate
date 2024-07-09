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
	"github.com/flowbehappy/tigate/common"
	"github.com/flowbehappy/tigate/rpc"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/tiflow/cdc/model"
)

type ReplicaSet struct {
	ID *common.TableSpan

	status *ReplicaSetStatus
}

func NewReplicaSet(id scheduler.InferiorID) scheduler.Inferior {
	r := &ReplicaSet{
		ID: id.(*common.TableSpan),
	}
	return r
}

func (r *ReplicaSet) GetID() scheduler.InferiorID {
	return r.ID
}

func (r *ReplicaSet) UpdateStatus(status scheduler.InferiorStatus) {
	r.status = status.(*ReplicaSetStatus)
}

func (r *ReplicaSet) IsAlive() bool {
	return true
}

func (r *ReplicaSet) NewInferiorStatus(status scheduler.ComponentStatus) scheduler.InferiorStatus {
	return &ReplicaSetStatus{
		ID:     r.ID,
		Status: status,
	}
}

func (r *ReplicaSet) NewAddInferiorMessage(model.CaptureID, bool) rpc.Message {
	return nil
}

func (r *ReplicaSet) NewRemoveInferiorMessage(model.CaptureID) rpc.Message {
	return nil
}

type ReplicaSetStatus struct {
	ID     *common.TableSpan
	Status scheduler.ComponentStatus
}

func (c *ReplicaSetStatus) GetInferiorID() scheduler.InferiorID {
	return scheduler.InferiorID(c.ID)
}

func (c *ReplicaSetStatus) GetInferiorState() scheduler.ComponentStatus {
	return c.Status
}
