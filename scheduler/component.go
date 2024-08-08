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

package scheduler

import (
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/rpc"
	"github.com/flowbehappy/tigate/utils"
	"github.com/pingcap/tiflow/cdc/model"
)

type Inferior interface {
	GetID() InferiorID
	UpdateStatus(InferiorStatus)
	SetStateMachine(*StateMachine)
	GetStateMachine() *StateMachine
	IsAlive() bool
	NewInferiorStatus(heartbeatpb.ComponentState) InferiorStatus
	NewAddInferiorMessage(model.CaptureID, bool) rpc.Message
	NewRemoveInferiorMessage(model.CaptureID) rpc.Message
}

type InferiorID interface {
	utils.MapKey
	Equal(t any) bool
	String() string
}

type InferiorStatus interface {
	GetInferiorID() InferiorID
	GetInferiorState() heartbeatpb.ComponentState
}

type ChangefeedID model.ChangeFeedID

func (c ChangefeedID) Less(t any) bool {
	cf := t.(ChangefeedID)
	return c.ID < cf.ID
}
func (c ChangefeedID) Equal(t any) bool {
	cf := t.(ChangefeedID)
	return c.ID == cf.ID
}
func (c ChangefeedID) String() string {
	return c.ID
}

type NewBootstrapFn func(id model.CaptureID) rpc.Message
