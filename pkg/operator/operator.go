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

package operator

import (
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
)

// Operator is the interface for the coordinator schedule maintainer
// operator thread run Start -> Schedule -> PostFinish
// Check, OnNodeRemove and OnTaskRemoved is called by the other thread when some event is triggered
type Operator[T comparable, S any] interface {
	// ID returns the ID
	ID() T
	// Type returns the operator type
	Type() string
	// Start is called when the operator is added to the operator executing queue
	Start()
	// Schedule schedules this operator returns the message to be sent to the remote node
	Schedule() *messaging.TargetMessage
	// IsFinished returns true if the operator is finished
	IsFinished() bool
	// PostFinish is called after the operator is finished and before remove from the task tracker
	// it is called with the lock of the operator controller
	// this is used to:
	// 1. do some cleanup work
	// 2. update the task and related status
	// 3. revert some modifies if the operator is canceled
	PostFinish()
	// Check checks when the new status comes, returns true if the operator is finished
	// It is called by when the node reported a new status
	Check(from node.ID, status S)
	// OnNodeRemove is called when node offline
	OnNodeRemove(node.ID)
	// AffectedNodes returns the nodes that are affected by this operator
	AffectedNodes() []node.ID
	// OnTaskRemoved is called when the task is removed
	OnTaskRemoved()
	// String returns the string representation of the operator
	String() string
}
