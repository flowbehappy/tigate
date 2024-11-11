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
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/operator"
	"go.uber.org/zap"
)

// Controller is the operator controller, it manages all operators.
// And the Controller is responsible for the execution of the operator.
type Controller struct {
	changefeedDB  *changefeed.ChangefeedDB
	operators     map[common.ChangeFeedID]*operator.OperatorWithTime[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]
	runningQueue  operator.OperatorQueue[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]
	batchSize     int
	messageCenter messaging.MessageCenter
	selfNode      *node.Info
	backend       changefeed.Backend

	lock sync.RWMutex
}

func NewOperatorController(mc messaging.MessageCenter,
	selfNode *node.Info,
	db *changefeed.ChangefeedDB,
	backend changefeed.Backend,
	batchSize int) *Controller {
	oc := &Controller{
		operators:     make(map[common.ChangeFeedID]*operator.OperatorWithTime[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]),
		runningQueue:  make(operator.OperatorQueue[common.ChangeFeedID, *heartbeatpb.MaintainerStatus], 0),
		messageCenter: mc,
		batchSize:     batchSize,
		changefeedDB:  db,
		selfNode:      selfNode,
		backend:       backend,
	}
	return oc
}

// Execute periodically execute the operator
// todo: use a better way to control the execution frequency
func (oc *Controller) Execute() time.Time {
	executedItem := 0
	for {
		r, next := oc.pollQueueingOperator()
		if !next {
			return time.Now().Add(time.Millisecond * 200)
		}
		if r == nil {
			continue
		}

		oc.lock.RLock()
		msg := r.Schedule()
		oc.lock.RUnlock()

		if msg != nil {
			_ = oc.messageCenter.SendCommand(msg)
			log.Info("send command to maintainer",
				zap.String("operator", r.String()))
		}
		executedItem++
		if executedItem >= oc.batchSize {
			return time.Now().Add(time.Millisecond * 50)
		}
	}
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *Controller) AddOperator(op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]) bool {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if pre, ok := oc.operators[op.ID()]; ok {
		log.Info("add operator failed, operator already exists",
			zap.Stringer("operator", op), zap.Stringer("previousOperator", pre.OP))
		return false
	}
	cf := oc.changefeedDB.GetByID(op.ID())
	if cf == nil {
		log.Warn("add operator failed, changefeed not found",
			zap.String("operator", op.String()))
		return false
	}
	oc.pushOperator(op)
	return true
}

// StopChangefeed stop changefeed when the changefeed is stopped/removed.
// if remove is true, it will remove the changefeed from the chagnefeed DB
// if remove is false, it only marks as the changefeed stooped in changefeed DB, so we will not schedule the changefeed again
func (oc *Controller) StopChangefeed(ctx context.Context, cfID common.ChangeFeedID, remove bool) {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	var scheduledNode = oc.changefeedDB.StopByChangefeedID(cfID, remove)
	if scheduledNode == "" {
		log.Info("changefeed is not scheduled, stop maintainer using coordinator node",
			zap.Bool("remove", remove),
			zap.String("changefeed", cfID.Name()))
		scheduledNode = oc.selfNode.ID
	}
	oc.pushStopChangefeedOperator(cfID, scheduledNode, remove)
}

// pushStopChangefeedOperator pushes a stop changefeed operator to the controller.
// it checks if the operator already exists, if exists, it will replace the old one.
// if the old operator is the removing operator, it will skip this operator.
func (oc *Controller) pushStopChangefeedOperator(cfID common.ChangeFeedID, nodeID node.ID, remove bool) {
	op := NewStopChangefeedOperator(cfID, nodeID, oc.selfNode, oc.backend, remove)
	if old, ok := oc.operators[cfID]; ok {
		oldStop, ok := old.OP.(*StopChangefeedOperator)
		if ok {
			if oldStop.removed {
				log.Info("changefeed is in removing progress, skip the stop operator",
					zap.String("changefeed", cfID.Name()))
				return
			}
		}
		log.Info("changefeed is stopped , replace the old one",
			zap.String("changefeed", cfID.Name()),
			zap.String("operator", old.OP.String()))
		old.OP.OnTaskRemoved()
		old.OP.PostFinish()
		old.Removed = true
		delete(oc.operators, old.OP.ID())
	}
	oc.pushOperator(op)
}

func (oc *Controller) UpdateOperatorStatus(id common.ChangeFeedID, from node.ID,
	status *heartbeatpb.MaintainerStatus) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	op, ok := oc.operators[id]
	if ok {
		op.OP.Check(from, status)
	}
}

// OnNodeRemoved is called when a node is offline,
// the controller will mark all maintainers on the node as absent if no operator is handling it,
// then the controller will notify all operators.
func (oc *Controller) OnNodeRemoved(n node.ID) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	for _, cf := range oc.changefeedDB.GetByNodeID(n) {
		_, ok := oc.operators[cf.ID]
		if !ok {
			oc.changefeedDB.MarkMaintainerAbsent(cf)
		}
	}
	for _, op := range oc.operators {
		op.OP.OnNodeRemove(n)
	}
}

// GetOperator returns the operator by id.
func (oc *Controller) GetOperator(id common.ChangeFeedID) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	if op, ok := oc.operators[id]; !ok {
		return nil
	} else {
		return op.OP
	}
}

// HasOperator returns true if the operator with the ChangeFeedDisplayName exists in the controller.
func (oc *Controller) HasOperator(dispName common.ChangeFeedDisplayName) bool {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	for id, _ := range oc.operators {
		if id.DisplayName == dispName {
			return true
		}
	}
	return false
}

// OperatorSize returns the number of operators in the controller.
func (oc *Controller) OperatorSize() int {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return len(oc.operators)
}

// pollQueueingOperator returns the operator need to be executed,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *Controller) pollQueueingOperator() (operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus], bool) {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if oc.runningQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.runningQueue).(*operator.OperatorWithTime[common.ChangeFeedID, *heartbeatpb.MaintainerStatus])
	if item.Removed {
		return nil, true
	}
	op := item.OP
	opID := item.OP.ID()
	// always call the PostFinish method to ensure the operator is cleaned up by itself.
	if op.IsFinished() {
		op.PostFinish()
		item.Removed = true
		delete(oc.operators, opID)
		metrics.CoordinatorFinishedOperatorCount.WithLabelValues(op.Type()).Inc()
		metrics.CoordinatorOperatorDuration.WithLabelValues(op.Type()).Observe(time.Since(item.EnqueueTime).Seconds())
		log.Info("operator finished",
			zap.String("operator", opID.String()),
			zap.String("operator", op.String()))
		return nil, true
	}
	now := time.Now()
	if now.Before(item.Time) {
		heap.Push(&oc.runningQueue, item)
		return nil, false
	}
	// pushes with new notify time.
	item.Time = time.Now().Add(time.Millisecond * 500)
	heap.Push(&oc.runningQueue, item)
	return op, true
}

// pushOperator add an operator to the controller queue.
func (oc *Controller) pushOperator(op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]) {
	log.Info("add operator to running queue",
		zap.String("operator", op.String()))
	opWithTime := operator.NewOperatorWithTime(op, time.Now())
	oc.operators[op.ID()] = opWithTime
	op.Start()
	heap.Push(&oc.runningQueue, opWithTime)
	metrics.CoordinatorCreatedOperatorCount.WithLabelValues(op.Type()).Inc()
}
