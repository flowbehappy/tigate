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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/operator"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// Controller is the operator controller, it manages all operators.
// And the Controller is responsible for the execution of the operator.
type Controller struct {
	changefeedID  string
	replicationDB *replica.ReplicationDB
	operators     map[common.DispatcherID]operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]
	runningQueue  operator.OperatorQueue[common.DispatcherID, *heartbeatpb.TableSpanStatus]
	batchSize     int
	messageCenter messaging.MessageCenter

	lock sync.RWMutex
}

func NewOperatorController(changefeedID string,
	mc messaging.MessageCenter,
	db *replica.ReplicationDB,
	batchSize int) *Controller {
	oc := &Controller{
		changefeedID:  changefeedID,
		operators:     make(map[common.DispatcherID]operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]),
		runningQueue:  make(operator.OperatorQueue[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0),
		messageCenter: mc,
		batchSize:     batchSize,
		replicationDB: db,
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
			log.Info("send command to dispatcher",
				zap.String("changefeed", oc.changefeedID),
				zap.String("operator", r.String()))
		}
		executedItem++
		if executedItem >= oc.batchSize {
			return time.Now().Add(time.Millisecond * 50)
		}
	}
}

// RemoveAllTasks remove all tasks, and notify all operators to stop.
// it is only called by the barrier when the changefeed is stopped.
func (oc *Controller) RemoveAllTasks() {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	for _, replicaSet := range oc.replicationDB.TryRemoveAll() {
		oc.removeReplicaSet(NewRemoveDispatcherOperator(oc.replicationDB, replicaSet))
	}
}

// RemoveTasksBySchemaID remove all tasks by schema id.
// it is only by the barrier when the schema is dropped by ddl
func (oc *Controller) RemoveTasksBySchemaID(schemaID int64) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	for _, replicaSet := range oc.replicationDB.TryRemoveBySchemaID(schemaID) {
		oc.removeReplicaSet(NewRemoveDispatcherOperator(oc.replicationDB, replicaSet))
	}
}

// RemoveTasksByTableIDs remove all tasks by table ids.
// it is only called by the barrier when the table is dropped by ddl
func (oc *Controller) RemoveTasksByTableIDs(tables ...int64) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	for _, replicaSet := range oc.replicationDB.TryRemoveByTableIDs(tables...) {
		oc.removeReplicaSet(NewRemoveDispatcherOperator(oc.replicationDB, replicaSet))
	}
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *Controller) AddOperator(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) bool {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if _, ok := oc.operators[op.ID()]; ok {
		log.Info("add operator failed, operator already exists",
			zap.String("changefeed", oc.changefeedID),
			zap.String("operator", op.String()))
		return false
	}
	span := oc.replicationDB.GetTaskByID(op.ID())
	if span == nil {
		log.Warn("add operator failed, span not found",
			zap.String("changefeed", oc.changefeedID),
			zap.String("operator", op.String()))
		return false
	}
	oc.pushOperator(op)
	return true
}

func (oc *Controller) UpdateOperatorStatus(id common.DispatcherID, from node.ID, status *heartbeatpb.TableSpanStatus) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	op, ok := oc.operators[id]
	if ok {
		op.Check(from, status)
	}
}

// OnNodeRemoved is called when a node is offline,
// the controller will mark all spans on the node as absent if no operator is handling it,
// then the controller will notify all operators.
func (oc *Controller) OnNodeRemoved(n node.ID) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	for _, span := range oc.replicationDB.GetTaskByNodeID(n) {
		_, ok := oc.operators[span.ID]
		if !ok {
			oc.replicationDB.MarkSpanAbsent(span)
		}
	}
	for _, op := range oc.operators {
		op.OnNodeRemove(n)
	}
}

// GetOperator returns the operator by id.
func (oc *Controller) GetOperator(id common.DispatcherID) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return oc.operators[id]
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
func (oc *Controller) pollQueueingOperator() (operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], bool) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	if oc.runningQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.runningQueue).(*operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus])
	op := item.OP
	opID := op.ID()
	// always call the PostFinish method to ensure the operator is cleaned up by itself.
	if op.IsFinished() {
		op.PostFinish()
		delete(oc.operators, opID)
		metrics.FinishedOperatorCount.WithLabelValues(model.DefaultNamespace, oc.changefeedID, op.Type()).Inc()
		metrics.OperatorDuration.WithLabelValues(model.DefaultNamespace, oc.changefeedID, op.Type()).Observe(time.Since(item.EnqueueTime).Seconds())
		log.Info("operator finished",
			zap.String("changefeed", oc.changefeedID),
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

// ReplicaSetRemoved if the replica set is removed,
// the controller will remove the operator. add a new operator to the controller.
func (oc *Controller) removeReplicaSet(op *RemoveDispatcherOperator) {
	if old, ok := oc.operators[op.ID()]; ok {
		log.Info("replica set is removed , replace the old one",
			zap.String("changefeed", oc.changefeedID),
			zap.String("replicaset", old.ID().String()),
			zap.String("operator", old.String()))
		old.OnTaskRemoved()
		delete(oc.operators, op.ID())
	}
	oc.pushOperator(op)
}

// pushOperator add an operator to the controller queue.
func (oc *Controller) pushOperator(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	log.Info("add operator to running queue",
		zap.String("changefeed", oc.changefeedID),
		zap.String("operator", op.String()))
	oc.operators[op.ID()] = op
	op.Start()
	heap.Push(&oc.runningQueue, &operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus]{OP: op, Time: time.Now(), EnqueueTime: time.Now()})
	metrics.CreatedOperatorCount.WithLabelValues(model.DefaultNamespace, oc.changefeedID, op.Type()).Inc()
}
