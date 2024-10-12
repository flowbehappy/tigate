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

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
)

type OperatorController struct {
	lock            sync.RWMutex
	operators       map[common.DispatcherID]Operator
	opNotifierQueue operatorQueue
	batchSize       int
	mc              messaging.MessageCenter
}

func NewOperatorController(mc messaging.MessageCenter, batchSize int) *OperatorController {
	oc := &OperatorController{
		operators:       make(map[common.DispatcherID]Operator),
		opNotifierQueue: make(operatorQueue, 0),
		mc:              mc,
		batchSize:       batchSize,
	}
	return oc
}

func (oc *OperatorController) OperatorSize() int {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return len(oc.operators)
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *OperatorController) AddOperator(op Operator) bool {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if _, ok := oc.operators[op.ID()]; ok {
		return false
	}
	oc.operators[op.ID()] = op
	heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op, time: time.Time{}})
	return true
}

func (oc *OperatorController) ReplaceOperator(op Operator) {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if old, ok := oc.operators[op.ID()]; ok {
		old.OnTaskRemoved()
		delete(oc.operators, op.ID())
	}
	oc.operators[op.ID()] = op
}

func (oc *OperatorController) GetOperator(id common.DispatcherID) Operator {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return oc.operators[id]
}

func (oc *OperatorController) OnNodeRemoved(n node.ID) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	for _, op := range oc.operators {
		op.OnNodeRemove(n)
	}
}

// Execute periodically execute the operator
func (oc *OperatorController) Execute() time.Time {
	executedItem := 0
	for {
		r, next := oc.pollQueueingOperator()
		if !next {
			return time.Now().Add(time.Millisecond * 200)
		}
		if r == nil {
			continue
		}

		if r.IsFinished() {
			oc.lock.Lock()
			r.PostFinished()
			delete(oc.operators, r.ID())
			oc.lock.Unlock()
		}
		msg := r.SchedulerMessage()
		if msg != nil {
			_ = oc.mc.SendCommand(msg)
		}
		executedItem++
		if executedItem >= oc.batchSize {
			return time.Now().Add(time.Millisecond * 50)
		}
	}
}

// pollQueueingOperator returns the operator need to be executed,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *OperatorController) pollQueueingOperator() (Operator, bool) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	if oc.opNotifierQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.opNotifierQueue).(*operatorWithTime)
	opID := item.op.ID()
	op, ok := oc.operators[opID]
	if !ok || op == nil {
		return nil, true
	}
	now := time.Now()
	if now.Before(item.time) {
		heap.Push(&oc.opNotifierQueue, item)
		return nil, false
	}
	// pushes with new notify time.
	item.time = time.Now().Add(time.Millisecond * 500)
	heap.Push(&oc.opNotifierQueue, item)
	return op, true
}
