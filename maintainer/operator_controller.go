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
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"
)

type OperatorController struct {
	lock      sync.RWMutex
	operators map[common.DispatcherID]Operator
}

func NewOperatorController() *OperatorController {
	oc := &OperatorController{}
	return oc
}

func (oc *OperatorController) OperatorSize() int {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return len(oc.operators)
}

func (oc *OperatorController) AddOperator(op Operator) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	oc.operators[op.ID()] = op
}

func (oc *OperatorController) RemoveOperator(id common.DispatcherID) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	delete(oc.operators, id)
}

func (oc *OperatorController) GetOperator(id common.DispatcherID) Operator {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return oc.operators[id]
}
