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

package dispatchermanager

import (
	"sync"
	"sync/atomic"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/common/event"
)

// MemoryController is a struct to control the memory usage of a dispatcher manager.
type MemoryController struct {
	totalMemory     int64
	availableMemory atomic.Int64
	// This is used to record the memory usage of each dispatcher.
	// When a dispatcher is removed, the memory usage of the dispatcher will be released.
	dispatchersUsedMemory sync.Map // dispatcherID -> usedMemory
}

func NewMemoryController(totalMemory int64) *MemoryController {
	mc := &MemoryController{
		totalMemory: totalMemory,
	}
	mc.availableMemory.Store(totalMemory)
	return mc
}

// RegisterEvent registers a DML event to the memory controller.
// It returns false if the memory is not enough.
func (mc *MemoryController) RegisterEvent(dispatcherID common.DispatcherID, dmlEvent *event.DMLEvent) bool {
	current := mc.availableMemory.Load()
	if current < dmlEvent.GetSize() {
		return false
	}
	mc.availableMemory.Add(-dmlEvent.GetSize())
	v, ok := mc.dispatchersUsedMemory.Load(dispatcherID)
	if ok {
		currentUsedMemory := v.(int64)
		mc.dispatchersUsedMemory.Store(dispatcherID, currentUsedMemory+dmlEvent.GetSize())
	} else {
		mc.dispatchersUsedMemory.Store(dispatcherID, dmlEvent.GetSize())
	}
	dmlEvent.AddPostFlushFunc(func() {
		mc.availableMemory.Add(dmlEvent.GetSize())
	})
	return true
}

// ReleaseDispatcher releases the memory taken by the dispatcher.
func (mc *MemoryController) ReleaseDispatcher(dispatcherID common.DispatcherID) {
	v, ok := mc.dispatchersUsedMemory.LoadAndDelete(dispatcherID)
	if ok {
		mc.availableMemory.Add(v.(int64))
	}
}

// AvailableMemory returns the available memory.
func (mc *MemoryController) AvailableMemory() int64 {
	return mc.availableMemory.Load()
}

// Available returns true if the available memory is greater than 25% of the total memory.
func (mc *MemoryController) Available() bool {
	return mc.availableMemory.Load() > mc.totalMemory/4
}
