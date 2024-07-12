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

package threadpool

import (
	"time"
)

type TaskStatus int

const (
	Success TaskStatus = iota
	Failed
	Running
	Waiting
	IO
	Canceled
)

type Task interface {
	// Await is used by wait reactor, to check whether the task reach the Running / IO status
	Await() TaskStatus
	// Eexcute the task, return the status of the task after execution,
	// you can use timeout to control the maximum time to wait for the task to complete
	Execute(timeout time.Duration) TaskStatus
	// release the resources used by the task
	Release()
	// Get status of the task
	GetStatus() TaskStatus
	// Set status of the task
	SetStatus(status TaskStatus)
	// Cancel the task
	// When the user calls cancel, the current task may still call execute once
	Cancel()
}

// BasicTask is the base struct of Task, it implements the basic functions of Task interface
//type BasicTask struct {
// 	status TaskStatus
// }

// func (t *BasicTask) Await() TaskStatus {
// 	return Failed
// }
// func (t *BasicTask) Execute(timeout time.Duration) TaskStatus {
// 	return Failed
// }
// func (t *BasicTask) Release() {}
// func (t *BasicTask) GetStatus() TaskStatus {
// 	return t.status
// }
// func (t *BasicTask) SetStatus(status TaskStatus) {
// 	t.status = status
// }
// func (t *BasicTask) Cancel() {
// 	t.status = Canceled
// }
