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

import "time"

// enum

type TaskStatus int

const (
	Success TaskStatus = iota
	Failed
	Running
	Waiting
	IO
)

type Task interface {
	// 用于检查是否达到了继续推进的条件，返回检查后的状态
	Await() TaskStatus
	// 执行任务，当切换状态后换出，或者超时以后换出
	Execute(timeout time.Duration) TaskStatus
	// 释放资源,后面再看要怎么做吧
	Release()
	// 获取 status 决定任务类型
	GetStatus() TaskStatus
}
