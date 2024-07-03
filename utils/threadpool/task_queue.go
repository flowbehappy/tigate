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

/*
做一个基础的 接口，必须要满足的功能包括了：
 1. 拿出一个 task
 2. 塞进去一个 task
 3. 达到 finish 状态释放没有跑的 task 资源
*/
type TaskQueue interface {
	Take() (bool, *Task)
	Submit(task *Task)
	Finish()
}
