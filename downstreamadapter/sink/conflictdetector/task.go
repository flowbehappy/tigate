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

package conflictdetector

import (
	"github.com/flowbehappy/tigate/utils/threadpool"

	"github.com/ngaut/log"
)

// 这个 task 就是用来检测是否 resolve 了，resolve 了就推进，没 resolve 了就结束
type ResolveCheckTask struct {
	node *Node
}

func newResolveCheckTask(node *Node) *ResolveCheckTask {
	return &ResolveCheckTask{
		node: node,
	}
}

func (t *ResolveCheckTask) execute() threadpool.TaskStatus {
	t.node.maybeResolve()
	return threadpool.Success
}

func (t *ResolveCheckTask) await() threadpool.TaskStatus {
	log.Warn("ResolveCheckTask should not call await()")
	return threadpool.Failed
}

func (t *ResolveCheckTask) release() {
	// TODO:
}
