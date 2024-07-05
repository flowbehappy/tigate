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

import "context"

// Task is the execution unit of the Maintainer, Maintainer is a long-running task that submitted to
// the thread pool,
// when there is task needed to be executed by the Maintainer, it goes to the Maintainer's taskCh first
// then the thread pool call the Execute method of Maintainer, Maintainer take a task from the  chanel,
type Task interface {
	// Execute the tasks, return error if execute failed
	Execute(ctx context.Context) error
}
