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
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/tiflow/cdc/model"
)

// Maintainer is response for handle changefeed replication tasks, Maintainer should:
// 1. schedules tables to ticdc node
// 2. calculate changefeed checkpoint ts
// 3. send changefeed status to coordinator
// 4. handle heartbeat reported by dispatcher
type Maintainer struct {
	id         model.ChangeFeedID
	supervisor *scheduler.Supervisor
	scheduler  scheduler.Scheduler
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer() *Maintainer {
	return &Maintainer{}
}
