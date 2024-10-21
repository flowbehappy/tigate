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

package node

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
)

// Coordinator is the master of the ticdc cluster,
// 1. schedules changefeed maintainer to ticdc watcher
// 2. save changefeed checkpoint ts to etcd
// 3. send checkpoint to downstream
// 4. manager gc safe point
// 5. response for open API call
type Coordinator interface {
	AsyncStop()
	// Run handles messages
	Run(ctx context.Context) error
	// CreateChangefeed creates a new changefeed
	CreateChangefeed(ctx context.Context, info *model.ChangeFeedInfo) error
	// RemoveChangefeed gets a changefeed
	RemoveChangefeed(ctx context.Context, id model.ChangeFeedID) (uint64, error)
	// PauseChangefeed pauses a changefeed
	PauseChangefeed(ctx context.Context, id model.ChangeFeedID) error
	// ResumeChangefeed resumes a changefeed
	ResumeChangefeed(ctx context.Context, id model.ChangeFeedID) error
	// UpdateChangefeed updates a changefeed
	UpdateChangefeed(ctx context.Context, id model.ChangeFeedID, change *model.ChangeFeedInfo) error
}
