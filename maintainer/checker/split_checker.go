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

package checker

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// SplitChecker is used to check the split status of all spans
type SplitChecker struct {
	changefeedID string
	splitter     *split.Splitter
	opController *operator.Controller
	db           *replica.ReplicationDB
	nodeManager  *watcher.NodeManager

	maxCheckTime  time.Duration
	checkInterval time.Duration
	lastCheckTime time.Time

	checkedIndex int
	cachedSpans  []*replica.SpanReplication
}

func NewSplitChecker(
	changefeedID string,
	splitter *split.Splitter,
	opController *operator.Controller,
	db *replica.ReplicationDB,
	nodeManager *watcher.NodeManager) *SplitChecker {
	return &SplitChecker{
		changefeedID: changefeedID,
		splitter:     splitter,
		opController: opController,
		db:           db,
		nodeManager:  nodeManager,

		maxCheckTime:  time.Second * 5,
		checkInterval: time.Second * 120,
	}
}

func (s *SplitChecker) Name() string {
	return "split-checker"
}

func (s *SplitChecker) Check() {
	if s.splitter == nil {
		return
	}
	if time.Since(s.lastCheckTime) < s.checkInterval {
		return
	}
	if s.cachedSpans == nil {
		s.cachedSpans = s.db.GetReplicating()
		s.checkedIndex = 0
	}
	start := time.Now()
	for ; s.checkedIndex < len(s.cachedSpans); s.checkedIndex++ {
		span := s.cachedSpans[s.checkedIndex]
		if s.db.GetTaskByID(span.ID) == nil {
			continue
		}
		spans := s.splitter.SplitSpans(context.Background(), span.Span, len(s.nodeManager.GetAliveNodes()))
		if len(spans) > 1 {
			log.Info("split span",
				zap.String("changefeed", s.changefeedID),
				zap.String("span", span.ID.String()),
				zap.Int("span szie", len(spans)))
			s.opController.AddOperator(operator.NewSplitDispatcherOperator(s.db, span, span.GetNodeID(), spans))
		}
		if time.Since(start) > s.maxCheckTime {
			break
		}
	}
	if s.checkedIndex >= len(s.cachedSpans) {
		s.cachedSpans = nil
		s.checkedIndex = 0
		s.lastCheckTime = time.Now()
	}
}
