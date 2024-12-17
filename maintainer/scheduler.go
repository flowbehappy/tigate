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
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

func NewScheduleController(changefeedID common.ChangeFeedID,
	batchSize int,
	oc *operator.Controller,
	db *replica.ReplicationDB,
	nodeM *watcher.NodeManager,
	balanceInterval time.Duration,
	splitter *split.Splitter,
) *scheduler.Controller {
	schedulers := map[string]scheduler.Scheduler{
		scheduler.BasicScheduler:   scheduler.NewBasicScheduler(changefeedID.String(), batchSize, oc, db, nodeM, oc.NewAddOperator),
		scheduler.BalanceScheduler: scheduler.NewBalanceScheduler(changefeedID.String(), batchSize, oc, db, nodeM, balanceInterval, oc.NewMoveOperator),
	}
	if splitter != nil {
		schedulers[scheduler.SplitScheduler] = newSplitScheduler(changefeedID, batchSize, splitter, oc, db, nodeM)
	}
	return scheduler.NewController(schedulers)
}

// splitScheduler is used to check the split status of all spans
type splitScheduler struct {
	changefeedID common.ChangeFeedID

	splitter     *split.Splitter
	opController *operator.Controller
	db           *replica.ReplicationDB
	nodeManager  *watcher.NodeManager

	maxCheckTime  time.Duration
	checkInterval time.Duration
	lastCheckTime time.Time

	cachedSpans []*replica.HotSpan
}

func newSplitScheduler(
	changefeedID common.ChangeFeedID, batchSize int, splitter *split.Splitter,
	oc *operator.Controller, db *replica.ReplicationDB, nodeManager *watcher.NodeManager,
) *splitScheduler {
	return &splitScheduler{
		changefeedID: changefeedID,
		splitter:     splitter,
		opController: oc,
		db:           db,
		nodeManager:  nodeManager,
		cachedSpans:  make([]*replica.HotSpan, batchSize),

		maxCheckTime:  time.Second * 5,
		checkInterval: time.Second * 120,
	}
}

func (s *splitScheduler) Execute() time.Time {
	if s.splitter == nil {
		return time.Time{}
	}
	if time.Since(s.lastCheckTime) < s.checkInterval {
		return s.lastCheckTime.Add(s.checkInterval)
	}

	log.Info("check split status", zap.String("changefeed", s.changefeedID.Name()),
		zap.String("hotSpans", s.db.GetHotSpanStat()), zap.String("groupDistribution", s.db.GetGroupStat()))

	batch := cap(s.cachedSpans)
	needBreak := false
	for _, group := range s.db.GetGroups() {
		if needBreak || batch <= 0 {
			break
		}

		s.cachedSpans = s.cachedSpans[:0]
		cachedSpans := s.db.GetHotSpansByGroup(group, s.cachedSpans)
		batch -= len(cachedSpans)

		checkedIndex, start := 0, time.Now()
		for ; checkedIndex < len(cachedSpans); checkedIndex++ {
			if time.Since(start) > s.maxCheckTime {
				needBreak = true
				break
			}
			span := cachedSpans[checkedIndex]
			if s.db.GetTaskByID(span.ID) == nil {
				continue
			}
			spans := s.splitter.SplitSpans(context.Background(), span.Span, len(s.nodeManager.GetAliveNodes()), int(span.HintMaxSpanNum))
			if len(spans) > 1 {
				log.Info("split span",
					zap.String("changefeed", s.changefeedID.Name()),
					zap.String("span", span.ID.String()),
					zap.Int("span szie", len(spans)))
				s.opController.AddOperator(operator.NewSplitDispatcherOperator(s.db, span.SpanReplication, span.GetNodeID(), spans))
			}
		}
		s.lastCheckTime = time.Now()
		s.db.ClearHotSpansByGroup(group, cachedSpans[:checkedIndex]...)
	}
	return s.lastCheckTime.Add(s.checkInterval)
}

func (s *splitScheduler) Name() string {
	return scheduler.SplitScheduler
}
