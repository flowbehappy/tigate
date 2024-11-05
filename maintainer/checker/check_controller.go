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
	"time"

	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/server/watcher"
)

// Controller is the controller of all checkers, it will periodically execute all checkers
type Controller struct {
	batchSize          int
	changefeedID       common.ChangeFeedID
	operatorController *operator.Controller
	replicationDB      *replica.ReplicationDB
	nodeManager        *watcher.NodeManager

	maxTimePerRound time.Duration
	checkers        []Checker
	checkedIndex    int
}

func NewController(changefeedID common.ChangeFeedID,
	splitter *split.Splitter,
	oc *operator.Controller,
	db *replica.ReplicationDB,
	nodeManager *watcher.NodeManager) *Controller {
	c := &Controller{
		changefeedID:       changefeedID,
		operatorController: oc,
		replicationDB:      db,
		nodeManager:        nodeManager,
		maxTimePerRound:    time.Second * 5,
	}
	c.checkers = []Checker{
		NewSplitChecker(changefeedID, splitter, oc, db, nodeManager),
		NewBalanceChecker(changefeedID, oc, db, nodeManager),
	}
	return c
}

// Execute periodically execute the operator
func (ctl *Controller) Execute() time.Time {
	now := time.Now()
	for {
		if ctl.checkedIndex >= len(ctl.checkers) {
			ctl.checkedIndex = 0
			break
		}
		checker := ctl.checkers[ctl.checkedIndex]
		checker.Check()
		ctl.checkedIndex++

		if time.Since(now) > ctl.maxTimePerRound {
			ctl.checkedIndex = ctl.checkedIndex % len(ctl.checkers)
			break
		}
	}
	return time.Now().Add(time.Second * 5)
}
