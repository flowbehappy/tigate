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
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/server/watcher"
)

// BalanceChecker is used to check the balance status of all spans among all nodes
type BalanceChecker struct {
	changefeedID       string
	operatorController *operator.Controller
	replicationDB      *replica.ReplicationDB
	nodeManager        *watcher.NodeManager
}

func NewBalanceChecker(
	changefeedID string,
	oc *operator.Controller,
	db *replica.ReplicationDB,
	nodeManager *watcher.NodeManager) *BalanceChecker {
	return &BalanceChecker{
		changefeedID:       changefeedID,
		operatorController: oc,
		replicationDB:      db,
		nodeManager:        nodeManager,
	}
}

func (b *BalanceChecker) Check() {

}

func (b *BalanceChecker) Name() string {
	return "balance-checker"
}
