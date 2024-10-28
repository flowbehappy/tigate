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

package scheduler

import (
	"math"

	"github.com/flowbehappy/tigate/pkg/node"
)

// CheckBalanceStatus checks the dispatcher scheduling balance status
// returns the table size need to be moved
func CheckBalanceStatus(nodeTaskSize map[node.ID]int, allNodes map[node.ID]*node.Info) int {
	// add the absent node to the node size map
	for nodeID, _ := range allNodes {
		if _, ok := nodeTaskSize[nodeID]; !ok {
			nodeTaskSize[nodeID] = 0
		}
	}
	totalSize := 0
	for _, ts := range nodeTaskSize {
		totalSize += ts
	}
	upperLimitPerCapture := int(math.Ceil(float64(totalSize) / float64(len(nodeTaskSize))))
	// tables need to be moved
	moveSize := 0
	for _, ts := range nodeTaskSize {
		tableNum2Remove := ts - upperLimitPerCapture
		if tableNum2Remove > 0 {
			moveSize += tableNum2Remove
		}
	}
	return moveSize
}
