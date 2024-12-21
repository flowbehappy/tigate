// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"time"

	"github.com/pingcap/ticdc/pkg/scheduler/replica"
)

type OperatorWithTime[T replica.ReplicationID, S replica.ReplicationStatus] struct {
	OP          Operator[T, S]
	Time        time.Time
	EnqueueTime time.Time
	Removed     bool
}

func NewOperatorWithTime[T replica.ReplicationID, S replica.ReplicationStatus](op Operator[T, S], time time.Time) *OperatorWithTime[T, S] {
	return &OperatorWithTime[T, S]{OP: op, Time: time, EnqueueTime: time}
}

type OperatorQueue[T replica.ReplicationID, S replica.ReplicationStatus] []*OperatorWithTime[T, S]

func (opn OperatorQueue[T, S]) Len() int { return len(opn) }

func (opn OperatorQueue[T, S]) Less(i, j int) bool {
	return opn[i].Time.Before(opn[j].Time)
}

func (opn OperatorQueue[T, S]) Swap(i, j int) {
	opn[i], opn[j] = opn[j], opn[i]
}

func (opn *OperatorQueue[T, S]) Push(x interface{}) {
	item := x.(*OperatorWithTime[T, S])
	*opn = append(*opn, item)
}

func (opn *OperatorQueue[T, S]) Pop() interface{} {
	old := *opn
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	*opn = old[0 : n-1]
	return item
}
