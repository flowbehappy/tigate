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

package heartbeatpb

import "math"

// UpdateMin updates the watermark with the minimum values of checkpointTs and resolvedTs from another watermark.
func (w *Watermark) UpdateMin(other Watermark) {
	if w.CheckpointTs > other.CheckpointTs {
		w.CheckpointTs = other.CheckpointTs
	}
	if w.ResolvedTs > other.ResolvedTs {
		w.ResolvedTs = other.ResolvedTs
	}
}

func NewMaxWatermark() *Watermark {
	return &Watermark{
		CheckpointTs: math.MaxUint64,
		ResolvedTs:   math.MaxUint64,
	}
}
