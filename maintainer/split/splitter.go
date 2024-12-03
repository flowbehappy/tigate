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

package split

import (
	"bytes"
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/utils"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const (
	// spanRegionLimit is the maximum number of regions a span can cover.
	spanRegionLimit = 50000
	// baseSpanNumberCoefficient is the base coefficient that use to
	// multiply the number of captures to get the number of spans.
	baseSpanNumberCoefficient = 3
	// defaultMaxSpanNumber is the maximum number of spans that can be split
	// in single batch.
	defaultMaxSpanNumber = 100
)

// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
type RegionCache interface {
	// ListRegionIDsInKeyRange lists ids of regions in [startKey,endKey].
	ListRegionIDsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regionIDs []uint64, err error)
	// LocateRegionByID searches for the region with ID.
	LocateRegionByID(bo *tikv.Backoffer, regionID uint64) (*tikv.KeyLocation, error)
}

type splitter interface {
	split(
		ctx context.Context, span *heartbeatpb.TableSpan, totalCaptures int, maxSpanNum int,
	) []*heartbeatpb.TableSpan
}

type Splitter struct {
	splitters    []splitter
	changefeedID common.ChangeFeedID
}

// NewSplitter returns a Splitter.
func NewSplitter(
	changefeedID common.ChangeFeedID,
	pdapi pdutil.PDAPIClient,
	regionCache RegionCache,
	config *config.ChangefeedSchedulerConfig,
) *Splitter {
	return &Splitter{
		changefeedID: changefeedID,
		splitters: []splitter{
			// write splitter has the highest priority.
			newWriteSplitter(changefeedID, pdapi, config.WriteKeyThreshold),
			newRegionCountSplitter(changefeedID, regionCache, config.RegionThreshold),
		},
	}
}

func (s *Splitter) SplitSpans(ctx context.Context,
	span *heartbeatpb.TableSpan,
	totalCaptures int,
	maxSpanNum int) []*heartbeatpb.TableSpan {
	spans := []*heartbeatpb.TableSpan{span}
	if maxSpanNum <= 0 {
		maxSpanNum = defaultMaxSpanNumber
	}
	for _, sp := range s.splitters {
		spans = sp.split(ctx, span, totalCaptures, maxSpanNum)
		if len(spans) > 1 {
			return spans
		}
	}
	return spans
}

// FindHoles returns an array of Span that are not covered in the range
func FindHoles(currentSpan utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication], totalSpan *heartbeatpb.TableSpan) []*heartbeatpb.TableSpan {
	lastSpan := &heartbeatpb.TableSpan{
		TableID:  totalSpan.TableID,
		StartKey: totalSpan.StartKey,
		EndKey:   totalSpan.StartKey,
	}
	var holes []*heartbeatpb.TableSpan
	// table span is sorted
	currentSpan.Ascend(func(current *heartbeatpb.TableSpan, _ *replica.SpanReplication) bool {
		ord := bytes.Compare(lastSpan.EndKey, current.StartKey)
		if ord < 0 {
			// Find a hole.
			holes = append(holes, &heartbeatpb.TableSpan{
				TableID:  totalSpan.TableID,
				StartKey: lastSpan.EndKey,
				EndKey:   current.StartKey,
			})
		} else if ord > 0 {
			log.Panic("map is out of order",
				zap.String("lastSpan", lastSpan.String()),
				zap.String("current", current.String()))
		}
		lastSpan = current
		return true
	})
	// Check if there is a hole in the end.
	// the lastSpan not reach the totalSpan end
	if !bytes.Equal(lastSpan.EndKey, totalSpan.EndKey) {
		holes = append(holes, &heartbeatpb.TableSpan{
			TableID:  totalSpan.TableID,
			StartKey: lastSpan.EndKey,
			EndKey:   totalSpan.EndKey,
		})
	}
	return holes
}
