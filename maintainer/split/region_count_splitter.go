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
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type regionCountSplitter struct {
	changefeedID    model.ChangeFeedID
	regionCache     RegionCache
	regionThreshold int
}

func newRegionCountSplitter(
	changefeedID model.ChangeFeedID, regionCache RegionCache, regionThreshold int,
) *regionCountSplitter {
	return &regionCountSplitter{
		changefeedID:    changefeedID,
		regionCache:     regionCache,
		regionThreshold: regionThreshold,
	}
}

func (m *regionCountSplitter) split(
	ctx context.Context, span *heartbeatpb.TableSpan, captureNum int,
) []*heartbeatpb.TableSpan {
	bo := tikv.NewBackoffer(ctx, 500)
	regions, err := m.regionCache.ListRegionIDsInKeyRange(bo, span.StartKey, span.EndKey)
	if err != nil {
		log.Warn("list regions failed, skip split span",
			zap.String("changefeed", m.changefeedID.ID),
			zap.String("span", span.String()),
			zap.Error(err))
		return []*heartbeatpb.TableSpan{span}
	}
	if len(regions) <= m.regionThreshold || captureNum == 0 {
		log.Info("skip split span by region count",
			zap.String("changefeed", m.changefeedID.ID),
			zap.String("span", span.String()),
			zap.Int("totalCaptures", captureNum),
			zap.Int("regionCount", len(regions)),
			zap.Int("regionThreshold", m.regionThreshold))
		return []*heartbeatpb.TableSpan{span}
	}

	stepper := newEvenlySplitStepper(
		getSpansNumber(len(regions), captureNum),
		len(regions))

	spans := make([]*heartbeatpb.TableSpan, 0, stepper.SpanCount())
	start, end := 0, stepper.Step()
	for {
		startRegion, err := m.regionCache.LocateRegionByID(bo, regions[start])
		if err != nil {
			log.Warn("schedulerv3: get regions failed, skip split span",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("span", span.String()),
				zap.Error(err))
			return []*heartbeatpb.TableSpan{span}
		}
		endRegion, err := m.regionCache.LocateRegionByID(bo, regions[end-1])
		if err != nil {
			log.Warn("schedulerv3: get regions failed, skip split span",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("span", span.String()),
				zap.Error(err))
			return []*heartbeatpb.TableSpan{span}
		}
		if len(spans) > 0 &&
			bytes.Compare(spans[len(spans)-1].EndKey, startRegion.StartKey) > 0 {
			log.Warn("schedulerv3: list region out of order detected",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("span", span.String()),
				zap.Stringer("lastSpan", spans[len(spans)-1]),
				zap.Stringer("region", startRegion))
			return []*heartbeatpb.TableSpan{span}
		}
		spans = append(spans, &heartbeatpb.TableSpan{
			TableID:  span.TableID,
			StartKey: startRegion.StartKey,
			EndKey:   endRegion.EndKey,
		},
		)

		if end == len(regions) {
			break
		}
		start = end
		step := stepper.Step()
		if end+step < len(regions) {
			end = end + step
		} else {
			end = len(regions)
		}
	}
	// Make sure spans does not exceed [startKey, endKey).
	spans[0].StartKey = span.StartKey
	spans[len(spans)-1].EndKey = span.EndKey
	log.Info("split span by region count",
		zap.String("changefeed", m.changefeedID.ID),
		zap.String("span", span.String()),
		zap.Int("spans", len(spans)),
		zap.Int("totalCaptures", captureNum),
		zap.Int("regionCount", len(regions)),
		zap.Int("regionThreshold", m.regionThreshold),
		zap.Int("spanRegionLimit", spanRegionLimit))
	return spans
}

type evenlySplitStepper struct {
	spanCount          int
	regionPerSpan      int
	extraRegionPerSpan int
	remain             int
}

func newEvenlySplitStepper(pages int, totalRegion int) evenlySplitStepper {
	extraRegionPerSpan := 0
	regionPerSpan, remain := totalRegion/pages, totalRegion%pages
	if regionPerSpan == 0 {
		regionPerSpan = 1
		extraRegionPerSpan = 0
		pages = totalRegion
	} else if remain != 0 {
		// Evenly distributes the remaining regions.
		extraRegionPerSpan = int(math.Ceil(float64(remain) / float64(pages)))
	}
	res := evenlySplitStepper{
		regionPerSpan:      regionPerSpan,
		spanCount:          pages,
		extraRegionPerSpan: extraRegionPerSpan,
		remain:             remain,
	}
	log.Info("evenly split stepper", zap.Any("evenlySplitStepper", res))
	return res
}

func (e *evenlySplitStepper) SpanCount() int {
	return e.spanCount
}

func (e *evenlySplitStepper) Step() int {
	if e.remain <= 0 {
		return e.regionPerSpan
	}
	e.remain = e.remain - e.extraRegionPerSpan
	return e.regionPerSpan + e.extraRegionPerSpan
}

const maxSpanNumber = 100

func getSpansNumber(regionNum, captureNum int) int {
	coefficient := captureNum - 1
	if baseSpanNumberCoefficient > coefficient {
		coefficient = baseSpanNumberCoefficient
	}
	spanNum := 1
	if regionNum > 1 {
		// spanNumber = max(captureNum * coefficient, totalRegions / spanRegionLimit)
		spanNum = captureNum * coefficient
		if regionNum/spanRegionLimit > spanNum {
			spanNum = regionNum / spanRegionLimit
		}
	}
	if spanNum > maxSpanNumber {
		spanNum = maxSpanNumber
	}
	return spanNum
}
