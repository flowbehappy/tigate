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
	"context"
	"encoding/hex"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"go.uber.org/zap"
)

const regionWrittenKeyBase = 1

type writeSplitter struct {
	changefeedID      common.ChangeFeedID
	pdAPIClient       pdutil.PDAPIClient
	writeKeyThreshold int
}

type splitRegionsInfo struct {
	RegionCounts []int
	Weights      []uint64
	WriteKeys    []uint64
	Spans        []*heartbeatpb.TableSpan
}

func newWriteSplitter(
	changefeedID common.ChangeFeedID,
	pdAPIClient pdutil.PDAPIClient,
	writeKeyThreshold int,
) *writeSplitter {
	return &writeSplitter{
		changefeedID:      changefeedID,
		pdAPIClient:       pdAPIClient,
		writeKeyThreshold: writeKeyThreshold,
	}
}

func (m *writeSplitter) split(
	ctx context.Context,
	span *heartbeatpb.TableSpan,
	captureNum int,
	maxSpanNum int,
) []*heartbeatpb.TableSpan {
	if m.writeKeyThreshold == 0 {
		return nil
	}
	regions, err := m.pdAPIClient.ScanRegions(ctx, tablepb.Span{
		TableID:  span.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	})
	if err != nil {
		// Skip split.
		log.Warn("scan regions failed, skip split span",
			zap.String("namespace", m.changefeedID.Namespace()),
			zap.String("changefeed", m.changefeedID.Name()),
			zap.String("span", span.String()),
			zap.Error(err))
		return []*heartbeatpb.TableSpan{span}
	}

	spansNum := getSpansNumber(len(regions), captureNum, maxSpanNum)
	if spansNum <= 1 {
		log.Warn("only one capture and the regions number less than"+
			" the maxSpanRegionLimit, skip split span",
			zap.String("namespace", m.changefeedID.Namespace()),
			zap.String("changefeed", m.changefeedID.Name()),
			zap.String("span", span.String()),
			zap.Error(err))
		return []*heartbeatpb.TableSpan{span}
	}

	splitInfo := m.splitRegionsByWrittenKeysV1(span.TableID, regions, spansNum)
	log.Info("split span by written keys",
		zap.String("namespace", m.changefeedID.Namespace()),
		zap.String("changefeed", m.changefeedID.Name()),
		zap.String("span", span.String()),
		zap.Ints("perSpanRegionCounts", splitInfo.RegionCounts),
		zap.Uint64s("weights", splitInfo.Weights),
		zap.Int("spans", len(splitInfo.Spans)),
		zap.Int("totalCaptures", captureNum),
		zap.Int("writeKeyThreshold", m.writeKeyThreshold),
		zap.Int("spanRegionLimit", spanRegionLimit),
		zap.Uint64("baseSpansNum", uint64(spansNum)))

	return splitInfo.Spans
}

// splitRegionsByWrittenKeysV1 tries to split the regions into at least `baseSpansNum` spans,
// each span has approximately the same write weight.
// The algorithm is:
//  1. Sum the written keys of all regions, and normalize the written keys of each region by
//     adding baseline weights (regionWrittenKeyBase) to each region's written keys. Which takes
//     the region number into account.
//  2. Calculate the writeLimitPerSpan.
//  3. Split the table into spans:
//     3.1 If the total write is less than writeKeyThreshold, don't need to split the regions.
//     3.2 If the restSpans count is one, and the restWeight is less than writeLimitPerSpan,
//     we will use the rest regions as the last span. If the restWeight is larger than writeLimitPerSpan,
//     then we need to add more restSpans (restWeight / writeLimitPerSpan) to split the rest regions.
//     3.3 If the restRegions is less than equal to restSpans, then every region will be a span.
//     3.4 If the spanWriteWeight is larger than writeLimitPerSpan or the regionCount is larger
//     than spanRegionLimit, then use the region range from spanStartIndex to i to as a span.
//  4. Return the split result.
func (m *writeSplitter) splitRegionsByWrittenKeysV1(
	tableID int64,
	regions []pdutil.RegionInfo,
	baseSpansNum int,
) *splitRegionsInfo {
	decodeKey := func(hexkey string) []byte {
		key, _ := hex.DecodeString(hexkey)
		return key
	}

	totalWrite, totalWriteNormalized := uint64(0), uint64(0)
	for i := range regions {
		totalWrite += regions[i].WrittenKeys
		regions[i].WrittenKeys += regionWrittenKeyBase
		totalWriteNormalized += regions[i].WrittenKeys
	}

	// 1. If the total write is less than writeKeyThreshold
	// don't need to split the regions
	if totalWrite < uint64(m.writeKeyThreshold) {
		return &splitRegionsInfo{
			RegionCounts: []int{len(regions)},
			Weights:      []uint64{totalWriteNormalized},
			Spans: []*heartbeatpb.TableSpan{{
				TableID:  tableID,
				StartKey: decodeKey(regions[0].StartKey),
				EndKey:   decodeKey(regions[len(regions)-1].EndKey),
			}},
		}
	}

	// calc the spansNum by totalWriteNormalized and writeKeyThreshold?

	// 2. Calculate the writeLimitPerSpan, if one span's write is larger that
	// this number, we should create a new span.
	writeLimitPerSpan := totalWriteNormalized / uint64(baseSpansNum)

	// The result of this method
	var (
		regionCounts = make([]int, 0, baseSpansNum)
		writeKeys    = make([]uint64, 0, baseSpansNum)
		weights      = make([]uint64, 0, baseSpansNum)
		spans        = make([]*heartbeatpb.TableSpan, 0, baseSpansNum)
	)

	// Temp variables used in the loop
	var (
		spanWriteWeight = uint64(0)
		spanStartIndex  = 0
		restSpans       = baseSpansNum
		regionCount     = 0
		restWeight      = int64(totalWriteNormalized)
	)

	// 3. Split the table into spans, each span has approximately
	// `writeWeightPerSpan` weight or `spanRegionLimit` regions.
	for i := 0; i < len(regions); i++ {
		restRegions := len(regions) - i
		regionCount++
		spanWriteWeight += regions[i].WrittenKeys
		// If the restSpans count is one, and the restWeight is less than writeLimitPerSpan,
		// we will use the rest regions as the last span. If the restWeight is larger than writeLimitPerSpan,
		// then we need to add more restSpans (restWeight / writeLimitPerSpan) to split the rest regions.
		if restSpans == 1 {
			if restWeight < int64(writeLimitPerSpan) {
				spans = append(spans, &heartbeatpb.TableSpan{
					TableID:  tableID,
					StartKey: decodeKey(regions[spanStartIndex].StartKey),
					EndKey:   decodeKey(regions[len(regions)-1].EndKey),
				})

				lastSpanRegionCount := len(regions) - spanStartIndex
				lastSpanWriteWeight := uint64(0)
				lastSpanWriteKey := uint64(0)
				for j := spanStartIndex; j < len(regions); j++ {
					lastSpanWriteKey += regions[j].WrittenKeys
					lastSpanWriteWeight += regions[j].WrittenKeys
				}
				regionCounts = append(regionCounts, lastSpanRegionCount)
				weights = append(weights, lastSpanWriteWeight)
				writeKeys = append(writeKeys, lastSpanWriteKey)
				break
			}
			// If the restWeight is larger than writeLimitPerSpan,
			// then we need to update the restSpans.
			restSpans = int(restWeight) / int(writeLimitPerSpan)
		}

		// If the restRegions is less than equal to restSpans,
		// then every region will be a span.
		if restRegions <= restSpans {
			spans = append(spans, &heartbeatpb.TableSpan{
				TableID:  tableID,
				StartKey: decodeKey(regions[spanStartIndex].StartKey),
				EndKey:   decodeKey(regions[i].EndKey),
			})
			regionCounts = append(regionCounts, regionCount)
			weights = append(weights, spanWriteWeight)

			// reset the temp variables to start a new span
			restSpans--
			restWeight -= int64(spanWriteWeight)
			spanWriteWeight = 0
			regionCount = 0
			spanStartIndex = i + 1
			continue
		}

		// If the spanWriteWeight is larger than writeLimitPerSpan or the regionCount
		// is larger than spanRegionLimit, then use the region range from
		// spanStartIndex to i to as a span.
		if spanWriteWeight > writeLimitPerSpan || regionCount >= spanRegionLimit {
			spans = append(spans, &heartbeatpb.TableSpan{
				TableID:  tableID,
				StartKey: decodeKey(regions[spanStartIndex].StartKey),
				EndKey:   decodeKey(regions[i].EndKey),
			})
			regionCounts = append(regionCounts, regionCount)
			weights = append(weights, spanWriteWeight)
			// reset the temp variables to start a new span
			restSpans--
			restWeight -= int64(spanWriteWeight)
			spanWriteWeight = 0
			regionCount = 0
			spanStartIndex = i + 1
		}
	}
	return &splitRegionsInfo{
		RegionCounts: regionCounts,
		Weights:      weights,
		WriteKeys:    writeKeys,
		Spans:        spans,
	}
}
