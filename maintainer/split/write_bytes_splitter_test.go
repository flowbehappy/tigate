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
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

func prepareRegionsInfo(writtenKeys []int) ([]pdutil.RegionInfo, map[int][]byte, map[int][]byte) {
	regions := []pdutil.RegionInfo{}
	start := byte('a')
	for i, writtenKey := range writtenKeys {
		regions = append(regions, pdutil.NewTestRegionInfo(uint64(i+2), []byte{start}, []byte{start + 1}, uint64(writtenKey)))
		start++
	}
	startKeys := map[int][]byte{}
	endKeys := map[int][]byte{}
	for _, r := range regions {
		b, _ := hex.DecodeString(r.StartKey)
		startKeys[int(r.ID)] = b
	}
	for _, r := range regions {
		b, _ := hex.DecodeString(r.EndKey)
		endKeys[int(r.ID)] = b
	}
	return regions, startKeys, endKeys
}

func cloneRegions(info []pdutil.RegionInfo) []pdutil.RegionInfo {
	return append([]pdutil.RegionInfo{}, info...)
}

func TestSplitRegionsByWrittenKeysUniform(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	cfID := common.NewChangeFeedIDWithName("test")
	regions, startKeys, endKeys := prepareRegionsInfo(
		[]int{100, 100, 100, 100, 100, 100, 100}) // region id: [2,3,4,5,6,7,8]
	splitter := newWriteSplitter(cfID, nil, 0)
	info := splitter.splitRegionsByWrittenKeysV1(0, cloneRegions(regions), 1)
	re.Len(info.RegionCounts, 1)
	re.EqualValues(7, info.RegionCounts[0])
	re.Len(info.Spans, 1)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[8], info.Spans[0].EndKey)

	info = splitter.splitRegionsByWrittenKeysV1(0, cloneRegions(regions), 2) // [2,3,4,5], [6,7,8]
	re.Len(info.RegionCounts, 2)
	re.EqualValues(4, info.RegionCounts[0])
	re.EqualValues(3, info.RegionCounts[1])
	re.Len(info.Weights, 2)
	re.EqualValues(404, info.Weights[0])
	re.EqualValues(303, info.Weights[1])
	re.Len(info.Spans, 2)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[5], info.Spans[0].EndKey)
	re.EqualValues(startKeys[6], info.Spans[1].StartKey)
	re.EqualValues(endKeys[8], info.Spans[1].EndKey)

	info = splitter.splitRegionsByWrittenKeysV1(0, cloneRegions(regions), 3) // [2,3,4], [5,6,7], [8]
	re.Len(info.RegionCounts, 3)
	re.EqualValues(3, info.RegionCounts[0])
	re.EqualValues(3, info.RegionCounts[1])
	re.EqualValues(1, info.RegionCounts[2])
	re.Len(info.Weights, 3)
	re.EqualValues(303, info.Weights[0])
	re.EqualValues(303, info.Weights[1])
	re.EqualValues(101, info.Weights[2])
	re.Len(info.Spans, 3)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[4], info.Spans[0].EndKey)
	re.EqualValues(startKeys[5], info.Spans[1].StartKey)
	re.EqualValues(endKeys[7], info.Spans[1].EndKey)
	re.EqualValues(startKeys[8], info.Spans[2].StartKey)
	re.EqualValues(endKeys[8], info.Spans[2].EndKey)

	// spans > regions
	for p := 7; p <= 10; p++ {
		info = splitter.splitRegionsByWrittenKeysV1(0, cloneRegions(regions), p)
		re.Len(info.RegionCounts, 7)
		for _, c := range info.RegionCounts {
			re.EqualValues(1, c)
		}
		re.Len(info.Weights, 7)
		for _, w := range info.Weights {
			re.EqualValues(101, w, info)
		}
		re.Len(info.Spans, 7)
		for i, r := range info.Spans {
			re.EqualValues(startKeys[2+i], r.StartKey)
			re.EqualValues(endKeys[2+i], r.EndKey)
		}
	}
}

func TestSplitRegionsByWrittenKeysHotspot1(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Hotspots
	cfID := common.NewChangeFeedIDWithName("test")
	regions, startKeys, endKeys := prepareRegionsInfo(
		[]int{100, 1, 100, 1, 1, 1, 100})
	splitter := newWriteSplitter(cfID, nil, 4)
	info := splitter.splitRegionsByWrittenKeysV1(0, regions, 4) // [2], [3,4], [5,6,7], [8]
	re.Len(info.RegionCounts, 4)
	re.EqualValues(1, info.RegionCounts[0])
	re.EqualValues(2, info.RegionCounts[1])
	re.EqualValues(3, info.RegionCounts[2])
	re.EqualValues(1, info.RegionCounts[3])
	re.Len(info.Weights, 4)
	re.EqualValues(101, info.Weights[0])
	re.EqualValues(103, info.Weights[1])
	re.EqualValues(6, info.Weights[2])
	re.EqualValues(101, info.Weights[3])
	re.Len(info.Spans, 4)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[2], info.Spans[0].EndKey)
	re.EqualValues(startKeys[3], info.Spans[1].StartKey)
	re.EqualValues(endKeys[4], info.Spans[1].EndKey)
	re.EqualValues(startKeys[5], info.Spans[2].StartKey)
	re.EqualValues(endKeys[7], info.Spans[2].EndKey)
	re.EqualValues(startKeys[8], info.Spans[3].StartKey)
	re.EqualValues(endKeys[8], info.Spans[3].EndKey)
}

func TestSplitRegionsByWrittenKeysHotspot2(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Hotspots
	cfID := common.NewChangeFeedIDWithName("test")
	regions, startKeys, endKeys := prepareRegionsInfo(
		[]int{1000, 1, 1, 1, 100, 1, 99})
	splitter := newWriteSplitter(cfID, nil, 4)
	info := splitter.splitRegionsByWrittenKeysV1(0, regions, 4) // [2], [3,4,5,6], [7], [8]
	re.Len(info.Spans, 4)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[2], info.Spans[0].EndKey)
	re.EqualValues(startKeys[3], info.Spans[1].StartKey)
	re.EqualValues(endKeys[6], info.Spans[1].EndKey)
	re.EqualValues(startKeys[7], info.Spans[2].StartKey)
	re.EqualValues(endKeys[7], info.Spans[2].EndKey)
	re.EqualValues(startKeys[8], info.Spans[3].StartKey)
	re.EqualValues(endKeys[8], info.Spans[3].EndKey)
}

func TestSplitRegionsByWrittenKeysCold(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	cfID := common.NewChangeFeedIDWithName("test")
	splitter := newWriteSplitter(cfID, nil, 0)
	baseSpanNum := getSpansNumber(2, 1, defaultMaxSpanNumber)
	require.Equal(t, 3, baseSpanNum)
	regions, startKeys, endKeys := prepareRegionsInfo(make([]int, 7))
	info := splitter.splitRegionsByWrittenKeysV1(0, regions, baseSpanNum) // [2,3,4], [5,6,7], [8]
	re.Len(info.RegionCounts, 3)
	re.EqualValues(3, info.RegionCounts[0], info)
	re.EqualValues(3, info.RegionCounts[1])
	re.EqualValues(1, info.RegionCounts[2])
	re.Len(info.Weights, 3)
	re.EqualValues(3, info.Weights[0])
	re.EqualValues(3, info.Weights[1])
	re.EqualValues(1, info.Weights[2])
	re.Len(info.Spans, 3)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[4], info.Spans[0].EndKey)
	re.EqualValues(startKeys[5], info.Spans[1].StartKey)
	re.EqualValues(endKeys[7], info.Spans[1].EndKey)
	re.EqualValues(startKeys[8], info.Spans[2].StartKey)
	re.EqualValues(endKeys[8], info.Spans[2].EndKey)
}

func TestNotSplitRegionsByWrittenKeysCold(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	cfID := common.NewChangeFeedIDWithName("test")
	splitter := newWriteSplitter(cfID, nil, 1)
	baseSpanNum := getSpansNumber(2, 1, defaultMaxSpanNumber)
	require.Equal(t, 3, baseSpanNum)
	regions, startKeys, endKeys := prepareRegionsInfo(make([]int, 7))
	info := splitter.splitRegionsByWrittenKeysV1(0, regions, baseSpanNum) // [2,3,4,5,6,7,8]
	re.Len(info.RegionCounts, 1)
	re.EqualValues(7, info.RegionCounts[0], info)
	re.Len(info.Weights, 1)
	re.EqualValues(7, info.Weights[0])
	re.Len(info.Spans, 1)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[8], info.Spans[0].EndKey)
}

func TestSplitRegionsByWrittenKeysConfig(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	cfID := common.NewChangeFeedIDWithName("test")
	splitter := newWriteSplitter(cfID, nil, math.MaxInt)
	regions, startKeys, endKeys := prepareRegionsInfo([]int{1, 1, 1, 1, 1, 1, 1})
	info := splitter.splitRegionsByWrittenKeysV1(1, regions, 3) // [2,3,4,5,6,7,8]
	re.Len(info.RegionCounts, 1)
	re.EqualValues(7, info.RegionCounts[0], info)
	re.Len(info.Weights, 1)
	re.EqualValues(14, info.Weights[0])
	re.Len(info.Spans, 1)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[8], info.Spans[0].EndKey)
	re.EqualValues(1, info.Spans[0].TableID)

	splitter.writeKeyThreshold = 0
	spans := splitter.split(context.Background(), &heartbeatpb.TableSpan{}, 3, defaultMaxSpanNumber)
	require.Empty(t, spans)
}

func TestSplitRegionEven(t *testing.T) {
	tblID := model.TableID(1)
	regionCount := 4653 + 1051 + 745 + 9530 + 1
	regions := make([]pdutil.RegionInfo, regionCount)
	for i := 0; i < regionCount; i++ {
		regions[i] = pdutil.RegionInfo{
			ID:          uint64(i),
			StartKey:    "" + strconv.Itoa(i),
			EndKey:      "" + strconv.Itoa(i),
			WrittenKeys: 2,
		}
	}
	cfID := common.NewChangeFeedIDWithName("test")
	splitter := newWriteSplitter(cfID, nil, 4)
	info := splitter.splitRegionsByWrittenKeysV1(tblID, regions, 5)
	require.Len(t, info.RegionCounts, 5)
	require.Len(t, info.Weights, 5)
	for i, w := range info.Weights {
		if i == 4 {
			require.Equal(t, uint64(9576), w, i)
		} else {
			require.Equal(t, uint64(9591), w, i)
		}
	}
}

func TestSpanRegionLimitBase(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	splitter := newWriteSplitter(cfID, nil, 0)
	var regions []pdutil.RegionInfo
	// test spanRegionLimit works
	for i := 0; i < spanRegionLimit*6; i++ {
		regions = append(regions, pdutil.NewTestRegionInfo(uint64(i+9), []byte("f"), []byte("f"), 100))
	}
	captureNum := 2
	spanNum := getSpansNumber(len(regions), captureNum, defaultMaxSpanNumber)
	info := splitter.splitRegionsByWrittenKeysV1(0, cloneRegions(regions), spanNum)
	require.Len(t, info.RegionCounts, spanNum)
	for _, c := range info.RegionCounts {
		require.LessOrEqual(t, float64(c), spanRegionLimit*1.1)
	}
}

func TestSpanRegionLimit(t *testing.T) {
	// Fisher-Yates shuffle algorithm to shuffle the writtenKeys
	// but keep the first preservationRate% of the writtenKeys in the left side of the list
	// to make the writtenKeys more like a hot region list
	shuffle := func(nums []int, preservationRate float64) []int {
		n := len(nums)
		shuffled := make([]int, n)
		copy(shuffled, nums)

		for i := n - 1; i > 0; i-- {
			if rand.Float64() < preservationRate {
				continue
			}
			j := rand.Intn(i + 1)
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		}

		return shuffled
	}

	// total region number
	totalRegionNumbers := spanRegionLimit * 10

	// writtenKeys over 20000 percentage
	percentOver20000 := 1
	// writtenKeys between 5000 and 10000 percentage
	percentBetween5000And10000 := 5

	countOver20000 := (percentOver20000 * totalRegionNumbers) / 100
	countBetween5000And10000 := (percentBetween5000And10000 * totalRegionNumbers) / 100
	countBelow1000 := totalRegionNumbers - countOver20000 - countBetween5000And10000

	// random generate writtenKeys for each region
	var writtenKeys []int

	for i := 0; i < countOver20000; i++ {
		number := rand.Intn(80000) + 20001
		writtenKeys = append(writtenKeys, number)
	}

	for i := 0; i < countBetween5000And10000; i++ {
		number := rand.Intn(5001) + 5000
		writtenKeys = append(writtenKeys, number)
	}

	for i := 0; i < countBelow1000; i++ {
		number := rand.Intn(1000)
		writtenKeys = append(writtenKeys, number)
	}

	// 70% hot written region is in the left side of the region list
	writtenKeys = shuffle(writtenKeys, 0.7)

	cfID := common.NewChangeFeedIDWithName("test")
	splitter := newWriteSplitter(cfID, nil, 0)
	var regions []pdutil.RegionInfo
	// region number is 500,000
	// weight is random between 0 and 40,000
	for i := 0; i < len(writtenKeys); i++ {
		regions = append(
			regions,
			pdutil.NewTestRegionInfo(uint64(i+9), []byte("f"), []byte("f"), uint64(writtenKeys[i])))
	}
	captureNum := 3
	spanNum := getSpansNumber(len(regions), captureNum, defaultMaxSpanNumber)
	info := splitter.splitRegionsByWrittenKeysV1(0, cloneRegions(regions), spanNum)
	require.LessOrEqual(t, spanNum, len(info.RegionCounts))
	for _, c := range info.RegionCounts {
		require.LessOrEqual(t, float64(c), spanRegionLimit*1.1)
	}
}
