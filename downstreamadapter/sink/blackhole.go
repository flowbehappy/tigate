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

package sink

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
)

// BlackHoleSink is responsible for writing data to blackhole.
// Including DDL and DML.
type BlackHoleSink struct {
}

func NewBlackHoleSink() (*BlackHoleSink, error) {
	blackholeSink := BlackHoleSink{}
	return &blackholeSink, nil
}

func (s *BlackHoleSink) IsNormal() bool {
	return true
}

func (s *BlackHoleSink) SinkType() common.SinkType {
	return common.BlackHoleSinkType
}

func (s *BlackHoleSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
}

func (s *BlackHoleSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	log.Debug("BlackHoleSink: DML Event", zap.Any("dml", event))
	for _, callback := range event.PostTxnFlushed {
		callback()
	}
}

func (s *BlackHoleSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *BlackHoleSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		e := event.(*commonEvent.DDLEvent)
		for _, callback := range e.PostTxnFlushed {
			callback()
		}
	case commonEvent.TypeSyncPointEvent:
		e := event.(*commonEvent.SyncPointEvent)
		for _, callback := range e.PostTxnFlushed {
			callback()
		}
	default:
		log.Error("unknown event type",
			zap.Any("event", event))
	}
	return nil
}

func (s *BlackHoleSink) AddCheckpointTs(ts uint64) {
}

func (s *BlackHoleSink) GetStartTsList(tableIds []int64, startTsList []int64) ([]int64, error) {
	return []int64{}, nil
}

func (s *BlackHoleSink) Close(removeChangefeed bool) error {
	return nil
}
