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
	"context"
	"net/url"

	"github.com/flowbehappy/tigate/downstreamadapter/sink/types"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/config"
	sinkutil "github.com/flowbehappy/tigate/pkg/sink/util"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
)

type SinkType int

const (
	MysqlSinkType SinkType = iota
	KafkaSinkType
)

type Sink interface {
	AddDMLEvent(event *commonEvent.DMLEvent, tableProgress *types.TableProgress)
	AddBlockEvent(event commonEvent.BlockEvent, tableProgress *types.TableProgress)
	PassBlockEvent(event commonEvent.BlockEvent, tableProgress *types.TableProgress)
	AddCheckpointTs(ts uint64)
	SetTableSchemaStore(tableSchemaStore *sinkutil.TableSchemaStore)
	CheckStartTs(tableId int64, startTs uint64) (int64, error)
	Close() error
	SinkType() SinkType
}

func NewSink(ctx context.Context, config *config.ChangefeedConfig, changefeedID model.ChangeFeedID) (Sink, error) {
	sinkURI, err := url.Parse(config.SinkURI)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	scheme := sink.GetScheme(sinkURI)
	switch scheme {
	case sink.MySQLScheme, sink.MySQLSSLScheme, sink.TiDBScheme, sink.TiDBSSLScheme:
		return NewMysqlSink(changefeedID, 16, config, sinkURI)
	case sink.KafkaScheme, sink.KafkaSSLScheme:
		sink, err := NewKafkaSink(changefeedID, sinkURI, config.SinkConfig)
		if err != nil {
			return nil, err
		}
		return sink, nil
	}
	return nil, nil
}
