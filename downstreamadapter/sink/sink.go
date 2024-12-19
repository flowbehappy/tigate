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

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	sinkutil "github.com/pingcap/ticdc/pkg/sink/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
)

type Sink interface {
	SinkType() common.SinkType
	IsNormal() bool

	AddDMLEvent(event *commonEvent.DMLEvent)
	WriteBlockEvent(event commonEvent.BlockEvent) error
	PassBlockEvent(event commonEvent.BlockEvent)
	AddCheckpointTs(ts uint64)

	SetTableSchemaStore(tableSchemaStore *sinkutil.TableSchemaStore)
	Close(removeChangefeed bool) error
}

func NewSink(ctx context.Context, config *config.ChangefeedConfig, changefeedID common.ChangeFeedID, errCh chan error) (Sink, error) {
	sinkURI, err := url.Parse(config.SinkURI)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	scheme := sink.GetScheme(sinkURI)
	switch scheme {
	case sink.MySQLScheme, sink.MySQLSSLScheme, sink.TiDBScheme, sink.TiDBSSLScheme:
		return NewMysqlSink(ctx, changefeedID, 16, config, sinkURI, errCh)
	case sink.KafkaScheme, sink.KafkaSSLScheme:
		return NewKafkaSink(ctx, changefeedID, sinkURI, config.SinkConfig, errCh)
	}
	return nil, nil
}
