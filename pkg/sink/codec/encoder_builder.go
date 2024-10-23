package codec

import (
	"context"

	"github.com/flowbehappy/tigate/pkg/sink/codec/canal"
	"github.com/flowbehappy/tigate/pkg/sink/codec/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec/encoder"
	"github.com/flowbehappy/tigate/pkg/sink/codec/open"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

func NewEventEncoder(ctx context.Context, cfg *common.Config) (encoder.EventEncoder, error) {
	switch cfg.Protocol {
	case config.ProtocolDefault, config.ProtocolOpen:
		return open.NewBatchEncoder(ctx, cfg)
	case config.ProtocolCanal:
		return canal.NewBatchEncoder(cfg)
	// case config.ProtocolAvro:
	// 	return avro.NewAvroEncoder(ctx, cfg)
	// case config.ProtocolCanalJSON:
	// 	return canal.NewJSONRowEventEncoder(ctx, cfg)
	// case config.ProtocolCraft:
	// 	return craft.NewBatchEncoder(cfg), nil
	// case config.ProtocolDebezium:
	// 	return debezium.NewBatchEncoder(cfg, config.GetGlobalServerConfig().ClusterID), nil
	// case config.ProtocolSimple:
	// 	return simple.NewEncoder(ctx, cfg)
	default:
		return nil, cerror.ErrSinkUnknownProtocol.GenWithStackByArgs(cfg.Protocol)
	}
}
