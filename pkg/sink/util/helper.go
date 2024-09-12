package util

import (
	"net/url"

	ticonfig "github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
)

// GetEncoderConfig returns the encoder config and validates the config.
func GetEncoderConfig(
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	protocol config.Protocol,
	sinkConfig *ticonfig.SinkConfig,
	maxMsgBytes int,
) (*common.Config, error) {
	encoderConfig := common.NewConfig(protocol)
	if err := encoderConfig.Apply(sinkURI, sinkConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}
	// Always set encoder's `MaxMessageBytes` equal to producer's `MaxMessageBytes`
	// to prevent that the encoder generate batched message too large
	// then cause producer meet `message too large`.
	encoderConfig = encoderConfig.
		WithMaxMessageBytes(maxMsgBytes).
		WithChangefeedID(changefeedID)

	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}
	encoderConfig.TimeZone = tz

	if err := encoderConfig.Validate(); err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}

	return encoderConfig, nil
}
