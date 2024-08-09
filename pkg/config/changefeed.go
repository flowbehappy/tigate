package config

import "github.com/pingcap/tiflow/pkg/config"

type ChangefeedConfig struct {
	Namespace string `json:"namespace"`
	ID        string `json:"changefeed_id"`
	StartTS   uint64 `json:"start_ts"`
	TargetTS  uint64 `json:"target_ts"`
	SinkURI   string `json:"sink_uri"`
	// timezone used when checking sink uri
	TimeZone string `json:"timezone" default:"system"`
	// if true, force to replicate some ineligible tables
	ForceReplicate bool                 `json:"force_replicate" default:"false"`
	Filter         *config.FilterConfig `toml:"filter" json:"filter"`

	SinkConfig *config.SinkConfig `json:"sink_config"`
}
