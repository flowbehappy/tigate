package config

import (
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
)

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

	//sync point related
	// TODO:syncPointRetention|default 可以不要吗
	EnableSyncPoint    bool           `json:"enable_sync_point" default:"false"`
	SyncPointInterval  *time.Duration `json:"sync_point_interval" default:"1m"`
	SyncPointRetention *time.Duration `json:"sync_point_retention" default:"24h"`

	SinkConfig *SinkConfig `json:"sink_config"`
}

// ChangeFeedInfo describes the detail of a ChangeFeed
type ChangeFeedInfo struct {
	UpstreamID uint64    `json:"upstream-id"`
	Namespace  string    `json:"namespace"`
	ID         string    `json:"changefeed-id"`
	SinkURI    string    `json:"sink-uri"`
	CreateTime time.Time `json:"create-time"`
	// Start sync at this commit ts if `StartTs` is specify or using the CreateTime of changefeed.
	StartTs uint64 `json:"start-ts"`
	// The ChangeFeed will exits until sync to timestamp TargetTs
	TargetTs uint64 `json:"target-ts"`
	// used for admin job notification, trigger watch event in capture
	AdminJobType model.AdminJobType `json:"admin-job-type"`
	Engine       model.SortEngine   `json:"sort-engine"`
	// SortDir is deprecated
	// it cannot be set by user in changefeed level, any assignment to it should be ignored.
	// but can be fetched for backward compatibility
	SortDir string `json:"sort-dir"`

	Config  *ReplicaConfig      `json:"config"`
	State   model.FeedState     `json:"state"`
	Error   *model.RunningError `json:"error"`
	Warning *model.RunningError `json:"warning"`

	CreatorVersion string `json:"creator-version"`
	// Epoch is the epoch of a changefeed, changes on every restart.
	Epoch uint64 `json:"epoch"`
}
