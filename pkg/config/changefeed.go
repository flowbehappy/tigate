package config

import (
	"encoding/json"
	"math"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type ChangefeedConfig struct {
	ChangefeedID common.ChangeFeedID `json:"changefeed_id"`
	StartTS      uint64              `json:"start_ts"`
	TargetTS     uint64              `json:"target_ts"`
	SinkURI      string              `json:"sink_uri"`
	// timezone used when checking sink uri
	TimeZone string `json:"timezone" default:"system"`
	// if true, force to replicate some ineligible tables
	ForceReplicate bool          `json:"force_replicate" default:"false"`
	Filter         *FilterConfig `toml:"filter" json:"filter"`
	MemoryQuota    uint64        `toml:"memory-quota" json:"memory-quota"`
	//sync point related
	// TODO:syncPointRetention|default 可以不要吗
	EnableSyncPoint    bool           `json:"enable_sync_point" default:"false"`
	SyncPointInterval  *time.Duration `json:"sync_point_interval" default:"1m"`
	SyncPointRetention *time.Duration `json:"sync_point_retention" default:"24h"`

	SinkConfig *SinkConfig `json:"sink_config"`
}

// ChangeFeedInfo describes the detail of a ChangeFeed
type ChangeFeedInfo struct {
	ChangefeedID common.ChangeFeedID `json:"id"`
	UpstreamID   uint64              `json:"upstream-id"`
	SinkURI      string              `json:"sink-uri"`
	CreateTime   time.Time           `json:"create-time"`
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

// NeedBlockGC returns true if the changefeed need to block the GC safepoint.
// Note: if the changefeed is failed by GC, it should not block the GC safepoint.
func (info *ChangeFeedInfo) NeedBlockGC() bool {
	switch info.State {
	case model.StateNormal, model.StateStopped, model.StatePending, model.StateWarning:
		return true
	case model.StateFailed:
		return !info.isFailedByGC()
	case model.StateFinished, model.StateRemoved:
	default:
	}
	return false
}

func (info *ChangeFeedInfo) isFailedByGC() bool {
	if info.Error == nil {
		log.Panic("changefeed info is not consistent",
			zap.Any("state", info.State), zap.Any("error", info.Error))
	}
	return cerror.IsChangefeedGCFastFailErrorCode(errors.RFCErrorCode(info.Error.Code))
}

// String implements fmt.Stringer interface, but hide some sensitive information
func (info *ChangeFeedInfo) String() (str string) {
	var err error
	str, err = info.Marshal()
	if err != nil {
		log.Error("failed to marshal changefeed info", zap.Error(err))
		return
	}
	clone := new(ChangeFeedInfo)
	err = clone.Unmarshal([]byte(str))
	if err != nil {
		log.Error("failed to unmarshal changefeed info", zap.Error(err))
		return
	}

	clone.SinkURI = util.MaskSensitiveDataInURI(clone.SinkURI)
	if clone.Config != nil {
		clone.Config.MaskSensitiveData()
	}

	str, err = clone.Marshal()
	if err != nil {
		log.Error("failed to marshal changefeed info", zap.Error(err))
	}
	return
}

// GetStartTs returns StartTs if it's specified or using the
// CreateTime of changefeed.
func (info *ChangeFeedInfo) GetStartTs() uint64 {
	if info.StartTs > 0 {
		return info.StartTs
	}

	return oracle.GoTimeToTS(info.CreateTime)
}

// GetCheckpointTs returns CheckpointTs if it's specified in ChangeFeedStatus, otherwise StartTs is returned.
func (info *ChangeFeedInfo) GetCheckpointTs(status *model.ChangeFeedStatus) uint64 {
	if status != nil {
		return status.CheckpointTs
	}
	return info.GetStartTs()
}

// GetTargetTs returns TargetTs if it's specified, otherwise MaxUint64 is returned.
func (info *ChangeFeedInfo) GetTargetTs() uint64 {
	if info.TargetTs > 0 {
		return info.TargetTs
	}
	return uint64(math.MaxUint64)
}

// Marshal returns the json marshal format of a ChangeFeedInfo
func (info *ChangeFeedInfo) Marshal() (string, error) {
	data, err := json.Marshal(info)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *ChangeFeedInfo from json marshal byte slice
func (info *ChangeFeedInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &info)
	if err != nil {
		return errors.Annotatef(
			cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
	}
	return nil
}

// Clone returns a cloned ChangeFeedInfo
func (info *ChangeFeedInfo) Clone() (*ChangeFeedInfo, error) {
	s, err := info.Marshal()
	if err != nil {
		return nil, err
	}
	cloned := new(ChangeFeedInfo)
	err = cloned.Unmarshal([]byte(s))
	return cloned, err
}

// VerifyAndComplete verifies changefeed info and may fill in some fields.
// If a required field is not provided, return an error.
// If some necessary filed is missing but can use a default value, fill in it.
func (info *ChangeFeedInfo) VerifyAndComplete() {
	defaultConfig := GetDefaultReplicaConfig()
	if info.Config.Filter == nil {
		info.Config.Filter = defaultConfig.Filter
	}
	if info.Config.Mounter == nil {
		info.Config.Mounter = defaultConfig.Mounter
	}
	if info.Config.Sink == nil {
		info.Config.Sink = defaultConfig.Sink
	}
	if info.Config.Consistent == nil {
		info.Config.Consistent = defaultConfig.Consistent
	}
	if info.Config.Scheduler == nil {
		info.Config.Scheduler = defaultConfig.Scheduler
	}

	if info.Config.Integrity == nil {
		info.Config.Integrity = defaultConfig.Integrity
	}
	if info.Config.ChangefeedErrorStuckDuration == nil {
		info.Config.ChangefeedErrorStuckDuration = defaultConfig.ChangefeedErrorStuckDuration
	}
	if info.Config.SyncedStatus == nil {
		info.Config.SyncedStatus = defaultConfig.SyncedStatus
	}
	info.RmUnusedFields()
}

// RmUnusedFields removes unnecessary fields based on the downstream type and
// the protocol. Since we utilize a common changefeed configuration template,
// certain fields may not be utilized for certain protocols.
func (info *ChangeFeedInfo) RmUnusedFields() {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Warn(
			"failed to parse the sink uri",
			zap.Error(err),
			zap.Any("sinkUri", info.SinkURI),
		)
		return
	}
	// blackhole is for testing purpose, no need to remove fields
	if sink.IsBlackHoleScheme(uri.Scheme) {
		return
	}
	if !sink.IsMQScheme(uri.Scheme) {
		info.rmMQOnlyFields()
	} else {
		// remove schema registry for MQ downstream with
		// protocol other than avro
		if util.GetOrZero(info.Config.Sink.Protocol) != ProtocolAvro.String() {
			info.Config.Sink.SchemaRegistry = nil
		}
	}

	if !sink.IsStorageScheme(uri.Scheme) {
		info.rmStorageOnlyFields()
	}

	if !sink.IsMySQLCompatibleScheme(uri.Scheme) {
		info.rmDBOnlyFields()
	} else {
		// remove fields only being used by MQ and Storage downstream
		info.Config.Sink.Protocol = nil
		info.Config.Sink.Terminator = nil
	}
}

func (info *ChangeFeedInfo) rmMQOnlyFields() {
	log.Info("since the downstream is not a MQ, remove MQ only fields",
		zap.String("namespace", info.ChangefeedID.Namespace()),
		zap.String("changefeed", info.ChangefeedID.Name()))
	info.Config.Sink.DispatchRules = nil
	info.Config.Sink.SchemaRegistry = nil
	info.Config.Sink.EncoderConcurrency = nil
	info.Config.Sink.EnableKafkaSinkV2 = nil
	info.Config.Sink.OnlyOutputUpdatedColumns = nil
	info.Config.Sink.DeleteOnlyOutputHandleKeyColumns = nil
	info.Config.Sink.ContentCompatible = nil
	info.Config.Sink.KafkaConfig = nil
}

func (info *ChangeFeedInfo) rmStorageOnlyFields() {
	info.Config.Sink.CSVConfig = nil
	info.Config.Sink.DateSeparator = nil
	info.Config.Sink.EnablePartitionSeparator = nil
	info.Config.Sink.FileIndexWidth = nil
	info.Config.Sink.CloudStorageConfig = nil
}

func (info *ChangeFeedInfo) rmDBOnlyFields() {
	info.Config.EnableSyncPoint = nil
	info.Config.BDRMode = nil
	info.Config.SyncPointInterval = nil
	info.Config.SyncPointRetention = nil
	info.Config.Consistent = nil
	info.Config.Sink.SafeMode = nil
	info.Config.Sink.MySQLConfig = nil
}

// FixIncompatible fixes incompatible changefeed meta info.
func (info *ChangeFeedInfo) FixIncompatible() {
	creatorVersionGate := version.NewCreatorVersionGate(info.CreatorVersion)
	if creatorVersionGate.ChangefeedStateFromAdminJob() {
		log.Info("Start fixing incompatible changefeed state", zap.String("changefeed", info.String()))
		info.fixState()
		log.Info("Fix incompatibility changefeed state completed", zap.String("changefeed", info.String()))
	}

	if creatorVersionGate.ChangefeedAcceptUnknownProtocols() {
		log.Info("Start fixing incompatible changefeed MQ sink protocol", zap.String("changefeed", info.String()))
		info.fixMQSinkProtocol()
		log.Info("Fix incompatibility changefeed MQ sink protocol completed", zap.String("changefeed", info.String()))
	}

	if creatorVersionGate.ChangefeedAcceptProtocolInMysqlSinURI() {
		log.Info("Start fixing incompatible changefeed sink uri", zap.String("changefeed", info.String()))
		info.fixMySQLSinkProtocol()
		log.Info("Fix incompatibility changefeed sink uri completed", zap.String("changefeed", info.String()))
	}

	if info.Config.MemoryQuota == uint64(0) {
		log.Info("Start fixing incompatible memory quota", zap.String("changefeed", info.String()))
		info.fixMemoryQuota()
		log.Info("Fix incompatible memory quota completed", zap.String("changefeed", info.String()))
	}

	if info.Config.ChangefeedErrorStuckDuration == nil {
		log.Info("Start fixing incompatible error stuck duration", zap.String("changefeed", info.String()))
		info.Config.ChangefeedErrorStuckDuration = GetDefaultReplicaConfig().ChangefeedErrorStuckDuration
		log.Info("Fix incompatible error stuck duration completed", zap.String("changefeed", info.String()))
	}

	log.Info("Start fixing incompatible scheduler", zap.String("changefeed", info.String()))
	inheritV66 := creatorVersionGate.ChangefeedInheritSchedulerConfigFromV66()
	info.fixScheduler(inheritV66)
	log.Info("Fix incompatible scheduler completed", zap.String("changefeed", info.String()))
}

// fixState attempts to fix state loss from upgrading the old owner to the new owner.
func (info *ChangeFeedInfo) fixState() {
	// Notice: In the old owner we used AdminJobType field to determine if the task was paused or not,
	// we need to handle this field in the new owner.
	// Otherwise, we will see that the old version of the task is paused and then upgraded,
	// and the task is automatically resumed after the upgrade.
	state := info.State
	// Upgrading from an old owner, we need to deal with cases where the state is normal,
	// but actually contains errors and does not match the admin job type.
	if state == model.StateNormal {
		switch info.AdminJobType {
		// This corresponds to the case of failure or error.
		case model.AdminNone, model.AdminResume:
			if info.Error != nil {
				if cerror.IsChangefeedGCFastFailErrorCode(errors.RFCErrorCode(info.Error.Code)) {
					state = model.StateFailed
				} else {
					state = model.StateWarning
				}
			}
		case model.AdminStop:
			state = model.StateStopped
		case model.AdminFinish:
			state = model.StateFinished
		case model.AdminRemove:
			state = model.StateRemoved
		}
	}

	if state != info.State {
		log.Info("handle old owner inconsistent state",
			zap.String("oldState", string(info.State)),
			zap.String("adminJob", info.AdminJobType.String()),
			zap.String("newState", string(state)))
		info.State = state
	}
}

func (info *ChangeFeedInfo) fixMySQLSinkProtocol() {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Warn("parse sink URI failed", zap.Error(err))
		// SAFETY: It is safe to ignore this unresolvable sink URI here,
		// as it is almost impossible for this to happen.
		// If we ignore it when fixing it after it happens,
		// it will expose the problem when starting the changefeed,
		// which is easier to troubleshoot than reporting the error directly in the bootstrap process.
		return
	}

	if sink.IsMQScheme(uri.Scheme) {
		return
	}

	query := uri.Query()
	protocolStr := query.Get(ProtocolKey)
	if protocolStr != "" || info.Config.Sink.Protocol != nil {
		maskedSinkURI, _ := util.MaskSinkURI(info.SinkURI)
		log.Warn("sink URI or sink config contains protocol, but scheme is not mq",
			zap.String("sinkURI", maskedSinkURI),
			zap.String("protocol", protocolStr),
			zap.Any("sinkConfig", info.Config.Sink))
		// always set protocol of mysql sink to ""
		query.Del(ProtocolKey)
		info.updateSinkURIAndConfigProtocol(uri, "", query)
	}
}

func (info *ChangeFeedInfo) fixMQSinkProtocol() {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Warn("parse sink URI failed", zap.Error(err))
		return
	}

	if !sink.IsMQScheme(uri.Scheme) {
		return
	}

	needsFix := func(protocolStr string) bool {
		_, err := ParseSinkProtocolFromString(protocolStr)
		// There are two cases:
		// 1. there is an error indicating that the old ticdc accepts
		//    a protocol that is not known. It needs to be fixed as open protocol.
		// 2. If it is default, then it needs to be fixed as open protocol.
		return err != nil || protocolStr == ProtocolDefault.String()
	}

	query := uri.Query()
	protocol := query.Get(ProtocolKey)
	openProtocol := ProtocolOpen.String()

	// The sinkURI always has a higher priority.
	if protocol != "" && needsFix(protocol) {
		query.Set(ProtocolKey, openProtocol)
		info.updateSinkURIAndConfigProtocol(uri, openProtocol, query)
		return
	}

	if needsFix(util.GetOrZero(info.Config.Sink.Protocol)) {
		log.Info("handle incompatible protocol from sink config",
			zap.String("oldProtocol", util.GetOrZero(info.Config.Sink.Protocol)),
			zap.String("fixedProtocol", openProtocol))
		info.Config.Sink.Protocol = util.AddressOf(openProtocol)
	}
}

func (info *ChangeFeedInfo) updateSinkURIAndConfigProtocol(uri *url.URL, newProtocol string, newQuery url.Values) {
	newRawQuery := newQuery.Encode()
	maskedURI, _ := util.MaskSinkURI(uri.String())
	log.Info("handle incompatible protocol from sink URI",
		zap.String("oldURI", maskedURI),
		zap.String("newProtocol", newProtocol))

	uri.RawQuery = newRawQuery
	fixedSinkURI := uri.String()
	info.SinkURI = fixedSinkURI
	info.Config.Sink.Protocol = util.AddressOf(newProtocol)
}

func (info *ChangeFeedInfo) fixMemoryQuota() {
	info.Config.FixMemoryQuota()
}

func (info *ChangeFeedInfo) fixScheduler(inheritV66 bool) {
	info.Config.FixScheduler(inheritV66)
}
