// Copyright 2022 PingCAP, Inc.
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

package v2

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/version"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// createChangefeed handles create changefeed request,
// it returns the changefeed's changefeedInfo that it just created
// CreateChangefeed creates a changefeed
// @Summary Create changefeed
// @Description create a new changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed body ChangefeedConfig true "changefeed config"
// @Success 200 {object} ChangeFeedInfo
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/changefeeds [post]
func (h *OpenAPIV2) createChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	cfg := &ChangefeedConfig{ReplicaConfig: GetDefaultReplicaConfig()}

	if err := c.BindJSON(&cfg); err != nil {
		_ = c.Error(errors.WrapError(errors.ErrAPIInvalidParam, err))
		return
	}

	// verify sinkURI
	if cfg.SinkURI == "" {
		_ = c.Error(errors.ErrSinkURIInvalid.GenWithStackByArgs(
			"sink_uri is empty, cannot create a changefeed without sink_uri"))
		return
	}

	// verify changefeedID
	if cfg.ID == "" {
		cfg.ID = uuid.New().String()
	}
	if err := model.ValidateChangefeedID(cfg.ID); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack(
			"invalid changefeed_id: %s", cfg.ID))
		return
	}
	if cfg.Namespace == "" {
		cfg.Namespace = model.DefaultNamespace
	}

	ts, logical, err := h.server.GetPdClient().GetTS(ctx)
	if err != nil {
		_ = c.Error(errors.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client"))
		return
	}
	currentTSO := oracle.ComposeTS(ts, logical)
	// verify start ts
	if cfg.StartTs == 0 {
		cfg.StartTs = currentTSO
	} else if cfg.StartTs > currentTSO {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack(
			"invalid start-ts %v, larger than current tso %v", cfg.StartTs, currentTSO))
		return
	}
	// Ensure the start ts is valid in the next 3600 seconds, aka 1 hour
	const ensureTTL = 60 * 60
	if err = gc.EnsureChangefeedStartTsSafety(
		ctx,
		h.server.GetPdClient(),
		h.server.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
		model.ChangeFeedID{Namespace: cfg.Namespace, ID: cfg.ID},
		ensureTTL, cfg.StartTs); err != nil {
		if !errors.ErrStartTsBeforeGC.Equal(err) {
			_ = c.Error(errors.ErrPDEtcdAPIError.Wrap(err))
			return
		}
		_ = c.Error(err)
		return
	}

	// verify target ts
	if cfg.TargetTs > 0 && cfg.TargetTs <= cfg.StartTs {
		_ = c.Error(errors.ErrTargetTsBeforeStartTs.GenWithStackByArgs(
			cfg.TargetTs, cfg.StartTs))
		return
	}

	// fill replicaConfig
	replicaCfg := cfg.ReplicaConfig.ToInternalReplicaConfig()
	// verify replicaConfig
	sinkURIParsed, err := url.Parse(cfg.SinkURI)
	if err != nil {
		_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err))
		return
	}
	err = replicaCfg.ValidateAndAdjust(sinkURIParsed)
	if err != nil {
		_ = c.Error(err)
		return
	}

	pdClient := h.server.GetPdClient()
	info := &config.ChangeFeedInfo{
		UpstreamID:     pdClient.GetClusterID(ctx),
		Namespace:      cfg.Namespace,
		ID:             cfg.ID,
		SinkURI:        cfg.SinkURI,
		CreateTime:     time.Now(),
		StartTs:        cfg.StartTs,
		TargetTs:       cfg.TargetTs,
		Config:         replicaCfg,
		State:          model.StateNormal,
		CreatorVersion: version.ReleaseVersion,
		Epoch:          owner.GenerateChangefeedEpoch(ctx, pdClient),
	}

	needRemoveGCSafePoint := false
	defer func() {
		if !needRemoveGCSafePoint {
			return
		}
		err := gc.UndoEnsureChangefeedStartTsSafety(
			ctx,
			pdClient,
			h.server.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
			model.ChangeFeedID{Namespace: cfg.Namespace, ID: cfg.ID},
		)
		if err != nil {
			_ = c.Error(err)
			return
		}
	}()

	co, err := h.server.GetCoordinator()
	if err != nil {
		needRemoveGCSafePoint = true
		_ = c.Error(err)
		return
	}
	err = co.CreateChangefeed(ctx, info)
	if err != nil {
		needRemoveGCSafePoint = true
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!",
		zap.String("id", info.ID),
		zap.String("changefeed", info.String()))
	c.JSON(http.StatusOK, toAPIModel(info,
		info.StartTs, info.StartTs,
		nil))
}

// listChangeFeeds lists all changgefeeds in cdc cluster
// @Summary List changefeed
// @Description list all changefeeds in cdc cluster
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param state query string false "state"
// @Param namespace query string false "default"
// @Success 200 {array} ChangefeedCommonInfo
// @Failure 500 {object} model.HTTPError
// @Router /api/v2/changefeeds [get]
func (h *OpenAPIV2) listChangeFeeds(c *gin.Context) {
	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}
	changefeeds, statuses, err := co.ListChangefeeds(c)
	if err != nil {
		_ = c.Error(err)
		return
	}
	commonInfos := make([]ChangefeedCommonInfo, 0, len(changefeeds))
	for idx, changefeed := range changefeeds {
		status := statuses[idx]
		var runningErr *model.RunningError
		if changefeed.Error != nil {
			runningErr = changefeed.Error
		} else {
			runningErr = changefeed.Warning
		}
		commonInfos = append(commonInfos, ChangefeedCommonInfo{
			UpstreamID:     changefeed.UpstreamID,
			Namespace:      changefeed.Namespace,
			ID:             changefeed.ID,
			FeedState:      changefeed.State,
			CheckpointTSO:  status.CheckpointTs,
			CheckpointTime: model.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
			RunningError:   runningErr,
		})
	}
	resp := &ListResponse[ChangefeedCommonInfo]{
		Total: len(commonInfos),
		Items: commonInfos,
	}
	c.JSON(http.StatusOK, resp)
}

// verifyTable verify table, return ineligibleTables and EligibleTables.
func (h *OpenAPIV2) verifyTable(c *gin.Context) {
	tables := &Tables{}
	c.JSON(http.StatusOK, tables)
}

// getChangefeed get detailed info of a changefeed
// @Summary Get changefeed
// @Description get detail information of a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param namespace query string false "default"
// @Success 200 {object} ChangeFeedInfo
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id} [get]
func (h *OpenAPIV2) getChangeFeed(c *gin.Context) {
	changefeedID := model.ChangeFeedID{Namespace: model.DefaultNamespace, ID: c.Param(api.APIOpVarChangefeedID)}
	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}
	cfInfo, status, err := co.GetChangefeed(c, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	taskStatus := make([]model.CaptureTaskStatus, 0)
	detail := toAPIModel(cfInfo, status.CheckpointTs,
		status.CheckpointTs, taskStatus)
	c.JSON(http.StatusOK, detail)
}

func toAPIModel(
	info *config.ChangeFeedInfo,
	resolvedTs uint64,
	checkpointTs uint64,
	taskStatus []model.CaptureTaskStatus,
) *ChangeFeedInfo {
	var runningError *RunningError

	// if the state is normal, we shall not return the error info
	// because changefeed will is retrying. errors will confuse the users
	if info.State != model.StateNormal && info.Error != nil {
		runningError = &RunningError{
			Addr:    info.Error.Addr,
			Code:    info.Error.Code,
			Message: info.Error.Message,
		}
	}

	sinkURI, err := util.MaskSinkURI(info.SinkURI)
	if err != nil {
		log.Error("failed to mask sink URI", zap.Error(err))
	}

	apiInfoModel := &ChangeFeedInfo{
		UpstreamID:     info.UpstreamID,
		Namespace:      info.Namespace,
		ID:             info.ID,
		SinkURI:        sinkURI,
		CreateTime:     info.CreateTime,
		StartTs:        info.StartTs,
		TargetTs:       info.TargetTs,
		AdminJobType:   info.AdminJobType,
		Config:         ToAPIReplicaConfig(info.Config),
		State:          info.State,
		Error:          runningError,
		CreatorVersion: info.CreatorVersion,
		CheckpointTs:   checkpointTs,
		ResolvedTs:     resolvedTs,
		CheckpointTime: model.JSONTime(oracle.GetTimeFromTS(checkpointTs)),
		TaskStatus:     taskStatus,
	}
	return apiInfoModel
}

// deleteChangefeed handles delete changefeed request
// @Summary Remove a changefeed
// @Description Remove a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Param namespace query string false "default"
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/changefeeds/{changefeed_id} [delete]
func (h *OpenAPIV2) deleteChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.ChangeFeedID{Namespace: model.DefaultNamespace, ID: c.Param(api.APIOpVarChangefeedID)}
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	coordinator, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}
	cfInfo, _, err := coordinator.GetChangefeed(c, changefeedID)
	if err != nil {
		if errors.ErrChangeFeedNotExists.Equal(err) {
			c.JSON(http.StatusOK, nil)
			return
		}
		_ = c.Error(err)
		return
	}
	checkpointTs, err := coordinator.RemoveChangefeed(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, struct {
		ChangeFeedID string `json:"changefeed_id"`
		CheckpointTs uint64 `json:"checkpoint_ts"`
		SinkURI      string `json:"sink_uri"`
	}{
		ChangeFeedID: changefeedID.ID,
		CheckpointTs: checkpointTs,
		SinkURI:      cfInfo.SinkURI,
	})
}

// pauseChangefeed handles pause changefeed request
// PauseChangefeed pauses a changefeed
// @Summary Pause a changefeed
// @Description Pause a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param namespace query string false "default"
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id}/pause [post]
func (h *OpenAPIV2) pauseChangefeed(c *gin.Context) {
	ctx := c.Request.Context()

	changefeedID := model.ChangeFeedID{Namespace: model.DefaultNamespace, ID: c.Param(api.APIOpVarChangefeedID)}
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	coordinator, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}
	_, _, err = coordinator.GetChangefeed(c, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	err = coordinator.PauseChangefeed(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}

// resumeChangefeed handles resume changefeed request.
// ResumeChangefeed resumes a changefeed
// @Summary Resume a changefeed
// @Description Resume a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Param namespace query string false "default"
// @Param resumeConfig body ResumeChangefeedConfig true "resume config"
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/changefeeds/{changefeed_id}/resume [post]
func (h *OpenAPIV2) resumeChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.ChangeFeedID{Namespace: model.DefaultNamespace, ID: c.Param(api.APIOpVarChangefeedID)}
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	cfg := new(ResumeChangefeedConfig)
	if err := c.BindJSON(&cfg); err != nil {
		_ = c.Error(errors.WrapError(errors.ErrAPIInvalidParam, err))
		return
	}
	coordinator, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}
	_, status, err := coordinator.GetChangefeed(c, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	// If there is no overrideCheckpointTs, then check whether the currentCheckpointTs is smaller than gc safepoint or not.
	newCheckpointTs := status.CheckpointTs
	if cfg.OverwriteCheckpointTs != 0 {
		newCheckpointTs = cfg.OverwriteCheckpointTs
	}

	if err := verifyResumeChangefeedConfig(
		ctx,
		h.server.GetPdClient(),
		h.server.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceResuming),
		changefeedID,
		newCheckpointTs); err != nil {
		_ = c.Error(err)
		return
	}
	needRemoveGCSafePoint := false
	defer func() {
		if !needRemoveGCSafePoint {
			return
		}
		err := gc.UndoEnsureChangefeedStartTsSafety(
			ctx,
			h.server.GetPdClient(),
			h.server.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceResuming),
			changefeedID,
		)
		if err != nil {
			_ = c.Error(err)
			return
		}
	}()
	err = coordinator.ResumeChangefeed(ctx, changefeedID, newCheckpointTs)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}

// updateChangefeed handles update changefeed request,
// it returns the updated changefeedInfo
// Can only update a changefeed's: TargetTs, SinkURI,
// ReplicaConfig, PDAddrs, CAPath, CertPath, KeyPath,
// SyncPointEnabled, SyncPointInterval
// UpdateChangefeed updates a changefeed
// @Summary Update a changefeed
// @Description Update a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param namespace query string false "default"
// @Param changefeedConfig body ChangefeedConfig true "changefeed config"
// @Success 200 {object} ChangeFeedInfo
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id} [put]
func (h *OpenAPIV2) updateChangefeed(c *gin.Context) {
	ctx := c.Request.Context()

	changefeedID := model.ChangeFeedID{Namespace: model.DefaultNamespace, ID: c.Param(api.APIOpVarChangefeedID)}
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	coordinator, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}
	oldCfInfo, status, err := coordinator.GetChangefeed(c, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	switch oldCfInfo.State {
	case model.StateStopped, model.StateFailed:
	default:
		_ = c.Error(
			errors.ErrChangefeedUpdateRefused.GenWithStackByArgs(
				"can only update changefeed config when it is stopped or failed",
			),
		)
		return
	}

	updateCfConfig := &ChangefeedConfig{}
	if err = c.BindJSON(updateCfConfig); err != nil {
		_ = c.Error(errors.WrapError(errors.ErrAPIInvalidParam, err))
		return
	}

	if updateCfConfig.TargetTs != 0 {
		if updateCfConfig.TargetTs <= oldCfInfo.StartTs {
			_ = c.Error(errors.ErrChangefeedUpdateRefused.GenWithStack(
				"can not update target_ts:%d less than start_ts:%d",
				updateCfConfig.TargetTs, oldCfInfo.StartTs))
			return
		}
		oldCfInfo.TargetTs = updateCfConfig.TargetTs
	}
	if updateCfConfig.ReplicaConfig != nil {
		oldCfInfo.Config = updateCfConfig.ReplicaConfig.ToInternalReplicaConfig()
	}
	if updateCfConfig.SinkURI != "" {
		oldCfInfo.SinkURI = updateCfConfig.SinkURI
	}

	// verify changefeed filter
	_, err = filter.NewFilter(oldCfInfo.Config.Filter, "", oldCfInfo.Config.CaseSensitive)
	if err != nil {
		_ = c.Error(errors.ErrChangefeedUpdateRefused.
			GenWithStackByArgs(errors.Cause(err).Error()))
		return
	}
	if err := coordinator.UpdateChangefeed(ctx, oldCfInfo); err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, toAPIModel(oldCfInfo, status.CheckpointTs, status.CheckpointTs, nil))
}

// verifyResumeChangefeedConfig verifies the changefeed config before resuming a changefeed
// overrideCheckpointTs is the checkpointTs of the changefeed that specified by the user.
// or it is the checkpointTs of the changefeed before it is paused.
// we need to check weather the resuming changefeed is gc safe or not.
func verifyResumeChangefeedConfig(
	ctx context.Context,
	pdClient pd.Client,
	gcServiceID string,
	changefeedID model.ChangeFeedID,
	overrideCheckpointTs uint64,
) error {
	if overrideCheckpointTs == 0 {
		return nil
	}

	ts, logical, err := pdClient.GetTS(ctx)
	if err != nil {
		return errors.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
	}
	currentTSO := oracle.ComposeTS(ts, logical)
	if overrideCheckpointTs > currentTSO {
		return errors.ErrAPIInvalidParam.GenWithStack(
			"invalid checkpoint-ts %v, larger than current tso %v", overrideCheckpointTs, currentTSO)
	}

	// 1h is enough for resuming a changefeed.
	gcTTL := int64(60 * 60)
	err = gc.EnsureChangefeedStartTsSafety(
		ctx,
		pdClient,
		gcServiceID,
		changefeedID,
		gcTTL, overrideCheckpointTs)
	if err != nil {
		if !errors.ErrStartTsBeforeGC.Equal(err) {
			return errors.ErrPDEtcdAPIError.Wrap(err)
		}
		return err
	}

	return nil
}
