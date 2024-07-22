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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/flowbehappy/tigate/version"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	cdcapi "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	cfg := &cdcapi.ChangefeedConfig{ReplicaConfig: cdcapi.GetDefaultReplicaConfig()}

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
	info := &model.ChangeFeedInfo{
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
	upstreamInfo := &model.UpstreamInfo{
		ID:            info.UpstreamID,
		PDEndpoints:   strings.Join(cfg.PDAddrs, ","),
		KeyPath:       cfg.KeyPath,
		CertPath:      cfg.CertPath,
		CAPath:        cfg.CAPath,
		CertAllowedCN: cfg.CertAllowedCN,
	}

	err = h.server.GetEtcdClient().CreateChangefeedInfo(ctx, upstreamInfo, info)
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
	changefeeds, err := h.server.GetEtcdClient().GetAllChangeFeedInfo(c)
	if err != nil {
		_ = c.Error(err)
		return
	}
	commonInfos := make([]cdcapi.ChangefeedCommonInfo, len(changefeeds))
	for id, changefeed := range changefeeds {
		status, _, err := h.server.GetEtcdClient().GetChangeFeedStatus(c, id)
		if err != nil {
			log.Warn("failed to load status", zap.String("id", id.String()), zap.Error(err))
		}
		var runningErr *model.RunningError
		if changefeed.Error != nil {
			runningErr = changefeed.Error
		} else {
			runningErr = changefeed.Warning
		}
		commonInfos = append(commonInfos, cdcapi.ChangefeedCommonInfo{
			UpstreamID:     changefeed.UpstreamID,
			Namespace:      changefeed.Namespace,
			ID:             changefeed.ID,
			FeedState:      changefeed.State,
			CheckpointTSO:  status.CheckpointTs,
			CheckpointTime: model.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
			RunningError:   runningErr,
		})
	}
	resp := &cdcapi.ListResponse[cdcapi.ChangefeedCommonInfo]{
		Total: len(commonInfos),
		Items: commonInfos,
	}
	c.JSON(http.StatusOK, resp)
}

// verifyTable verify table, return ineligibleTables and EligibleTables.
func (h *OpenAPIV2) verifyTable(c *gin.Context) {
	tables := &cdcapi.Tables{}
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
	ctx := c.Request.Context()
	changefeedID := model.ChangeFeedID{Namespace: model.DefaultNamespace, ID: c.Param(api.APIOpVarChangefeedID)}
	cfInfo, err := h.server.GetEtcdClient().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	status, _, err := h.server.GetEtcdClient().GetChangeFeedStatus(ctx, changefeedID)
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
	info *model.ChangeFeedInfo,
	resolvedTs uint64,
	checkpointTs uint64,
	taskStatus []model.CaptureTaskStatus,
) *cdcapi.ChangeFeedInfo {
	var runningError *cdcapi.RunningError

	// if the state is normal, we shall not return the error info
	// because changefeed will is retrying. errors will confuse the users
	if info.State != model.StateNormal && info.Error != nil {
		runningError = &cdcapi.RunningError{
			Addr:    info.Error.Addr,
			Code:    info.Error.Code,
			Message: info.Error.Message,
		}
	}

	sinkURI, err := util.MaskSinkURI(info.SinkURI)
	if err != nil {
		log.Error("failed to mask sink URI", zap.Error(err))
	}

	apiInfoModel := &cdcapi.ChangeFeedInfo{
		UpstreamID:     info.UpstreamID,
		Namespace:      info.Namespace,
		ID:             info.ID,
		SinkURI:        sinkURI,
		CreateTime:     info.CreateTime,
		StartTs:        info.StartTs,
		TargetTs:       info.TargetTs,
		AdminJobType:   info.AdminJobType,
		Config:         cdcapi.ToAPIReplicaConfig(info.Config),
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

	etcdCli := h.server.GetEtcdClient()
	cfInfo, err := etcdCli.GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		if errors.ErrChangeFeedNotExists.Equal(err) {
			c.JSON(http.StatusOK, nil)
			return
		}
		_ = c.Error(err)
		return
	}

	status, _, err := etcdCli.GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	infoKey := etcd.GetEtcdKeyChangeFeedInfo(etcdCli.GetClusterID(), changefeedID)
	jobKey := etcd.GetEtcdKeyJob(etcdCli.GetClusterID(), changefeedID)

	opsThen := []clientv3.Op{}
	opsThen = append(opsThen, clientv3.OpDelete(infoKey))
	opsThen = append(opsThen, clientv3.OpDelete(jobKey))

	resp, err := etcdCli.GetEtcdClient().Txn(ctx, []clientv3.Cmp{}, opsThen, []clientv3.Op{})
	if !resp.Succeeded {
		err := errors.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("delete changefeed %s", changefeedID))
		_ = c.Error(err)
		return
	}

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
		CheckpointTs: status.CheckpointTs,
		SinkURI:      cfInfo.SinkURI,
	})
}
