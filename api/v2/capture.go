// Copyright 2023 PingCAP, Inc.
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
	"net/http"

	"github.com/gin-gonic/gin"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/server/watcher"
)

// listCaptures lists all captures
// @Summary List captures
// @Description list all captures in cdc cluster
// @Tags capture,v2
// @Produce json
// @Success 200 {array} Capture
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/captures [get]
func (h *OpenAPIV2) listCaptures(c *gin.Context) {
	info, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodes := nodeManager.GetAliveNodes()
	captures := make([]Capture, 0, len(nodes))
	for _, c := range nodeManager.GetAliveNodes() {
		captures = append(captures,
			Capture{
				ID:            c.ID.String(),
				IsCoordinator: c.ID == info.ID,
				AdvertiseAddr: c.AdvertiseAddr,
				ClusterID:     h.server.GetEtcdClient().GetClusterID(),
			})
	}
	resp := &ListResponse[Capture]{
		Total: len(captures),
		Items: captures,
	}
	c.JSON(http.StatusOK, resp)
}
