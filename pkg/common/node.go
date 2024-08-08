// Copyright 2020 PingCAP, Inc.
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

package common

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// NodeID is the type for node ID
type NodeID = string

// NodeInfo store in etcd.
type NodeInfo struct {
	ID            NodeID `json:"id"`
	AdvertiseAddr string `json:"address"`

	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`

	// Epoch represents how many times the node has been restarted.
	Epoch uint64 `json:"epoch"`
}

// Marshal using json.Marshal.
func (c *NodeInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *NodeInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(cerror.WrapError(cerror.ErrUnmarshalFailed, err),
		"unmarshal data: %v", data)
}

func CaptureInfoToNodeInfo(captureInfo *model.CaptureInfo) *NodeInfo {
	return &NodeInfo{
		ID:             captureInfo.ID,
		AdvertiseAddr:  captureInfo.AdvertiseAddr,
		Version:        captureInfo.Version,
		GitHash:        captureInfo.GitHash,
		DeployPath:     captureInfo.DeployPath,
		StartTimestamp: captureInfo.StartTimestamp,
	}
}

func CaptureInfosToNodeInfos(captureInfos map[model.CaptureID]*model.CaptureInfo) map[NodeID]*NodeInfo {
	nodeInfos := make(map[NodeID]*NodeInfo)
	for _, ci := range captureInfos {
		nodeInfos[ci.ID] = CaptureInfoToNodeInfo(ci)
	}
	return nodeInfos

}
