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

package node

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/version"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type ID string

func (s ID) String() string {
	return string(s)
}

func (s ID) GetSize() int64 {
	return int64(len(s))
}

func NewID() ID {
	return ID(uuid.New().String())
}

// Info store in etcd.
type Info struct {
	ID            ID     `json:"id"`
	AdvertiseAddr string `json:"address"`

	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`

	// Epoch represents how many times the node has been restarted.
	Epoch uint64 `json:"epoch"`
}

func NewInfo(addr string, deployPath string) *Info {
	return &Info{
		ID:             NewID(),
		AdvertiseAddr:  addr,
		Version:        version.ReleaseVersion,
		GitHash:        version.GitHash,
		DeployPath:     deployPath,
		StartTimestamp: time.Now().Unix(),
	}
}

// Marshal using json.Marshal.
func (c *Info) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *Info) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(cerror.WrapError(cerror.ErrUnmarshalFailed, err),
		"unmarshal data: %v", data)
}

func CaptureInfoToNodeInfo(captureInfo *model.CaptureInfo) *Info {
	return &Info{
		ID:             ID(captureInfo.ID),
		AdvertiseAddr:  captureInfo.AdvertiseAddr,
		Version:        captureInfo.Version,
		GitHash:        captureInfo.GitHash,
		DeployPath:     captureInfo.DeployPath,
		StartTimestamp: captureInfo.StartTimestamp,
	}
}

func CaptureInfosToNodeInfos(captureInfos map[model.CaptureID]*model.CaptureInfo) map[ID]*Info {
	nodeInfos := make(map[ID]*Info)
	for _, ci := range captureInfos {
		nodeInfos[ID(ci.ID)] = CaptureInfoToNodeInfo(ci)
	}
	return nodeInfos

}
