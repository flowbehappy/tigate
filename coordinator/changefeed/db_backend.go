// Copyright 2024 PingCAP, Inc.
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

package changefeed

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ChangefeedMeta struct {
	infoModRevision   int64
	statusModRevision int64

	info   *model.ChangeFeedInfo
	status *model.ChangeFeedStatus
}

func loadAllChangefeeds(ctx context.Context,
	etcdClient etcd.CDCEtcdClient) (map[string]*ChangefeedMeta, error) {
	changefeedPrefix := etcd.NamespacedPrefix(etcdClient.GetClusterID(), model.DefaultNamespace) + "/changefeed"

	resp, err := etcdClient.GetEtcdClient().Get(ctx, changefeedPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfMap := make(map[string]*ChangefeedMeta)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		cfID, isStatus := extractKeySuffix(key)
		meta, ok := cfMap[cfID]
		if !ok {
			meta = &ChangefeedMeta{}
			cfMap[cfID] = meta
		}
		if isStatus {
			status := &model.ChangeFeedStatus{}
			err = status.Unmarshal(kv.Value)
			if err != nil {
				log.Warn("failed to unmarshal change feed status, ignore",
					zap.String("key", key), zap.Error(err))
				continue
			}
			meta.status = status
			meta.statusModRevision = kv.ModRevision
		} else {
			detail := &model.ChangeFeedInfo{}
			err = detail.Unmarshal(kv.Value)
			if err != nil {
				log.Warn("failed to unmarshal change feed info, ignore",
					zap.String("key", key), zap.Error(err))
				continue
			}
			meta.info = detail
			meta.infoModRevision = kv.ModRevision
		}
	}
	// check the invalid cf without info
	for id, meta := range cfMap {
		if meta.info == nil {
			log.Warn("failed to load change feed info, ignore",
				zap.String("id", id))
			delete(cfMap, id)
			continue
		}
		if meta.status == nil {
			log.Warn("failed to load change feed status, add a new one")
			status := &model.ChangeFeedStatus{
				CheckpointTs:      meta.info.StartTs,
				MinTableBarrierTs: meta.info.StartTs,
				AdminJobType:      model.AdminNone,
			}
			data, err := json.Marshal(status)
			if err != nil {
				log.Warn("failed to marshal change feed status, ignore", zap.Error(err))
				delete(cfMap, id)
				continue
			}
			statusRsp, err := etcdClient.GetEtcdClient().Put(ctx, etcd.GetEtcdKeyJob(etcdClient.GetClusterID(),
				model.DefaultChangeFeedID(id)), string(data))
			if err != nil {
				log.Warn("failed to save change feed status, ignore", zap.Error(err))
				delete(cfMap, id)
				continue
			}
			meta.statusModRevision = statusRsp.Header.Revision
			meta.status = status
		}
	}
	return cfMap, nil
}

func CreateChangefeed(ctx context.Context,
	etcdClient etcd.CDCEtcdClient, info *model.ChangeFeedInfo) (*ChangefeedMeta, error) {
	changefeedID := model.DefaultChangeFeedID(info.ID)
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(etcdClient.GetClusterID(), changefeedID)
	infoValue, err := info.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	status := &model.ChangeFeedStatus{
		CheckpointTs:      info.StartTs,
		MinTableBarrierTs: info.StartTs,
		AdminJobType:      model.AdminNone,
	}
	jobValue, err := status.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobKey := etcd.GetEtcdKeyJob(etcdClient.GetClusterID(), changefeedID)

	opsThen := []clientv3.Op{}
	opsThen = append(opsThen, clientv3.OpPut(infoKey, infoValue))
	opsThen = append(opsThen, clientv3.OpPut(jobKey, jobValue))

	resp, err := etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{}, opsThen, []clientv3.Op{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !resp.Succeeded {
		err := errors.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("delete changefeed %s", changefeedID))
		return nil, errors.Trace(err)
	}
	return &ChangefeedMeta{
		infoModRevision:   resp.Header.Revision,
		statusModRevision: resp.Header.Revision,
		info:              info,
		status:            status,
	}, nil
}

func UpdateChangefeed(ctx context.Context, etcdClient etcd.CDCEtcdClient,
	info *model.ChangeFeedInfo, preModVision int64) (int64, error) {
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(etcdClient.GetClusterID(), model.DefaultChangeFeedID(info.ID))
	newStr, err := info.Marshal()
	if err != nil {
		return 0, errors.Trace(err)
	}

	opsThen := []clientv3.Op{}
	opsThen = append(opsThen, clientv3.OpPut(infoKey, newStr))

	putResp, err := etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", preModVision),
	}, opsThen, []clientv3.Op{})
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !putResp.Succeeded {
		err := errors.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("pause changefeed %s", info.ID))
		return 0, errors.Trace(err)
	}
	return putResp.Header.Revision, nil
}

func PauseChangefeed(ctx context.Context, etcdClient etcd.CDCEtcdClient, meta *ChangefeedMeta) error {
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(etcdClient.GetClusterID(), model.DefaultChangeFeedID(meta.info.ID))
	meta.info.State = model.StateStopped
	newStr, err := meta.info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	opsThen := []clientv3.Op{}
	opsThen = append(opsThen, clientv3.OpPut(infoKey, newStr))

	putResp, err := etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", meta.infoModRevision),
	}, opsThen, []clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !putResp.Succeeded {
		err := errors.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("pause changefeed %s", meta.info.ID))
		return errors.Trace(err)
	}
	meta.infoModRevision = putResp.Header.Revision
	return nil
}

func DeleteChangefeed(ctx context.Context, etcdClient etcd.CDCEtcdClient,
	infoModVision int64, statusModVision int64,
	changefeedID model.ChangeFeedID) error {
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(etcdClient.GetClusterID(), changefeedID)
	jobKey := etcd.GetEtcdKeyJob(etcdClient.GetClusterID(), changefeedID)

	opsThen := []clientv3.Op{}
	opsThen = append(opsThen, clientv3.OpDelete(infoKey))
	opsThen = append(opsThen, clientv3.OpDelete(jobKey))
	resp, err := etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", infoModVision),
		clientv3.Compare(clientv3.ModRevision(jobKey), "=", statusModVision),
	}, opsThen, []clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		err := errors.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("delete changefeed %s", changefeedID))
		return errors.Trace(err)
	}
	return nil
}

func ResumeChangefeed(ctx context.Context,
	etcdClient etcd.CDCEtcdClient,
	meta *ChangefeedMeta, newCheckpointTs uint64) error {
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(etcdClient.GetClusterID(), model.DefaultChangeFeedID(meta.info.ID))
	oldState := meta.info.State
	meta.info.State = model.StateNormal
	newStr, err := meta.info.Marshal()
	meta.info.State = oldState

	if err != nil {
		return errors.Trace(err)
	}
	opsThen := []clientv3.Op{
		clientv3.OpPut(infoKey, newStr),
	}
	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", meta.infoModRevision),
	}
	if newCheckpointTs > 0 {
		oldCheckpointTs := meta.status.CheckpointTs
		jobValue, err := meta.status.Marshal()
		meta.status.CheckpointTs = oldCheckpointTs
		if err != nil {
			return errors.Trace(err)
		}
		jobKey := etcd.GetEtcdKeyJob(etcdClient.GetClusterID(), model.DefaultChangeFeedID(meta.info.ID))
		cmps = append(cmps,
			clientv3.Compare(clientv3.ModRevision(jobKey), "=", meta.statusModRevision))
		opsThen = append(opsThen, clientv3.OpPut(jobKey, jobValue))
	}

	putResp, err := etcdClient.GetEtcdClient().Txn(ctx, cmps, opsThen, []clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !putResp.Succeeded {
		err := errors.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("pause changefeed %s", meta.info.ID))
		return errors.Trace(err)
	}
	meta.infoModRevision = putResp.Header.Revision
	meta.info.State = model.StateNormal
	if newCheckpointTs > 0 {
		meta.status.CheckpointTs = newCheckpointTs
		meta.statusModRevision = putResp.Header.Revision
	}
	return nil
}

func UpdateChangefeedStatus(ctx context.Context,
	etcdClient etcd.CDCEtcdClient, metas []*ChangefeedMeta) error {
	opsThen := make([]clientv3.Op, 0, 128)
	cmps := make([]clientv3.Cmp, 0, 128)
	tmpMetas := make([]*ChangefeedMeta, 0, 128)

	txnFunc := func() error {
		putResp, err := etcdClient.GetEtcdClient().Txn(ctx, cmps, opsThen, []clientv3.Op{})
		if err != nil {
			return errors.Trace(err)
		}
		logEtcdOps(opsThen, putResp.Succeeded)
		if !putResp.Succeeded {
			return errors.New("commit failed")
		}
		logEtcdCmps(cmps)
		for _, tm := range tmpMetas {
			tm.statusModRevision = putResp.Header.Revision
		}
		return err
	}
	for _, meta := range metas {
		jobValue, err := meta.status.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		jobKey := etcd.GetEtcdKeyJob(etcdClient.GetClusterID(), model.DefaultChangeFeedID(meta.info.ID))
		cmps = append(cmps,
			clientv3.Compare(clientv3.ModRevision(jobKey), "=", meta.statusModRevision))
		opsThen = append(opsThen, clientv3.OpPut(jobKey, jobValue))
		tmpMetas = append(tmpMetas, meta)
		if len(tmpMetas) >= 128 {
			if err := txnFunc(); err != nil {
				return errors.Trace(err)
			}
			cmps = cmps[:0]
			opsThen = opsThen[:0]
			tmpMetas = tmpMetas[:0]
		}
	}
	if len(tmpMetas) > 0 {
		if err := txnFunc(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// extractKeySuffix extracts the suffix of an etcd key, such as extracting
// "6a6c6dd290bc8732" from /tidb/cdc/cluster/namespace/changefeed/info/6a6c6dd290bc8732
// or from /tidb/cdc/cluster/namespace/changefeed/status/6a6c6dd290bc8732
func extractKeySuffix(key string) (string, bool) {
	subs := strings.Split(key, "/")
	return subs[len(subs)-1], subs[len(subs)-2] == "status"
}

func logEtcdOps(ops []clientv3.Op, committed bool) {
	if committed && (log.GetLevel() != zapcore.DebugLevel || len(ops) == 0) {
		return
	}
	logFn := log.Debug
	if !committed {
		logFn = log.Info
	}
	logFn("[etcd worker] ==========Update State to ETCD==========")
	for _, op := range ops {
		if op.IsDelete() {
			logFn("[etcd worker] delete key", zap.ByteString("key", op.KeyBytes()))
		} else {
			logFn("[etcd worker] put key", zap.ByteString("key", op.KeyBytes()), zap.ByteString("value", op.ValueBytes()))
		}
	}
	logFn("[etcd worker] ============State Commit=============", zap.Bool("committed", committed))
}

func logEtcdCmps(cmps []clientv3.Cmp) {
	log.Info("[etcd worker] ==========Failed Etcd Txn Cmps==========")
	for _, cmp := range cmps {
		cmp := etcdserverpb.Compare(cmp)
		log.Info("[etcd worker] compare",
			zap.String("cmp", cmp.String()))
	}
	log.Info("[etcd worker] ============End Failed Etcd Txn Cmps=============")
}
