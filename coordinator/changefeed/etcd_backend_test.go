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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	mock_etcd "github.com/pingcap/ticdc/pkg/etcd/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestGetAllChangefeeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	cdcClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient := mock_etcd.NewMockClient(ctrl)
	cdcClient.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cdcClient.EXPECT().GetClusterID().Return("test-cluster-id").AnyTimes()

	// get changefeeds failed
	backend := NewEtcdBackend(cdcClient)
	etcdClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("get key failed")).
		Times(1)
	resp, err := backend.GetAllChangefeeds(context.Background())
	require.Nil(t, resp)
	require.NotNil(t, err)

	// info unmarshal failed, changefeed will be ignored
	etcdClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
			{Key: []byte("/tidb/cdc/default/default/changefeed/info/test"), Value: []byte("invalid json")},
			{Key: []byte("/tidb/cdc/default/default/changefeed/status/test"), Value: []byte("{}")},
		}}, nil).
		Times(1)
	resp, err = backend.GetAllChangefeeds(context.Background())
	require.NotNil(t, resp)
	require.Nil(t, err)
	require.Len(t, resp, 0)

	// status unmarshal failed, changefeed will not be ignored, and the checkpiont ts will be the start ts
	// the old version of changefeed without gid
	etcdClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
			{Key: []byte("/tidb/cdc/default/default/changefeed/info/test"), Value: []byte("{\"changefeed-id\":\"test\", \"start-ts\": 1}")},
			{Key: []byte("/tidb/cdc/default/default/changefeed/status/test"), Value: []byte("}{")},
		}}, nil).
		Times(1)
	// put the gid and status
	etcdClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)
	resp, err = backend.GetAllChangefeeds(context.Background())
	require.NotNil(t, resp)
	require.Nil(t, err)
	require.Len(t, resp, 1)
	for _, v := range resp {
		require.Equal(t, uint64(1), v.Status.CheckpointTs)
		require.Equal(t, config.ProgressNone, v.Status.Progress)
	}

	// has no info, changefeed will be ignored
	etcdClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
			{Key: []byte("/tidb/cdc/default/default/changefeed/status/test"), Value: []byte("{}")},
		}}, nil).
		Times(1)
	resp, err = backend.GetAllChangefeeds(context.Background())
	require.NotNil(t, resp)
	require.Nil(t, err)
	require.Len(t, resp, 0)
}

func TestCreateChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	cdcClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient := mock_etcd.NewMockClient(ctrl)
	cdcClient.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cdcClient.EXPECT().GetClusterID().Return("test-cluster-id").AnyTimes()
	backend := NewEtcdBackend(cdcClient)

	// create changefeeds failed
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("txn failed")).Times(1)
	require.NotNil(t, backend.CreateChangefeed(context.Background(), &config.ChangeFeedInfo{}))

	// txn fail
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&clientv3.TxnResponse{Succeeded: false}, nil).Times(1)
	require.NotNil(t, backend.CreateChangefeed(context.Background(), &config.ChangeFeedInfo{}))

	// txn success
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Len(2), gomock.Len(2), gomock.Any()).
		Return(&clientv3.TxnResponse{Succeeded: true}, nil).Times(1)
	require.Nil(t, backend.CreateChangefeed(context.Background(), &config.ChangeFeedInfo{}))
}

func TestUpdateChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	cdcClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient := mock_etcd.NewMockClient(ctrl)
	cdcClient.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cdcClient.EXPECT().GetClusterID().Return("test-cluster-id").AnyTimes()
	backend := NewEtcdBackend(cdcClient)

	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("txn failed")).Times(1)
	require.NotNil(t, backend.UpdateChangefeed(context.Background(), &config.ChangeFeedInfo{}, 0, config.ProgressStopping))

	// txn fail
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&clientv3.TxnResponse{Succeeded: false}, nil).Times(1)
	require.NotNil(t, backend.UpdateChangefeed(context.Background(), &config.ChangeFeedInfo{}, 0, config.ProgressStopping))

	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Len(0), NewFuncMatcher(func(i interface{}) bool {
		ops := i.([]clientv3.Op)
		require.Len(t, ops, 2)
		require.True(t, ops[0].IsPut())
		require.True(t, ops[1].IsPut())
		return true
	}), gomock.Any()).Return(&clientv3.TxnResponse{Succeeded: true}, nil).Times(1)
	require.Nil(t, backend.UpdateChangefeed(context.Background(), &config.ChangeFeedInfo{}, 2, config.ProgressStopping))
}

func TestPauseChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cdcClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient := mock_etcd.NewMockClient(ctrl)
	cdcClient.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cdcClient.EXPECT().GetClusterID().Return("test-cluster-id").AnyTimes()
	backend := NewEtcdBackend(cdcClient)

	changefeedID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{State: model.StateNormal}
	status := &config.ChangeFeedStatus{Progress: config.ProgressStopping}

	cdcClient.EXPECT().GetChangeFeedInfo(gomock.Any(), changefeedID.DisplayName).Return(info, nil).Times(1)
	cdcClient.EXPECT().GetChangeFeedStatus(gomock.Any(), changefeedID).Return(status, int64(0), nil).Times(1)
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.TxnResponse{Succeeded: true}, nil).Times(1)

	err := backend.PauseChangefeed(context.Background(), changefeedID)
	require.Nil(t, err)
}

func TestDeleteChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cdcClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient := mock_etcd.NewMockClient(ctrl)
	cdcClient.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cdcClient.EXPECT().GetClusterID().Return("test-cluster-id").AnyTimes()
	backend := NewEtcdBackend(cdcClient)

	changefeedID := common.NewChangeFeedIDWithName("test")

	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), NewFuncMatcher(func(i interface{}) bool {
		ops := i.([]clientv3.Op)
		require.Len(t, ops, 2)
		require.True(t, ops[0].IsDelete())
		require.True(t, ops[1].IsDelete())
		return true
	}), gomock.Any()).Return(&clientv3.TxnResponse{Succeeded: true}, nil).Times(1)

	err := backend.DeleteChangefeed(context.Background(), changefeedID)
	require.Nil(t, err)
}

func TestResumeChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cdcClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient := mock_etcd.NewMockClient(ctrl)
	cdcClient.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cdcClient.EXPECT().GetClusterID().Return("test-cluster-id").AnyTimes()
	backend := NewEtcdBackend(cdcClient)

	changefeedID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{State: model.StateStopped}
	status := &config.ChangeFeedStatus{CheckpointTs: 100}

	cdcClient.EXPECT().GetChangeFeedInfo(gomock.Any(), changefeedID.DisplayName).Return(info, nil).Times(1)
	cdcClient.EXPECT().GetChangeFeedStatus(gomock.Any(), changefeedID).Return(status, int64(0), nil).Times(1)
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.TxnResponse{Succeeded: true}, nil).Times(1)

	err := backend.ResumeChangefeed(context.Background(), changefeedID, 200)
	require.Nil(t, err)
}

func TestSetChangefeedProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cdcClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient := mock_etcd.NewMockClient(ctrl)
	cdcClient.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cdcClient.EXPECT().GetClusterID().Return("test-cluster-id").AnyTimes()
	backend := NewEtcdBackend(cdcClient)

	changefeedID := common.NewChangeFeedIDWithName("test")
	status := &config.ChangeFeedStatus{Progress: config.ProgressNone}

	cdcClient.EXPECT().GetChangeFeedStatus(gomock.Any(), changefeedID).Return(status, int64(0), nil).Times(1)
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.TxnResponse{Succeeded: true}, nil).Times(1)

	err := backend.SetChangefeedProgress(context.Background(), changefeedID, config.ProgressRemoving)
	require.Nil(t, err)
}

func TestUpdateChangefeedCheckpointTs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cdcClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient := mock_etcd.NewMockClient(ctrl)
	cdcClient.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cdcClient.EXPECT().GetClusterID().Return("test-cluster-id").AnyTimes()
	backend := NewEtcdBackend(cdcClient)

	cps := map[common.ChangeFeedID]uint64{
		common.NewChangeFeedIDWithName("test1"): 100,
	}
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.TxnResponse{Succeeded: false}, nil).Times(1)
	err := backend.UpdateChangefeedCheckpointTs(context.Background(), cps)
	require.NotNil(t, err)

	cps = make(map[common.ChangeFeedID]uint64)
	for i := 0; i < 129; i++ {
		cps[common.NewChangeFeedIDWithName(fmt.Sprintf("%d", i))] = 100
	}
	etcdClient.EXPECT().Txn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.TxnResponse{Succeeded: true}, nil).Times(2)
	err = backend.UpdateChangefeedCheckpointTs(context.Background(), cps)
	require.Nil(t, err)
}

type FuncMarcher struct {
	m func(interface{}) bool
}

func NewFuncMatcher(m func(interface{}) bool) gomock.Matcher {
	return &FuncMarcher{
		m: m,
	}
}

func (f *FuncMarcher) Matches(x interface{}) bool {
	return f.m(x)
}

func (f *FuncMarcher) String() string {
	return "func"
}
