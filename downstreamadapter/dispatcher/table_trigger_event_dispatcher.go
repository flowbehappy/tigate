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

package dispatcher

import (
	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tiflow/pkg/filter"

	"github.com/pingcap/log"
)

/*
ActionCreateSchema -- 这个要保证自己是晚于 drop database d1 即可，另外要比自己后面的 ddl 早（以防后面有 create table d1.t1)
ActionDropSchema -- 也要保证顺序，要早于 create database d1，但不能早于其他这个 db 中的表执行到这个 ddl.commitTs， 不然会丢失数据 -- 每个 db 内的表都会收一份
ActionCreateTable -- 要晚于 drop table t1
ActionDropTable -- 要早于 create table t1，以及不能早于这个表本身执行到 ddl.CommitTs -- shared with 对应 table
ActionTruncateTable -- 这个也要卡在这个表本身执行到 ddl.CommitTs -- shared with 对应的 table
ActionRenameTable -- 卡对应 table 本身 -- shared with 对应 table
ActionAddTablePartition -- 卡在内部对应物理表的相同时间
ActionDropTablePartition -- 卡在内部对应物理表的相同时间,主要是为了保证 add table partition 不会出问题
ActionTruncateTablePartition
ActionRecoverTable
ActionRepairTable
ActionExchangeTablePartition
ActionRemovePartitioning
ActionRenameTables
ActionCreateTables
ActionReorganizePartition
ActionFlashbackCluster -- 只支持没有表结构和库结构变更时同步 -- 要做一下检查
ActionMultiSchemaChange -- 这个只是一个 action，不改变 table 本身，但是要卡全局的 ts
ActionCreateResourceGroup/ActionAlterResourceGroup/ActionDropResourceGroup 卡全局 ts，但是只对下游是 tidb 的时候可用，别的时候都别往下扔
*/

/*
TableTriggerEventDispatcher implements the Dispatcher interface.

TableTriggerEventDispatcher is a speical dispatcher.

It is responsible for getting the ddl events from the Logservice and sending them to the Sink in an appropriate order.
It only pay attention to the speical ddl events, which will leads to new table or remove table,
such as Create Table, Drop Table, Rename Table, Exchange Table Partition, etc.

In each EventDispatcherManager, there is only one TableTriggerEventDispatcher,
and it also the first dispatcher in the EventDispatcherManager.

It also communicates with the Maintainer periodically to report self progress,
and get the other dispatcher's progress and action of the blocked event.
*/
type TableTriggerEventDispatcher struct {
	Id            string
	Ch            chan *common.TxnEvent // 接受 event -- 先做个基础版本的，每次处理一条 ddl 的那种
	Filter        filter.Filter
	Sink          sink.Sink
	HeartbeatChan chan *HeartBeatResponseMessage
	State         *State
	TableSpan     *common.TableSpan // 给一个特殊的 tableSpan
	ResolvedTs    uint64

	MemoryUsage *MemoryUsage
}

func (d *TableTriggerEventDispatcher) GetSink() sink.Sink {
	return d.Sink
}

func (d *TableTriggerEventDispatcher) GetTableSpan() *common.TableSpan {
	return d.TableSpan
}

func (d *TableTriggerEventDispatcher) GetState() *State {
	return d.State
}

func (d *TableTriggerEventDispatcher) GetEventChan() chan *common.TxnEvent {
	return d.Ch
}

func (d *TableTriggerEventDispatcher) GetResolvedTs() uint64 {
	return d.ResolvedTs
}

func (d *TableTriggerEventDispatcher) GetId() string {
	return d.Id
}

func (d *TableTriggerEventDispatcher) GetDispatcherType() DispatcherType {
	return TableTriggerEventDispatcherType
}

func (d *TableTriggerEventDispatcher) GetHeartBeatChan() chan *HeartBeatResponseMessage {
	return d.HeartbeatChan
}

func (d *TableTriggerEventDispatcher) UpdateResolvedTs(ts uint64) {
	d.ResolvedTs = ts
}

func (d *TableTriggerEventDispatcher) GetSyncPointInfo() *SyncPointInfo {
	log.Error("TableEventDispatcher.GetSyncPointInfo is not implemented")
	return nil
}

func (d *TableTriggerEventDispatcher) GetMemoryUsage() *MemoryUsage {
	return d.MemoryUsage
}

func (d *TableTriggerEventDispatcher) PushTxnEvent(event *common.TxnEvent) {
	//d.GetMemoryUsage().Add(event.CommitTs, event.MemoryCost())
	d.Ch <- event // 换成一个函数
}

func (d *TableTriggerEventDispatcher) GetCheckpointTs() uint64 { return 0 }

func (d *TableTriggerEventDispatcher) GetComponentStatus() heartbeatpb.ComponentState {
	return heartbeatpb.ComponentState_Working
}
