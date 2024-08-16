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
	"context"
	"sync"

	"github.com/flowbehappy/tigate/downstreamadapter/sink"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/zap"
)

//filter 问题 -- 能收到这条就至少说明有相关的 table（比如 renames / create tables / exchange partitions -- 这个应该不支持一个在一个不在的），对于跟 table 有关的表来说，那就前面两种就可以在 ddl 生成的时候用 config 处理了
// 这个要让 logService 传过来 新增 table span 删除 tableSpan 的问题
/* double check 一下类型吧
ActionCreateSchema -- 只需要执行下游语句
ActionDropSchema -- 只需要执行下游语句
ActionCreateTable -- maintainer 通知 table trigger event dispatcher 执行下游语句，成功后创建 dispatcher
ActionDropTable -- maintainer 通知 table trigger event dispatcher 执行下游语句，成功后删除 dispatcher
ActionTruncateTable -- maintainer 通知 table trigger event dispatcher 执行下游语句，成功后删除老 dispatcher，创建新 dispatcher
ActionRenameTable -- 先在下游执行这个语句，然后删掉老的 dispatcher，创建新的 dispatcher
ActionAddTablePartition -- 先执行这个语句，然后创建新的 dispatcher
ActionDropTablePartition -- 先执行这个语句，然后删除老的 dispatcher
ActionTruncateTablePartition -- 先执行这个语句，然后删除老的 dispatcher，创建新的 dispatcher
ActionRecoverTable -- 先执行这个语句，然后创建新的 dispatcher
ActionRepairTable -- 只需要执行
ActionExchangeTablePartition -- 先执行这个语句，这个理论上 table id 变了，其他应该没怎么变，可以先考虑删了老 dispatcher 然后创建新的，后面也可以考虑要不要变成更轻量的修改。
ActionRemovePartitioning -- 先执行这个语句，然后删除老的 dispatcher
ActionRenameTables -- rename table 的复数版
ActionCreateTables -- create table 的复数版
ActionReorganizePartition -- 先执行，然后该删删该加加
ActionFlashbackCluster -- 执行语句,只对 tidb 有效果
ActionCreateResourceGroup/ActionAlterResourceGroup/ActionDropResourceGroup 执行语句，且只对 tidb
*/

/*
TableTriggerEventDispatcher 需要接收所有跟创建表或者删表相关的 ddl，其中每一条 DDL 的推进，都要跟 maintainer 沟通，确认是否需要自己执行（ pass / write），所以可以理解为同一个 changefeed 的多个节点的 tableTriggerEventDispatcher 进度至少是基本同步的。如果是 rename 这种 ddl，就还要等对应表的 checkpointTs 推到。





假设我有两个节点A，B, table C 一开始在 A 上同步，然后下一条 event 是 checkpointTs d 的 rename 操作，然后 A 和 B 的 tableTriggerEventTable 的 checkpointTs 也推到 t-1 了，现在大家都跟 maintainer 通信说了自己推到了 d-1，然后 maintainer 会通知某个tableTrigger 执行这条 ddl, 通知另一个节点 B 跳过这条 ddl，通知 这个表跳过这条 ddl（执行的先通知，跳过的是执行完才通知的）。
如果这个时候这个表被迁移了，没收到那个跳过的通知，所以 maintainer 需要再次通知他跳过这条 ddl。 -- 这个是maintainer 需要做的事情。也就是要求maintainer 要至少知道 ddl 的推进进度，保证 skip 可以重发。
那如果通知执行这条 ddl 后，却没有收到推进的消息，则应该选择同节点重发，不能换节点。
对于 table trigger event table 的 ddl，我们需要严格按照顺序执行，满足前一条没有执行成功时，后一条不能开始执行的要求。skip 和下一条的执行是可以一起通知的。所有的 ddl 信息在收到以后快速等10ms 或者其他时间以后就按照心跳发送给 maintainer。如果没有什么 ddl，这个就跟着大家定期汇报进度，有 ddl 到了或者在排队等待的时候，就应该更高频？比如 20ms 这样可以发50个来回？每次最多发两个 ddl event 给上面。



在哪里生成那些没有 query 的 ddl query -- 单表的没有问题，多表的话也可以先都生成，我自己来做 filter

add index 的问题 -- 这个异步去做，并且更改现在的状态为有 ddl 执行中，没执行完后续的 ddl 不能推进，dml 可以先正常推进知道卡到下一个 ddl 。


所以本质来说 tableTriggerEvent 持续接收 ddl，然后跟maintainer 沟通决定是否能推进。


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
	id              string
	eventCh         chan *common.TxnEvent
	filter          filter.Filter
	sink            sink.Sink
	ddlActions      chan *heartbeatpb.DispatcherAction
	acks            chan *heartbeatpb.ACK
	tableSpan       *common.TableSpan // 给一个特殊的 tableSpan
	resolvedTs      *TsWithMutex
	componentStatus *ComponentStateWithMutex

	tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus

	wg     sync.WaitGroup
	cancel context.CancelFunc
	// ddl 相关的可以考虑塞进一个 struct 去
	ddlPendingEvent *common.TxnEvent
	ddlFinishCh     chan struct{}
	//MemoryUsage *MemoryUsage
}

func NewTableTriggerEventDispatcher(sink sink.Sink, startTs uint64, tableSpanStatusesChan chan *heartbeatpb.TableSpanStatus, filter filter.Filter) *TableTriggerEventDispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	tableTriggerEventDispatcher := &TableTriggerEventDispatcher{
		id:                    uuid.NewString(),
		filter:                filter,
		eventCh:               make(chan *common.TxnEvent, 1000),
		resolvedTs:            newTsWithMutex(startTs),
		ddlActions:            make(chan *heartbeatpb.DispatcherAction, 16),
		acks:                  make(chan *heartbeatpb.ACK, 16),
		tableSpanStatusesChan: tableSpanStatusesChan,
		sink:                  sink,
		tableSpan:             &common.DDLSpan,
		componentStatus:       newComponentStateWithMutex(heartbeatpb.ComponentState_Working),
		cancel:                cancel,
		ddlFinishCh:           make(chan struct{}),
		//MemoryUsage:   dispatcher.NewMemoryUsage(),
	}
	tableTriggerEventDispatcher.sink.AddTableSpan(tableTriggerEventDispatcher.tableSpan)

	tableTriggerEventDispatcher.wg.Add(1)
	go tableTriggerEventDispatcher.DispatcherEvents(ctx)

	tableTriggerEventDispatcher.wg.Add(1)
	go HandleDDLActions(tableTriggerEventDispatcher, ctx)

	log.Info("table trigger event dispatcher created", zap.Any("DispatcherID", tableTriggerEventDispatcher.id))

	return tableTriggerEventDispatcher
}

func (d *TableTriggerEventDispatcher) DispatcherEvents(ctx context.Context) {
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-d.GetEventChan():
			if event.IsDDLEvent() {
				AddDDLEventToSinkWhenAvailable(d, event)
			} else {
				d.resolvedTs.Set(event.ResolvedTs)
			}
		}
	}
}

func (d *TableTriggerEventDispatcher) GetSink() sink.Sink {
	return d.sink
}

func (d *TableTriggerEventDispatcher) GetTableSpan() *common.TableSpan {
	return d.tableSpan
}

func (d *TableTriggerEventDispatcher) GetEventChan() chan *common.TxnEvent {
	return d.eventCh
}

func (d *TableTriggerEventDispatcher) GetResolvedTs() uint64 {
	return d.resolvedTs.Get()
}

func (d *TableTriggerEventDispatcher) GetId() string {
	return d.id
}

func (d *TableTriggerEventDispatcher) GetDispatcherType() DispatcherType {
	return TableTriggerEventDispatcherType
}

func (d *TableTriggerEventDispatcher) GetDDLActions() chan *heartbeatpb.DispatcherAction {
	return d.ddlActions
}

func (d *TableTriggerEventDispatcher) GetACKs() chan *heartbeatpb.ACK {
	return d.acks
}

func (d *TableTriggerEventDispatcher) GetTableSpanStatusesChan() chan *heartbeatpb.TableSpanStatus {
	return d.tableSpanStatusesChan
}

func (d *TableTriggerEventDispatcher) UpdateResolvedTs(ts uint64) {
	d.GetEventChan() <- &common.TxnEvent{ResolvedTs: ts}
}

// func (d *TableTriggerEventDispatcher) GetSyncPointInfo() *SyncPointInfo {
// 	log.Error("TableEventDispatcher.GetSyncPointInfo is not implemented")
// 	return nil
// }

// func (d *TableTriggerEventDispatcher) GetMemoryUsage() *MemoryUsage {
// 	return d.MemoryUsage
// }

func (d *TableTriggerEventDispatcher) PushTxnEvent(event *common.TxnEvent) {
	//d.GetMemoryUsage().Add(event.CommitTs, event.MemoryCost())
	d.GetEventChan() <- event // 换成一个函数
}

func (d *TableTriggerEventDispatcher) GetCheckpointTs() uint64 {
	checkpointTs := d.GetSink().GetCheckpointTs(d.GetTableSpan())
	if checkpointTs == 0 {
		// 说明从没有数据写到过 sink，则选择用 resolveTs 作为 checkpointTs
		checkpointTs = d.GetResolvedTs()
	}
	return checkpointTs
}

func (d *TableTriggerEventDispatcher) GetComponentStatus() heartbeatpb.ComponentState {
	return heartbeatpb.ComponentState_Working
}

// TryClose try to close the tableTriggerEventDispatcher,
// It should first check whether the related events in sink is finished.
// If yes, then return checkpointTs, true, else return 0, false.
func (d *TableTriggerEventDispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
	if d.sink.IsEmpty(d.tableSpan) {
		d.sink.RemoveTableSpan(d.tableSpan)
		w.CheckpointTs = w.GetCheckpointTs()
		w.ResolvedTs = d.GetResolvedTs()
		return w, true
	}
	return w, false
}

func (d *TableTriggerEventDispatcher) GetFilter() filter.Filter {
	return d.filter
}

func (d *TableTriggerEventDispatcher) GetWG() *sync.WaitGroup {
	return &d.wg
}

func (d *TableTriggerEventDispatcher) GetDDLPendingEvent() *common.TxnEvent {
	return d.ddlPendingEvent
}

func (d *TableTriggerEventDispatcher) SetDDLPendingEvent(event *common.TxnEvent) {
	if d.ddlPendingEvent != nil {
		log.Error("there is already a pending ddl event, can not set a new one")
		return
	}
	d.ddlPendingEvent = event
}

func (d *TableTriggerEventDispatcher) GetDDLFinishCh() chan struct{} {
	return d.ddlFinishCh
}

func (d *TableTriggerEventDispatcher) Remove() {
	// TODO: 修改这个 dispatcher 的 status 为 removing
	d.cancel()
	d.sink.StopTableSpan(d.tableSpan)
	log.Info("table event dispatcher component status changed to stopping", zap.String("table", d.tableSpan.String()))
	d.componentStatus.Set(heartbeatpb.ComponentState_Stopping)
}
