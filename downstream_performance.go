package main

import (
	"strconv"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
	"github.com/flowbehappy/tigate/downstreamadapter/eventcollector"
	"github.com/flowbehappy/tigate/downstreamadapter/heartbeatcollector"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const totalCount = 1000

func initContext(serverId messaging.ServerId) {
	context.SetService(context.MessageCenter, messaging.NewMessageCenter(serverId, watcher.TempEpoch, config.NewDefaultMessageCenterConfig()))
	context.SetService(context.EventCollector, eventcollector.NewEventCollector(100*1024*1024*1024, serverId)) // 100GB for demo
	context.SetService(context.HeartbeatCollector, heartbeatcollector.NewHeartBeatCollector(serverId))
}

func pushDataIntoDispatcher(dispatcherId int, eventDispatcherManager *dispatchermanager.EventDispatcherManager, tableSpanMap map[uint64]*common.TableSpan) {
	for count := 0; count < totalCount; count++ {
		event := common.TxnEvent{
			StartTs:  uint64(count) + 10,
			CommitTs: uint64(count) + 11,
			Rows: []*common.RowChangedEvent{
				{
					TableInfo: &common.TableInfo{
						TableName: common.TableName{
							Schema: "test_schema__0",
							Table:  "test_table_" + strconv.Itoa(dispatcherId),
						},
					},
					Columns: []*common.Column{
						{Name: "id", Value: count, Flag: common.HandleKeyFlag | common.PrimaryKeyFlag},
						{Name: "name", Value: "Alice"},
						{Name: "age", Value: dispatcherId % 50},
						{Name: "gender", Value: "female"},
					},
					PhysicalTableID: int64(dispatcherId),
				},
			},
		}

		dispatcherItem, ok := eventDispatcherManager.GetDispatcherMap().Get(tableSpanMap[uint64(dispatcherId)])
		if ok {
			dispatcherItem.PushTxnEvent(&event)
			dispatcherItem.UpdateResolvedTs(uint64(count) + 11)
		} else {
			log.Error("dispatcher not found")
		}
	}
	log.Info("Finish Pushing All data into dispatcher", zap.Any("dispatcher id", dispatcherId))
}
func main() {
	serverId := messaging.ServerId("test")
	initContext(serverId)

	changefeedConfig := model.ChangefeedConfig{
		SinkURI: "tidb://root:@127.0.0.1:4000",
	}
	changefeedID := model.DefaultChangeFeedID("test")
	eventDispatcherManager := dispatchermanager.NewEventDispatcherManager(changefeedID, &changefeedConfig, serverId, serverId)
	context.GetService[*heartbeatcollector.HeartBeatCollector](context.HeartbeatCollector).RegisterEventDispatcherManager(eventDispatcherManager)

	dispatcherCount := 10000
	tableSpanMap := make(map[uint64]*common.TableSpan)
	for i := 0; i < dispatcherCount; i++ {
		tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: uint64(i)}}
		tableSpanMap[uint64(i)] = tableSpan
		eventDispatcherManager.NewTableEventDispatcher(tableSpan, 0)
	}

	// 插入数据, 先固定 data 格式
	for i := 0; i < dispatcherCount; i++ {
		go pushDataIntoDispatcher(i, eventDispatcherManager, tableSpanMap)
	}

	finishVec := make([]bool, dispatcherCount)
	finishCount := 0
	for i := 0; i < dispatcherCount; i++ {
		finishVec[i] = false
	}
	for {
		for i := 0; i < dispatcherCount; i++ {
			if !finishVec[i] {
				dispatcherItem, ok := eventDispatcherManager.GetDispatcherMap().Get(tableSpanMap[uint64(i)])
				if ok {
					checkpointTs := dispatcherItem.GetCheckpointTs()
					//log.Info("progress is ", zap.Any("dispatcher id", i), zap.Any("checkpointTs", checkpointTs))
					if checkpointTs == uint64(totalCount)+10 {
						finishVec[i] = true
						log.Info("One dispatcher is finished", zap.Any("dispatcher id", i))
						finishCount += 1
						if finishCount == dispatcherCount {
							log.Info("All data consuming is finished")
							return
						}
					}
				} else {
					log.Error("dispatcher not found")
				}
			}
		}
	}

	//log.Info("All data consuming is finished")
}
