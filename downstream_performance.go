package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
	"github.com/flowbehappy/tigate/downstreamadapter/eventcollector"
	"github.com/flowbehappy/tigate/downstreamadapter/heartbeatcollector"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const totalCount = 10000

func initContext(serverId messaging.ServerId) {
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(serverId, watcher.TempEpoch, config.NewDefaultMessageCenterConfig()))
	appcontext.SetService(appcontext.EventCollector, eventcollector.NewEventCollector(100*1024*1024*1024, serverId)) // 100GB for demo
	appcontext.SetService(appcontext.HeartbeatCollector, heartbeatcollector.NewHeartBeatCollector(serverId))
}

func pushDataIntoDispatcher(dispatcherId int, eventDispatcherManager *dispatchermanager.EventDispatcherManager, tableSpan *common.TableSpan) {
	var IndexColumnsOffset [][]int
	var offset []int
	offset = append(offset, 0)
	IndexColumnsOffset = append(IndexColumnsOffset, offset)
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
						IndexColumnsOffset: IndexColumnsOffset,
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

		dispatcherItem, ok := eventDispatcherManager.GetDispatcherMap().Get(tableSpan)
		if ok {
			dispatcherItem.PushTxnEvent(&event)
			dispatcherItem.UpdateResolvedTs(uint64(count) + 11)
		} else {
			log.Error("dispatcher not found")
		}
	}
	//log.Info("Finish Pushing All data into dispatcher", zap.Any("dispatcher id", dispatcherId))
}
func main() {
	dispatcherCount := 1000
	createTables(dispatcherCount / 100)

	time.Sleep(10 * time.Second)

	serverId := messaging.ServerId("test")
	initContext(serverId)

	changefeedConfig := model.ChangefeedConfig{
		SinkURI: "tidb://root:@127.0.0.1:4000",
	}
	changefeedID := model.DefaultChangeFeedID("test")
	eventDispatcherManager := dispatchermanager.NewEventDispatcherManager(changefeedID, &changefeedConfig, serverId, serverId)
	appcontext.GetService[*heartbeatcollector.HeartBeatCollector](appcontext.HeartbeatCollector).RegisterEventDispatcherManager(eventDispatcherManager)

	tableSpanMap := make(map[uint64]*common.TableSpan)
	var mutex sync.Mutex
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < dispatcherCount; i++ {
		tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: uint64(i)}}
		mutex.Lock()
		tableSpanMap[uint64(i)] = tableSpan
		mutex.Unlock()
		wg.Add(1)
		go func(tableSpan *common.TableSpan, wg *sync.WaitGroup) {
			defer wg.Done()
			eventDispatcherManager.NewTableEventDispatcher(tableSpan, 0)
		}(tableSpan, &wg)
	}

	wg.Wait()
	log.Info("test begin", zap.Any("create dispatcher cost time", time.Since(start)))

	// 插入数据, 先固定 data 格式
	for i := 0; i < dispatcherCount; i++ {
		tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{TableID: uint64(i)}}
		mutex.Lock()
		tableSpanMap[uint64(i)] = tableSpan
		mutex.Unlock()
		// eventDispatcherManager.NewTableEventDispatcher(tableSpan, 0)
		go pushDataIntoDispatcher(i, eventDispatcherManager, tableSpan)
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
						//log.Info("One dispatcher is finished", zap.Any("dispatcher id", i))
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

func createTables(tables int) {
	// host := flag.String("host", "127.0.0.1", "host")
	// port := flag.Int("port", 4000, "port")
	// thread := flag.Int("thread", 10, "thread")
	// databaseCnt := flag.Int("database", 1, "database")
	// databaseNamePrefix := flag.String("database_name_prefix", "test_schema_", "database name prefix")
	// tableNamePrefix := flag.String("table_name_prefix", "test_table_", "table name prefix")
	// tableCnt := flag.Int("table", 1000, "table")
	// username := flag.String("username", "root", "username")
	// owner := flag.Bool("owner", true, "owner")
	host := "127.0.0.1"
	port := 4000
	thread := 100
	databaseCnt := 1
	databaseNamePrefix := "test_schema_"
	tableNamePrefix := "test_table_"
	tableCnt := tables
	username := "root"
	owner := true

	fmt.Printf("host: %s, port: %d, thread: %d, database: %d, table: %d\n", host, port, thread, databaseCnt, tableCnt)

	if owner {
		prepare(host, databaseNamePrefix, port, databaseCnt)
	}

	start := time.Now()
	for i := 0; i < databaseCnt; i++ {
		startDB := time.Now()
		db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(%s:%d)/%s_%d", username, host, port, databaseNamePrefix, i))
		if err != nil {
			fmt.Printf("Failed to connect to MySQL database: %v\n", err)
			return
		}
		dbconns := make([]*sql.Conn, 0, thread)

		for i := 0; i < thread; i++ {
			conn, err := db.Conn(context.Background())
			if err != nil {
				fmt.Printf("Failed to connect to MySQL database: %v\n", err)
				db.Close()
				return
			}
			dbconns = append(dbconns, conn)
		}

		var wg sync.WaitGroup
		for i := 0; i < thread; i++ {
			wg.Add(1)
			go createTable(dbconns[i], &wg, i, tableCnt, tableNamePrefix)
		}
		wg.Wait()
		totalTimeDB := time.Since(startDB)
		fmt.Printf("Created %d tables in database %s_%d, time %v\n", tableCnt*thread, databaseNamePrefix, i, totalTimeDB)
		for _, conn := range dbconns {
			conn.Close()
		}
		db.Close()
	}
	totalTime := time.Since(start)
	fmt.Printf("Total execution time: %v\n", totalTime)
	//cleanUp()
}

var (
	TableSQL = "CREATE TABLE `%s` " +
		"  (id int primary key , name char(10), age int, gender char(10) )"
)

func prepare(host, databaseNamePrefix string, port, databaseCnt int) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%d)/", host, port))
	if err != nil {
		fmt.Printf("Failed to connect to MySQL database: %v\n", err)
		return
	}
	defer db.Close()

	for i := 0; i < databaseCnt; i++ {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s_%d", databaseNamePrefix, i))
		if err != nil {
			fmt.Printf("Failed to drop database %s_%d: %v\n", databaseNamePrefix, i, err)
		} else {
			fmt.Printf("Dropped database %s_%d\n", databaseNamePrefix, i)
		}
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s_%d", databaseNamePrefix, i))
		if err != nil {
			fmt.Printf("Failed to create database %s_%d: %v\n", databaseNamePrefix, i, err)
		} else {
			fmt.Printf("Created database %s_%d\n", databaseNamePrefix, i)
		}
		_, err = db.Exec("SET GLOBAL tidb_schema_cache_size=2000000000")
		if err != nil {
			fmt.Printf("Failed to tidb schema cache size, %v", err)
		}
		_, err = db.Exec("SET GLOBAL tidb_enable_fast_create_table=ON")
		if err != nil {
			fmt.Printf("Failed to tidb fast create table, %v", err)
		}
	}

}

func cleanUp(host, databaseNamePrefix string, port, databaseCnt int) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%d)/", host, port))
	if err != nil {
		fmt.Printf("Failed to connect to MySQL database: %v\n", err)
		return
	}
	defer db.Close()
	for i := 0; i < databaseCnt; i++ {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s_%d", databaseNamePrefix, i))
		if err != nil {
			fmt.Printf("Failed to drop database %s_%d: %v\n", databaseNamePrefix, i, err)
		} else {
			fmt.Printf("Dropped database %s_%d\n", databaseNamePrefix, i)
		}
	}
}

func createTable(db *sql.Conn, wg *sync.WaitGroup, idx int, tableCnt int, tableNamePrefix string) {
	for i := 0; i < tableCnt; i++ {
		num := idx*tableCnt + i
		tableName := fmt.Sprintf("%s%d", tableNamePrefix, num)
		tableCreateSQL := fmt.Sprintf(TableSQL, tableName)
		fmt.Println("TableCreateSql", tableCreateSQL)
		_, err := db.ExecContext(context.Background(), tableCreateSQL)
		if err != nil {
			fmt.Printf("Error creating table %s: %s\n", tableName, err.Error())
		}
	}
	wg.Done()
}
