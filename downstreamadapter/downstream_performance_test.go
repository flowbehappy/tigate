package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/node"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/tiflow/cdc/model"
	ticonfig "github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

const totalCount = 30
const dispatcherCount = 10000
const databaseCount = 1

func initContext(serverId node.ID) {
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(context.Background(), serverId, 100, config.NewDefaultMessageCenterConfig()))
	appcontext.SetService(appcontext.EventCollector, eventcollector.New(context.Background(), 100*1024*1024*1024, serverId)) // 100GB for demo
	appcontext.SetService(appcontext.HeartbeatCollector, dispatchermanager.NewHeartBeatCollector(serverId))
}

func pushDataIntoDispatchers(dispatcherIDSet map[common.DispatcherID]interface{}, helper *commonEvent.EventTestHelper) {
	// 因为开了 dryrun，所以不用避免冲突，随便写'
	dispatcherEventsDynamicStream := dispatcher.GetEventsDynamicStream()
	idx := 0
	var mutex sync.Mutex
	var wg sync.WaitGroup
	var listMutex sync.Mutex
	eventList := make([]*commonEvent.DMLEvent, 0, totalCount*dispatcherCount)
	for id, _ := range dispatcherIDSet {
		wg.Add(1)
		go func(idx int, id common.DispatcherID) {
			defer wg.Done()
			tableName := "test.t" + strconv.Itoa(idx)
			ddlQuery := "create table " + tableName + " (a int primary key, b int, c double, d varchar(100))"
			mutex.Lock()
			_ = helper.DDL2Job(ddlQuery)
			mutex.Unlock()
			for count := 1; count <= totalCount; count++ {
				mutex.Lock()
				event := helper.DML2Event("test", "t"+strconv.Itoa(int(idx)), "insert into "+tableName+" values ("+strconv.Itoa(count)+", 1, 1.1, 'test')")
				mutex.Unlock()
				event.DispatcherID = id
				event.PhysicalTableID = int64(idx)
				event.StartTs = uint64(count) + 10
				event.CommitTs = uint64(count) + 11

				listMutex.Lock()
				eventList = append(eventList, event)
				listMutex.Unlock()
			}
		}(idx, id)
		idx += 1
	}

	wg.Wait()
	log.Warn("begin to push data into dispatchers")
	for _, event := range eventList {
		dispatcherEventsDynamicStream.In() <- dispatcher.DispatcherEvent{
			Event: event,
		}
	}
	log.Warn("end to push data into dispatchers")
}

func TestDownstream(t *testing.T) {
	log.SetLevel(zap.WarnLevel)
	go func() {
		http.ListenAndServe("0.0.0.0:6100", nil)
	}()
	//createTables(dispatcherCount/100, databaseCount)

	serverId := node.ID("test")
	initContext(serverId)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	var wg sync.WaitGroup
	start := time.Now()

	managerMap := make(map[int]*dispatchermanager.EventDispatcherManager)

	dispatcherIDSet := make(map[common.DispatcherID]interface{})
	var mutex sync.Mutex
	openProtocol := "open-protocol"
	for db_index := 0; db_index < databaseCount; db_index++ {
		changefeedConfig := config.ChangefeedConfig{
			// SinkURI: "tidb://root:@127.0.0.1:4000?dry-run=true",
			SinkURI: "kafka://127.0.0.1:9094/topic-name?protocol=open-protocol&kafka-version=2.4.0&max-message-bytes=67108864&replication-factor=1",
			Filter:  &ticonfig.FilterConfig{},
			SinkConfig: &config.SinkConfig{
				Protocol: &openProtocol,
			},
		}
		changefeedID := model.DefaultChangeFeedID("test" + strconv.Itoa(db_index))
		eventDispatcherManager, err := dispatchermanager.NewEventDispatcherManager(changefeedID, &changefeedConfig, serverId)
		if err != nil {
			t.Fatal(err)
		}
		managerMap[db_index] = eventDispatcherManager

		for i := 0; i < dispatcherCount; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				tableSpan := &heartbeatpb.TableSpan{TableID: int64(db_index*dispatcherCount + i)}
				dispatcherID := common.NewDispatcherID()
				mutex.Lock()
				dispatcherIDSet[dispatcherID] = nil
				mutex.Unlock()
				eventDispatcherManager.NewDispatcher(dispatcherID, tableSpan, 0, 1)
			}(&wg)
		}
	}

	wg.Wait()
	log.Warn("test begin", zap.Any("create dispatcher cost time", time.Since(start)))

	// 插入数据, 先固定 data 格式
	go pushDataIntoDispatchers(dispatcherIDSet, helper)

	finishCount := 0
	finishVec := make([]bool, databaseCount)
	for db_index := 0; db_index < databaseCount; db_index++ {
		finishVec[db_index] = false
	}
	for {
		for db_index := 0; db_index < databaseCount; db_index++ {
			if finishVec[db_index] {
				continue
			}
			eventDispatcherManager := managerMap[db_index]
			message := eventDispatcherManager.CollectHeartbeatInfo(false)
			checkpointTs := message.Watermark.CheckpointTs
			if checkpointTs == uint64(totalCount)+10 {
				finishVec[db_index] = true
				finishCount++
				if finishCount == databaseCount {
					log.Warn("All data consuming is finished")
					return
				}
			}
		}
	}

}

/*
func createTables(tables int, db int) {
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
	databaseCnt := db
	databaseNamePrefix := "test_schema"
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
*/
