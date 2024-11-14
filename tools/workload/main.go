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

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap"
	"workload/schema"
)

var (
	logFile  string
	logLevel string

	tableCount      int
	tableStartIndex int
	tps             int
	rowCount        int64
	thread          int

	dbHost     string
	dbPort     int
	dbUser     string
	dbPassword string
	dbName     string

	total      uint64
	totalError uint64

	workloadType string

	skipCreateTable bool
	onlyDDL         bool

	rowSize       int
	largeRowSize  int
	largeRowRatio float64

	action                    string
	tableCountForUpdate       int
	threadPercentageForUpdate int
	maxTpsForUpdate           int

	dbNum    int
	dbPrefix string
)

const (
	bank     = "bank"
	sysbench = "sysbench"
	largeRow = "large_row"
)

func init() {
	flag.StringVar(&logFile, "log-file", "workload.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	flag.StringVar(&workloadType, "workload-type", "sysbench", "workload type: [bank, sysbench, express, common, one, bigtable, large_row, wallet]")
	flag.IntVar(&tableCount, "table-count", 1, "table count of the workload")
	flag.IntVar(&tableCountForUpdate, "table-count-for-update", 0, "table count for update")
	flag.IntVar(&tableStartIndex, "table-start-index", 0, "table start index, sbtest<index>")
	flag.IntVar(&tps, "tps", 1000, "tps of the workload")
	flag.IntVar(&maxTpsForUpdate, "max-tps-for-update", 50, "max tps for update")
	flag.IntVar(&thread, "thread", 0, "total thread of the workload")
	flag.IntVar(&threadPercentageForUpdate, "thread-percentage-for-update", 0, "thread percentage for update: [0, 100]")
	flag.Int64Var(&rowCount, "row-count", 1000000, "the total row count of the workload")
	flag.BoolVar(&skipCreateTable, "skip-create-table", false, "do not create tables")
	flag.StringVar(&action, "action", "prepare", "action of the workload: [prepare, insert, update, delete, write, cleanup]")

	flag.IntVar(&rowSize, "row-size", 10240, "the size of each row")
	flag.StringVar(&dbPrefix, "db-prefix", "", "the prefix of the database name")
	flag.IntVar((&dbNum), "db-num", 1, "the number of databases")

	flag.IntVar(&largeRowSize, "large-row-size", 1024*1024, "the size of the large row")
	flag.Float64Var(&largeRowRatio, "large-ratio", 0.0, "large row ratio in the each transaction")

	flag.StringVar(&dbHost, "database-host", "127.0.0.1", "database host")
	flag.StringVar(&dbUser, "database-user", "root", "database user")
	flag.StringVar(&dbPassword, "database-password", "", "database password")
	flag.StringVar(&dbName, "database-db-name", "test", "database db name")
	flag.IntVar(&dbPort, "database-port", 4000, "database port")
	flag.BoolVar(&onlyDDL, "only-ddl", false, "only generate ddl")
	flag.Parse()
}

func main() {
	err := logutil.InitLogger(&logutil.Config{
		Level: logLevel,
		File:  logFile,
	})
	if err != nil {
		log.Error("init logger failed", zap.Error(err))
		return
	}

	threadCount := getThreadCount()
	threadForUpdate := threadCount * threadPercentageForUpdate / 100
	threadForInsert := thread - threadForUpdate
	fmt.Printf("use thread %d\n", threadCount)
	group := &sync.WaitGroup{}

	dbs := make([]*sql.DB, dbNum)
	if dbPrefix != "" {
		for i := 0; i < dbNum; i++ {
			dbName := fmt.Sprintf("%s%d", dbPrefix, i+1)
			db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&maxAllowedPacket=1073741824", dbUser, dbPassword, dbHost, dbPort, dbName))
			if err != nil {
				log.Info("create the sql client failed", zap.Error(err))
			}
			db.SetMaxIdleConns(256)
			db.SetMaxOpenConns(256)
			db.SetConnMaxLifetime(time.Minute)
			dbs[i] = db
		}
	} else {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&maxAllowedPacket=1073741824", dbUser, dbPassword, dbHost, dbPort, dbName))
		if err != nil {
			log.Info("create the sql client failed", zap.Error(err))
		}
		db.SetMaxIdleConns(256)
		db.SetMaxOpenConns(256)
		db.SetConnMaxLifetime(time.Minute)
		dbs[0] = db
	}

	log.Info("created db count", zap.Int("db count", len(dbs)))

	var workload schema.Workload
	switch workloadType {
	case bank:
		workload = schema.NewBankWorkload()
	case sysbench:
		workload = schema.NewSysbenchWorkload()
	case largeRow:
		fmt.Println("use large_row workload")
		workload = schema.NewLargeRowWorkload(rowSize, largeRowSize, largeRowRatio)
	default:
		log.Panic("unsupported workload type", zap.String("workload", workloadType))
	}

	if !skipCreateTable && (action == "prepare") {
		fmt.Printf("skip create table: %v\n", skipCreateTable)
		fmt.Printf("start to create tables, total tables: %d\n", tableCount)
		for _, db := range dbs {
			if err := initTables(db, workload); err != nil {
				panic(err)
			}
		}
	}

	if onlyDDL {
		return
	}

	log.Info("start running workload",
		zap.String("workload_type", workloadType), zap.Int("thread-count", threadCount),
		zap.Int64("total-rows", rowCount), zap.Float64("large-ratio", largeRowRatio),
		zap.Int("tps", tps), zap.String("action", action),
	)

	if action == "insert" || action == "write" || action == "prepare" {
		for i, db := range dbs {
			log.Info("start to insert data to db", zap.Int("dbNum", i))
			dbi := db
			group.Add(threadForInsert)
			for i := 0; i < threadForInsert; i++ {
				go func() {
					defer group.Done()
					doInsert(dbi, threadForInsert, workload)
				}()
			}
		}
	}

	if action == "write" || action == "update" {
		if threadForUpdate == 0 {
			panic("thread-percentage-for-update should be set when action is update or write")
		}
		if tableCountForUpdate == 0 {
			panic("table-count-for-update should be set when action is update or write")
		}
		fmt.Printf("start to update data, use thread %d\n", threadForUpdate)

		group.Add(threadForUpdate + 1)
		updateTaskCh := make(chan updateTask, threadForUpdate*2)
		for i := 0; i < threadForUpdate; i++ {
			go func() {
				defer group.Done()
				doUpdate(dbs[0], workload, updateTaskCh)
			}()
		}
		// generate update tasks
		go func() {
			defer group.Done()
			genUpdateTask(updateTaskCh)
		}()
	}

	go printTPS()
	group.Wait()
}

func getThreadCount() int {
	cpuNum := runtime.NumCPU()
	if thread > 0 {
		cpuNum = thread
	}
	if tps < cpuNum {
		cpuNum = tps
	}
	return cpuNum
}

// initTables create tables if not exists
func initTables(db *sql.DB, workload schema.Workload) error {
	var tableNum atomic.Int32
	wg := sync.WaitGroup{}
	for i := 0; i < thread; i++ {
		wg.Add(1)
		go func() {
			log.Info("create table worker started", zap.Int("worker: ", i))
			defer wg.Done()
			for {
				tableIndex := int(tableNum.Load())
				if tableIndex >= tableCount {
					log.Info("create table worker finished", zap.Int("worker: ", i))
					return
				}
				tableNum.Add(1)
				fmt.Printf("try to create table %d\n", tableIndex+tableStartIndex)
				if _, err := db.Exec(workload.BuildCreateTableStatement(tableIndex + tableStartIndex)); err != nil {
					err := errors.Annotate(err, "create table failed")
					log.Error("create table failed", zap.Error(err))
				}
			}
		}()
	}
	wg.Wait()
	log.Info("create tables finished")
	return nil
}

type updateTask struct {
	schema.UpdateOption
	// reserved for future use
	cb func()
}

func genUpdateTask(output chan updateTask) {
	for {
		for i := 0; i < tableCountForUpdate; i++ {
			// TODO: add more randomness.
			rowCount := rand.Intn(maxTpsForUpdate) + 1
			task := updateTask{
				UpdateOption: schema.UpdateOption{
					Table:    i,
					RowCount: rowCount,
				},
			}
			output <- task
		}
	}
}

func doUpdate(db *sql.DB, workload schema.Workload, input chan updateTask) {
	for task := range input {
		updateSql := workload.BuildUpdateSql(task.UpdateOption)
		res, err := db.Exec(updateSql)
		if err != nil {
			fmt.Println("update error: ", err, ". sql: ", updateSql)
			atomic.AddUint64(&totalError, 1)
		}
		if res != nil {
			cnt, err := res.RowsAffected()
			if err != nil {
				fmt.Println("get rows affected error: ", err)
				atomic.AddUint64(&totalError, 1)
			}
			atomic.AddUint64(&total, uint64(cnt))
			if task.IsSpecialUpdate {
				fmt.Printf("update full table %d succeed, row count %d\n", task.Table, cnt)
			}
		} else {
			fmt.Println("update result is nil")
		}
		if task.cb != nil {
			task.cb()
		}
	}
}

func doInsert(db *sql.DB, threadCount int, workload schema.Workload) {
	rowPerThread := rowCount / int64(threadCount)
	tpsPerThread := int(math.Round(float64(tps) / float64(threadCount)))
	count := rowPerThread

	t := time.Tick(time.Second)
	printedError := false
	for range t {
		if count <= 0 && action == "prepare" {
			return
		}
		rowsPerTable := make(map[int]int)
		// choose thread tables to insert
		for q := 0; q < threadCount; q++ {
			n := rand.Int63()
			tableNum := int(n)%tableCount + tableStartIndex
			rowsPerTable[tableNum] = tpsPerThread
		}

		for tableN, trCount := range rowsPerTable {
			insertSql := workload.BuildInsertSql(tableN, trCount)
			_, err := db.Exec(insertSql)
			if err != nil {
				// if table not exists, we create it
				if strings.Contains(err.Error(), "Error 1146") {
					_, err = db.Exec(workload.BuildCreateTableStatement(tableN))
					if err != nil {
						fmt.Println("create table error: ", err)
						continue
					}
					_, err = db.Exec(insertSql)
					if err != nil {
						log.Info("insert error", zap.Error(err), zap.String("sql", insertSql))
						atomic.AddUint64(&totalError, uint64(trCount))
						continue
					}
				}

				if !printedError {
					fmt.Println(err)
					printedError = true
				}
				fmt.Println("insert error: ", err, ". sql: ", insertSql)
				atomic.AddUint64(&totalError, uint64(trCount))
			}
			atomic.AddUint64(&total, uint64(trCount))
			count = count - int64(trCount)
		}
	}
}

func printTPS() {
	t := time.Tick(time.Second * 5)
	old := uint64(0)
	oldErr := uint64(0)
	for {
		select {
		case <-t:
			temp := atomic.LoadUint64(&total)
			qps := (float64(temp) - float64(old)) / 5.0
			old = temp
			temp = atomic.LoadUint64(&totalError)
			fmt.Printf("total %d, total err %d, qps is %f, err qps %f\n", total, totalError, qps, (float64(temp)-float64(oldErr))/5.0)
			oldErr = temp
		}
	}
}
