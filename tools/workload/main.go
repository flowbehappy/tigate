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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"workload/schema"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap"
)

var (
	logFile  string
	logLevel string

	tableCount      int
	tableStartIndex int
	qps             int
	rps             int

	dbHost     string
	dbPort     int
	dbUser     string
	dbPassword string
	dbName     string

	totalCount uint64
	total      uint64
	totalError uint64

	workloadType string

	skipCreateTable bool
	onlyDDL         bool

	rowSize       int
	largeRowSize  int
	largeRowRatio float64

	action              string
	percentageForUpdate int

	dbNum    int
	dbPrefix string
)

const (
	bank     = "bank"
	sysbench = "sysbench"
	largeRow = "large_row"
)

func init() {
	flag.StringVar(&dbPrefix, "db-prefix", "", "the prefix of the database name")
	flag.IntVar((&dbNum), "db-num", 1, "the number of databases")
	flag.IntVar(&tableCount, "table-count", 1, "table count of the workload")
	flag.IntVar(&tableStartIndex, "table-start-index", 0, "table start index, sbtest<index>")
	flag.IntVar(&qps, "qps", 1000, "qps of the workload")
	flag.IntVar(&rps, "rps", 10, "the row count per second of the workload")
	flag.Uint64Var(&totalCount, "total-row-count", 1000000, "the total row count of the workload")
	flag.IntVar(&percentageForUpdate, "percentage-for-update", 0, "percentage for update: [0, 100]")
	flag.BoolVar(&skipCreateTable, "skip-create-table", false, "do not create tables")
	flag.StringVar(&action, "action", "prepare", "action of the workload: [prepare, insert, update, delete, write, cleanup]")
	flag.StringVar(&workloadType, "workload-type", "sysbench", "workload type: [bank, sysbench, express, common, one, bigtable, large_row, wallet]")
	flag.StringVar(&dbHost, "database-host", "127.0.0.1", "database host")
	flag.StringVar(&dbUser, "database-user", "root", "database user")
	flag.StringVar(&dbPassword, "database-password", "", "database password")
	flag.StringVar(&dbName, "database-db-name", "test", "database db name")
	flag.IntVar(&dbPort, "database-port", 4000, "database port")
	flag.BoolVar(&onlyDDL, "only-ddl", false, "only generate ddl")
	flag.StringVar(&logFile, "log-file", "workload.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	// For large row workload
	flag.IntVar(&rowSize, "row-size", 10240, "the size of each row")
	flag.IntVar(&largeRowSize, "large-row-size", 1024*1024, "the size of the large row")
	flag.Float64Var(&largeRowRatio, "large-ratio", 0.0, "large row ratio in the each transaction")
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
		dbNum = 1
		db.SetMaxIdleConns(256)
		db.SetMaxOpenConns(256)
		db.SetConnMaxLifetime(time.Minute)
		dbs[0] = db
	}

	qpsForUpdate := qps * percentageForUpdate / 100
	qpsForInsert := qps - qpsForUpdate

	log.Info("database info", zap.Int("dbCount", dbNum), zap.Int("tableCount", tableCount))

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

	go printTPS()
	group := &sync.WaitGroup{}
	if !skipCreateTable && (action == "prepare") {
		log.Info("start to create tables", zap.Int("tableCount", tableCount))
		for _, db := range dbs {
			if err := initTables(db, workload); err != nil {
				panic(err)
			}
		}
		// insert
		if totalCount != 0 {
			group.Add(qpsForInsert)
			for i := 0; i < qpsForInsert; i++ {
				go func() {
					defer group.Done()
					doInsert(dbs, workload)
				}()
			}
			group.Wait()
		}
		return
	}

	if onlyDDL {
		return
	}

	log.Info("start running workload",
		zap.String("workload_type", workloadType), zap.Int("rps", rps), zap.Float64("large-ratio", largeRowRatio),
		zap.Int("qps", qps), zap.String("action", action),
	)
	if action == "insert" || action == "write" {
		group.Add(qpsForInsert)
		for i := 0; i < qpsForInsert; i++ {
			go func() {
				defer group.Done()
				doInsert(dbs, workload)
			}()
		}
	}

	if (action == "write" || action == "update") && qpsForUpdate != 0 {
		updateTaskCh := make(chan updateTask, rps)
		group.Add(qpsForUpdate)
		for i := 0; i < qpsForUpdate; i++ {
			go func() {
				defer group.Done()
				doUpdate(dbs[0], workload, updateTaskCh)
			}()
		}
		go func() {
			defer group.Done()
			genUpdateTask(updateTaskCh)
		}()
	}

	group.Wait()
	for _, db := range dbs {
		db.Close()
	}
}

// initTables create tables if not exists
func initTables(db *sql.DB, workload schema.Workload) error {
	var tableNum atomic.Int32
	wg := sync.WaitGroup{}
	for i := 0; i < tableCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tableIndex := int(tableNum.Load())
				if tableIndex >= tableCount {
					return
				}
				tableNum.Add(1)
				log.Info("try to create table", zap.Int("index", tableIndex+tableStartIndex))
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
		j := rand.Intn(tableCount) + tableStartIndex
		// TODO: add more randomness.
		task := updateTask{
			UpdateOption: schema.UpdateOption{
				Table:    j,
				RowCount: rps,
			},
		}
		output <- task
	}
}

func doUpdate(db *sql.DB, workload schema.Workload, input chan updateTask) {
	for task := range input {
		updateSql := workload.BuildUpdateSql(task.UpdateOption)
		res, err := db.Exec(updateSql)
		if err != nil {
			log.Info("update error", zap.Error(err), zap.String("sql", updateSql))
			atomic.AddUint64(&totalError, 1)
		}
		if res != nil {
			cnt, err := res.RowsAffected()
			if err != nil || cnt != int64(task.RowCount) {
				log.Info("get rows affected error", zap.Error(err), zap.Int64("affectedRows", cnt), zap.Int("rowCount", task.RowCount))
				atomic.AddUint64(&totalError, 1)
			}
			atomic.AddUint64(&total, 1)
			if task.IsSpecialUpdate {
				log.Info("update full table succeed, row count %d\n", zap.Int("table", task.Table), zap.Int64("affectedRows", cnt))
			}
		} else {
			log.Info("update result is nil")
		}
		if task.cb != nil {
			task.cb()
		}
	}
}

func doInsert(dbs []*sql.DB, workload schema.Workload) {
	t := time.Tick(time.Second)
	for range t {
		i := rand.Intn(dbNum)
		j := rand.Intn(tableCount) + tableStartIndex
		insertSql := workload.BuildInsertSql(j, rps)
		err := exceInsert(dbs[i], insertSql, workload, j)
		if err != nil {
			log.Info("insert error", zap.Error(err))
			atomic.AddUint64(&totalError, 1)
			continue
		}
		atomic.AddUint64(&total, 1)
		if total*uint64(rps) >= totalCount {
			return
		}
	}
}

func exceInsert(db *sql.DB, sql string, workload schema.Workload, n int) error {
	_, err := db.Exec(sql)
	if err != nil {
		// if table not exists, we create it
		if strings.Contains(err.Error(), "Error 1146") {
			_, err := db.Exec(workload.BuildCreateTableStatement(n))
			if err != nil {
				log.Info("create table error: ", zap.Error(err))
				return err
			}
			_, err = db.Exec(sql)
			return err
		}
	}
	return nil
}

func printTPS() {
	t := time.Tick(time.Second * 5)
	old := uint64(0)
	oldErr := uint64(0)
	pre := time.Now()
	for now := range t {
		duration := now.Sub(pre).Seconds()
		pre = now
		temp := atomic.LoadUint64(&total)
		qps := (float64(temp) - float64(old)) / duration
		old = temp
		temp = atomic.LoadUint64(&totalError)
		errQps := (float64(temp) - float64(oldErr)) / duration
		log.Info("metric",
			zap.Uint64("total", total),
			zap.Uint64("totalErr", totalError),
			zap.Float64("qps", qps),
			zap.Float64("errQps", errQps),
			zap.Float64("tps", qps*float64(rps)),
		)
		oldErr = temp
	}
}
