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

package writer

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
)

const (
	defaultDDLMaxRetry uint64 = 20
)

// 用于给 mysql 类型的下游做 flush, 主打一个粗糙，先能跑起来再说
type MysqlWriter struct {
	db *sql.DB
	//cfg *MysqlConfig
	cfg *pmysql.Config
}

func NewMysqlWriter(cfg *MysqlConfig) *MysqlWriter {
	db, _ := createMysqlDBConn(cfg)
	return &MysqlWriter{
		db:  db,
		cfg: cfg,
	}
}

func (w *MysqlWriter) FlushDDLEvent(event *Event) error {
	if event.GetDDLType() == timodel.ActionAddIndex && w.cfg.IsTiDB {
		return w.asyncExecAddIndexDDLIfTimeout(event)
	}
	return w.execDDLWithMaxRetries(event)

}

func (w *MysqlWriter) asyncExecAddIndexDDLIfTimeout(event *Event) error {
	done := make(chan error, 1)
	// wait for 2 seconds at most
	tick := time.NewTimer(2 * time.Second)
	defer tick.Stop()
	log.Info("async exec add index ddl start",
		zap.Uint64("commitTs", event.CommitTs()),
		zap.String("ddl", event.GetDDLQuery()))
	go func() {
		if err := w.execDDLWithMaxRetries(event); err != nil {
			log.Error("async exec add index ddl failed",
				zap.Uint64("commitTs", event.CommitTs()),
				zap.String("ddl", event.GetDDLQuery()))
			done <- err
			return
		}
		log.Info("async exec add index ddl done",
			zap.Uint64("commitTs", event.CommitTs()),
			zap.String("ddl", event.GetDDLQuery()))
		done <- nil
	}()

	select {
	case err := <-done:
		// if the ddl is executed within 2 seconds, we just return the result to the caller.
		return err
	case <-tick.C:
		// if the ddl is still running, we just return nil,
		// then if the ddl is failed, the downstream ddl is lost.
		// because the checkpoint ts is forwarded.
		log.Info("async add index ddl is still running",
			zap.Uint64("commitTs", event.CommitTs()),
			zap.String("ddl", event.GetDDLQuery()))
		return nil
	}
}

func (w *MysqlWriter) execDDL(event *Event) error {

	shouldSwitchDB := needSwitchDB(ddl)

	ctx := context.Background()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(event.TableInfo.TableName.Schema)+";")
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return err
		}
	}

	// we try to set cdc write source for the ddl
	if err = pmysql.SetWriteSource(ctx, w.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback", zap.Error(err))
			}
		}
		return err
	}

	if _, err = tx.ExecContext(ctx, event.GetDDLQuey()); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", event.GetDDLQuey()), zap.Error(err))
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		log.Error("Failed to exec DDL", zap.String("sql", event.GetDDLQuey(), zap.Error(err)))
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", event.GetDDLQuey())))
	}

	log.Info("Exec DDL succeeded", zap.String("sql", event.GetDDLQuey()))
	return nil
}

func (w *MysqlWriter) execDDLWithMaxRetries(event *Event) error {
	return retry.Do(context.Background(), func() error {
		return w.execDDL(event)
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(errorutil.IsRetryableDDLError))
}

func (w *MysqlWriter) Flush(events []*Event) error {
	dmls := w.prepareDMLs(events)

	if err := w.execDMLWithMaxRetries(dmls); err != nil {
		log.Error("execute DMLs failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (w *MysqlWriter) prepareDMLs(events []*Event) *preparedDMLs {
	// callback 还没处理过
	// TODO: use a sync.Pool to reduce allocations.
	startTs := make([]uint64, 0)
	sqls := make([]string, 0)
	values := make([][]interface{}, 0)

	rowCount := 0
	approximateSize := int64(0)
	for _, event := range events {
		if len(event.Rows) == 0 {
			continue
		}
		rowCount += len(event.Rows)

		firstRow := event.Rows[0]
		if len(startTs) == 0 || startTs[len(startTs)-1] != firstRow.StartTs {
			startTs = append(startTs, firstRow.StartTs)
		}

		quoteTable := firstRow.TableInfo.TableName.QuoteString()
		for _, row := range event.Rows {
			var query string
			var args []interface{}
			// Update Event
			if len(row.PreColumns) != 0 && len(row.Columns) != 0 {
				query, args = prepareUpdate(
					quoteTable,
					row.GetPreColumns(),
					row.GetColumns())
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
				approximateSize += int64(len(query)) + row.ApproximateDataSize
				continue
			}

			// Delete Event
			if len(row.PreColumns) != 0 {
				query, args = prepareDelete(quoteTable, row.GetPreColumns())
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
			}

			// Insert Event
			// It will be translated directly into a
			// INSERT(not in safe mode)
			// or REPLACE(in safe mode) SQL.
			if len(row.Columns) != 0 {
				query, args = prepareReplace(
					quoteTable,
					row.GetColumns(),
					true)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
			}

			approximateSize += int64(len(query)) + row.ApproximateDataSize
		}
	}

	// if len(callbacks) == 0 {
	// 	callbacks = nil
	// }

	return &preparedDMLs{
		startTs: startTs,
		sqls:    sqls,
		values:  values,
		//callbacks:       callbacks,
		rowCount:        rowCount,
		approximateSize: approximateSize,
	}
}

func (w *MysqlWriter) execDMLWithMaxRetries(dmls *preparedDMLs) error {
	if len(dmls.sqls) != len(dmls.values) {
		log.Error("unexpected number of sqls and values",
			zap.Strings("sqls", dmls.sqls),
			zap.Any("values", dmls.values))
		return cerror.ErrUnexpected.FastGenByArgs("unexpected number of sqls and values")
	}

	ctx := context.Background()
	// approximateSize is multiplied by 2 because in extreme circustumas, every
	// byte in dmls can be escaped and adds one byte.
	return retry.Do(ctx, func() error {
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			log.Error("err", err)
		}

		// Set session variables first and then execute the transaction.
		// we try to set write source for each txn,
		// so we can use it to trace the data source
		if err = pmysql.SetWriteSource(ctx, w.cfg, tx); err != nil {
			log.Error("err", err)
			if rbErr := tx.Rollback(); rbErr != nil {
				if errors.Cause(rbErr) != context.Canceled {
					log.Warn("failed to rollback txn", zap.String("changefeed", s.changefeed), zap.Error(rbErr))
				}
			}
			return err
		}

		// If interplated SQL size exceeds maxAllowedPacket, mysql driver will
		// fall back to the sequantial way.
		// error can be ErrPrepareMulti, ErrBadConn etc.
		// TODO: add a quick path to check whether we should fallback to
		// the sequence way.
		// if s.cfg.MultiStmtEnable && !fallbackToSeqWay {
		// 	err = s.multiStmtExecute(pctx, dmls, tx, writeTimeout)
		// 	if err != nil {
		// 		fallbackToSeqWay = true
		// 		return 0, 0, err
		// 	}
		// } else {
		err = w.sequenceExecute(ctx, dmls, tx, 20*time.Second)
		if err != nil {
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}
		log.Debug("Exec Rows succeeded")
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(8))
}

func (w *MysqlWriter) sequenceExecute(
	ctx context.Context, dmls *preparedDMLs, tx *sql.Tx, writeTimeout time.Duration,
) error {
	for i, query := range dmls.sqls {
		args := dmls.values[i]
		log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
		ctx, cancelFunc := context.WithTimeout(ctx, writeTimeout)

		_, err := tx.ExecContext(ctx, query, args...)

		if err != nil {
			log.Error("err", err)
			if rbErr := tx.Rollback(); rbErr != nil {
				if errors.Cause(rbErr) != context.Canceled {
					log.Warn("failed to rollback txn", zap.Error(rbErr))
				}
			}
			cancelFunc()
			return err
		}
		cancelFunc()
	}
	return nil
}
