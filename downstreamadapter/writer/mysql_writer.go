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
	"strconv"
	"strings"
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/retry"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
)

const (
	defaultDDLMaxRetry uint64 = 20
)

// 用于给 mysql 类型的下游做 flush, 主打一个粗糙，先能跑起来再说
type MysqlWriter struct {
	db                     *sql.DB
	cfg                    *MysqlConfig
	syncPointTableInit     bool
	statistics             *metrics.Statistics
	ChangefeedID           model.ChangeFeedID
	lastCleanSyncPointTime time.Time
}

func NewMysqlWriter(db *sql.DB, cfg *MysqlConfig, changefeedID model.ChangeFeedID) *MysqlWriter {
	statistics := metrics.NewStatistics(changefeedID, "TxnSink")
	return &MysqlWriter{
		db:                     db,
		cfg:                    cfg,
		syncPointTableInit:     false,
		ChangefeedID:           changefeedID,
		lastCleanSyncPointTime: time.Now(),
		statistics:             statistics,
	}
}

func (w *MysqlWriter) FlushDDLEvent(event *commonEvent.DDLEvent) error {
	if event.GetDDLType() == timodel.ActionAddIndex && w.cfg.IsTiDB {
		return w.asyncExecAddIndexDDLIfTimeout(event)
	}
	err := w.execDDLWithMaxRetries(event)
	if err != nil {
		log.Error("exec ddl failed", zap.Error(err))
		return err
	}

	for _, callback := range event.PostTxnFlushed {
		callback()
	}
	return nil
}

func (w *MysqlWriter) FlushSyncPointEvent(event *commonEvent.SyncPointEvent) error {
	if !w.syncPointTableInit {
		// create sync point table if not exist
		err := w.CreateSyncTable(context.Background())
		if err != nil {
			log.Error("create sync table failed", zap.Error(err))
			return err
		}
		w.syncPointTableInit = true
	}
	err := w.SendSyncPointEvent(event)
	if err != nil {
		log.Error("send syncpoint event failed", zap.Error(err))
		return err
	}
	for _, callback := range event.PostTxnFlushed {
		callback()
	}
	return nil
}

func (w *MysqlWriter) SendSyncPointEvent(event *commonEvent.SyncPointEvent) error {
	ctx := context.Background()
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("sync table: begin Tx fail", zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "sync table: begin Tx fail;"))
	}
	row := tx.QueryRow("select @@tidb_current_ts")
	var secondaryTs string
	err = row.Scan(&secondaryTs)
	if err != nil {
		log.Info("sync table: get tidb_current_ts err", zap.String("changefeed", w.ChangefeedID.String()))
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to write syncpoint table", zap.Error(err))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table;"))
	}
	// insert ts map
	var builder strings.Builder
	builder.WriteString("insert ignore into ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.SyncPointTable)
	builder.WriteString("(ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('")
	builder.WriteString(config.GetGlobalServerConfig().ClusterID)
	builder.WriteString("', '")
	builder.WriteString(w.ChangefeedID.String())
	builder.WriteString("', ")
	builder.WriteString(strconv.FormatUint(event.GetCommitTs(), 10))
	builder.WriteString(", ")
	builder.WriteString(secondaryTs)
	builder.WriteString(")")
	query := builder.String()

	_, err = tx.Exec(query)
	if err != nil {
		log.Error("failed to write syncpoint table", zap.Error(err))
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to write syncpoint table", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table;"))
	}

	// set global tidb_external_ts to secondary ts
	// TiDB supports tidb_external_ts system variable since v6.4.0.
	query = fmt.Sprintf("set global tidb_external_ts = %s", secondaryTs)
	_, err = tx.Exec(query)
	if err != nil {
		if errorutil.IsSyncPointIgnoreError(err) {
			// TODO(dongmen): to confirm if we need to log this error.
			log.Warn("set global external ts failed, ignore this error", zap.Error(err))
		} else {
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write syncpoint table", zap.Error(err2))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table;"))
		}
	}

	// clean stale ts map in downstream
	if time.Since(w.lastCleanSyncPointTime) >= w.cfg.SyncPointRetention {
		var builder strings.Builder
		builder.WriteString("DELETE IGNORE FROM ")
		builder.WriteString(filter.TiCDCSystemSchema)
		builder.WriteString(".")
		builder.WriteString(filter.SyncPointTable)
		builder.WriteString(" WHERE ticdc_cluster_id = '")
		builder.WriteString(config.GetGlobalServerConfig().ClusterID)
		builder.WriteString("' and changefeed = '")
		builder.WriteString(w.ChangefeedID.String())
		builder.WriteString("' and created_at < (NOW() - INTERVAL ")
		builder.WriteString(fmt.Sprintf("%.2f", w.cfg.SyncPointRetention.Seconds()))
		builder.WriteString(" SECOND)")
		query := builder.String()

		_, err = tx.Exec(query)
		if err != nil {
			// It is ok to ignore the error, since it will not affect the correctness of the system,
			// and no any business logic depends on this behavior, so we just log the error.
			log.Error("failed to clean syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err)))
		} else {
			w.lastCleanSyncPointTime = time.Now()
		}
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table;"))
}

func (w *MysqlWriter) CreateSyncTable(ctx context.Context) error {
	database := filter.TiCDCSystemSchema
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("create sync table: begin Tx fail", zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "create sync table: begin Tx fail;"))
	}

	// we try to set cdc write source for the ddl
	if err = SetWriteSource(w.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback", zap.Error(err))
			}
		}
		return err
	}

	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to create syncpoint table", zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to create syncpoint table;"))
	}
	_, err = tx.Exec("USE " + database)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to create syncpoint table", zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to create syncpoint table;"))
	}
	query := `CREATE TABLE IF NOT EXISTS %s
	(
		ticdc_cluster_id varchar (255),
		changefeed varchar(255),
		primary_ts varchar(18),
		secondary_ts varchar(18),
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX (created_at),
		PRIMARY KEY (changefeed, primary_ts)
	);`
	query = fmt.Sprintf(query, filter.SyncPointTable)
	_, err = tx.Exec(query)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to create syncpoint table", zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to create syncpoint table;"))
	}
	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to create syncpoint table;"))
}

func (w *MysqlWriter) asyncExecAddIndexDDLIfTimeout(event *commonEvent.DDLEvent) error {
	done := make(chan error, 1)
	// wait for 2 seconds at most
	tick := time.NewTimer(2 * time.Second)
	defer tick.Stop()
	log.Info("async exec add index ddl start",
		zap.Uint64("commitTs", event.FinishedTs),
		zap.String("ddl", event.GetDDLQuery()))
	go func() {
		if err := w.execDDLWithMaxRetries(event); err != nil {
			log.Error("async exec add index ddl failed",
				zap.Uint64("commitTs", event.FinishedTs),
				zap.String("ddl", event.GetDDLQuery()))
			done <- err
			return
		}
		log.Info("async exec add index ddl done",
			zap.Uint64("commitTs", event.FinishedTs),
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
			zap.Uint64("commitTs", event.FinishedTs),
			zap.String("ddl", event.GetDDLQuery()))
		return nil
	}
}

func (w *MysqlWriter) execDDL(event *commonEvent.DDLEvent) error {
	if w.cfg.DryRun {
		log.Info("Dry run DDL", zap.String("sql", event.GetDDLQuery()))
		return nil
	}

	shouldSwitchDB := needSwitchDB(event)

	ctx := context.Background()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+common.QuoteName(event.GetDDLSchemaName())+";")
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return err
		}
	}

	// we try to set cdc write source for the ddl
	if err = SetWriteSource(w.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback", zap.Error(err))
			}
		}
		return err
	}

	query := event.GetDDLQuery()
	_, err = tx.ExecContext(ctx, query)
	if err != nil {
		log.Error("Fail to ExecContext", zap.Any("err", err))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", event.GetDDLQuery()), zap.Error(err))
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		log.Error("Failed to exec DDL", zap.String("sql", event.GetDDLQuery()), zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", event.GetDDLQuery())))
	}

	log.Info("Exec DDL succeeded", zap.String("sql", event.GetDDLQuery()))
	return nil
}

func (w *MysqlWriter) execDDLWithMaxRetries(event *commonEvent.DDLEvent) error {
	return retry.Do(context.Background(), func() error {
		err := w.statistics.RecordDDLExecution(func() error { return w.execDDL(event) })
		if err != nil {
			if errorutil.IsIgnorableMySQLDDLError(err) {
				// NOTE: don't change the log, some tests depend on it.
				log.Info("Execute DDL failed, but error can be ignored",
					zap.String("ddl", event.Query),
					zap.Error(err))
				// If the error is ignorable, we will ignore the error directly.
				return nil
			}
			log.Warn("Execute DDL with error, retry later",
				zap.String("ddl", event.Query),
				zap.Error(err))
			return err
		}
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(errorutil.IsRetryableDDLError))
}

func (w *MysqlWriter) Flush(events []*commonEvent.DMLEvent, workerNum int) error {
	w.statistics.ObserveRows(events)
	dmls := w.prepareDMLs(events)
	//log.Debug("prepare DMLs", zap.Any("dmlsCount", dmls.rowCount), zap.Any("dmls", fmt.Sprintf("%v", dmls.sqls)), zap.Any("values", dmls.values), zap.Any("startTs", dmls.startTs), zap.Any("workerNum", workerNum))
	if dmls.rowCount == 0 {
		return nil
	}

	if !w.cfg.DryRun {
		if err := w.execDMLWithMaxRetries(dmls); err != nil {
			log.Error("execute DMLs failed", zap.Error(err))
			return errors.Trace(err)
		}
	} else {
		// dry run mode, just record the metrics
		w.statistics.RecordBatchExecution(func() (int, int64, error) {
			return dmls.rowCount, dmls.approximateSize, nil
		})
	}

	for _, event := range events {
		for _, callback := range event.PostTxnFlushed {
			callback()
		}
	}
	return nil
}

func (w *MysqlWriter) prepareDMLs(events []*commonEvent.DMLEvent) *preparedDMLs {
	// TODO: use a sync.Pool to reduce allocations.
	startTs := make([]uint64, 0)
	sqls := make([]string, 0)
	values := make([][]interface{}, 0)
	rowCount := 0
	approximateSize := int64(0)

	for _, event := range events {
		if event.Len() == 0 {
			continue
		}
		// For metrics and logging.
		rowCount += event.Len()
		approximateSize += event.GetSize()
		if len(startTs) == 0 || startTs[len(startTs)-1] != event.StartTs {
			startTs = append(startTs, event.StartTs)
		}

		// translateToInsert control the update and insert behavior.
		translateToInsert := !w.cfg.SafeMode
		translateToInsert = translateToInsert && event.CommitTs > event.ReplicatingTs
		log.Debug("translate to insert",
			zap.Bool("translateToInsert", translateToInsert),
			zap.Uint64("firstRowCommitTs", event.CommitTs),
			zap.Uint64("firstRowReplicatingTs", event.ReplicatingTs),
			zap.Bool("safeMode", w.cfg.SafeMode))
		for {
			row, ok := event.GetNextRow()
			if !ok {
				break
			}
			var query string
			var args []interface{}
			// Update Event
			if row.RowType == commonEvent.RowTypeUpdate {
				query, args = buildUpdate(event.TableInfo, row)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
				continue
			}

			// Delete Event
			if row.RowType == commonEvent.RowTypeDelete {
				query, args = buildDelete(event.TableInfo, row)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
			}

			// Insert Event
			// It will be translated directly into a
			// INSERT(not in safe mode)
			// or REPLACE(in safe mode) SQL.
			if row.RowType == commonEvent.RowTypeInsert {
				query, args = buildInsert(event.TableInfo, row, w.cfg.SafeMode)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
			}
		}
	}

	return &preparedDMLs{
		sqls:            sqls,
		values:          values,
		rowCount:        rowCount,
		approximateSize: approximateSize,
		startTs:         startTs,
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
	tryExec := func() (int, int64, error) {
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			log.Error("BeginTx", zap.Error(err))
			return 0, 0, err
		}

		// Set session variables first and then execute the transaction.
		// we try to set write source for each txn,
		// so we can use it to trace the data source
		if err = SetWriteSource(w.cfg, tx); err != nil {
			log.Error("SetWriteSource", zap.Error(err))
			if rbErr := tx.Rollback(); rbErr != nil {
				if errors.Cause(rbErr) != context.Canceled {
					log.Warn("failed to rollback txn", zap.Error(rbErr))
				}
			}
			return 0, 0, err
		}

		err = w.multiStmtExecute(ctx, dmls, tx, 20*time.Second)
		if err != nil {
			return 0, 0, err
		}

		if err = tx.Commit(); err != nil {
			return 0, 0, err
		}
		log.Debug("Exec Rows succeeded")
		return dmls.rowCount, dmls.approximateSize, nil
	}
	return retry.Do(ctx, func() error {
		err := w.statistics.RecordBatchExecution(tryExec)
		if err != nil {
			log.Error("RecordBatchExecution", zap.Error(err))
			return err
		}
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
			log.Error("ExecContext", zap.Error(err), zap.Any("dmls", dmls))
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

// execute SQLs in the multi statements way.
func (w *MysqlWriter) multiStmtExecute(
	ctx context.Context, dmls *preparedDMLs, tx *sql.Tx, writeTimeout time.Duration,
) error {
	var multiStmtArgs []any
	for _, value := range dmls.values {
		multiStmtArgs = append(multiStmtArgs, value...)
	}
	multiStmtSQL := strings.Join(dmls.sqls, ";")

	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	//start := time.Now()
	_, err := tx.ExecContext(ctx, multiStmtSQL, multiStmtArgs...)
	if err != nil {
		log.Error("ExecContext", zap.Error(err), zap.Any("multiStmtSQL", multiStmtSQL), zap.Any("multiStmtArgs", multiStmtArgs))
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Warn("failed to rollback txn", zap.Error(rbErr))
			}
		}
		cancel()
		return err
	}
	return nil
}
