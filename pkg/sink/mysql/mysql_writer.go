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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
)

const (
	defaultDDLMaxRetry uint64 = 20

	// networkDriftDuration is used to construct a context timeout for database operations.
	networkDriftDuration = 5 * time.Second
)

// MysqlWriter is responsible for writing various dml events, ddl events, syncpoint events to mysql downstream.
type MysqlWriter struct {
	ctx          context.Context
	db           *sql.DB
	cfg          *MysqlConfig
	ChangefeedID common.ChangeFeedID

	syncPointTableInit     bool
	lastCleanSyncPointTime time.Time

	ddlTsTableInit   bool
	tableSchemaStore *util.TableSchemaStore

	// implement stmtCache to improve performance, especially when the downstream is TiDB
	stmtCache *lru.Cache
	// Indicate if the CachePrepStmts should be enabled or not
	cachePrepStmts   bool
	maxAllowedPacket int64

	statistics *metrics.Statistics
}

func NewMysqlWriter(ctx context.Context, db *sql.DB, cfg *MysqlConfig, changefeedID common.ChangeFeedID, statistics *metrics.Statistics) *MysqlWriter {
	return &MysqlWriter{
		ctx:                    ctx,
		db:                     db,
		cfg:                    cfg,
		syncPointTableInit:     false,
		ChangefeedID:           changefeedID,
		lastCleanSyncPointTime: time.Now(),
		ddlTsTableInit:         false,
		cachePrepStmts:         cfg.CachePrepStmts,
		maxAllowedPacket:       cfg.MaxAllowedPacket,
		stmtCache:              cfg.stmtCache,
		statistics:             statistics,
	}
}

func (w *MysqlWriter) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	w.tableSchemaStore = tableSchemaStore
}

func (w *MysqlWriter) FlushDDLEvent(event *commonEvent.DDLEvent) error {
	if event.GetDDLType() == timodel.ActionAddIndex && w.cfg.IsTiDB {
		return w.asyncExecAddIndexDDLIfTimeout(event) // todo flush checkpointTs
	}

	if !(event.TiDBOnly && !w.cfg.IsTiDB) {
		err := w.execDDLWithMaxRetries(event)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// We need to record ddl' ts after each ddl for each table in the downstream when sink is mysql-compatible.
	// Only in this way, when the node restart, we can continue sync data from the last ddl ts at least.
	// Otherwise, after restarting, we may sync old data in new schema, which will leading to data loss.

	// We make Flush ddl ts before callback(), in order to make sure the ddl ts is flushed
	// before new checkpointTs will report to maintainer. Therefore, when the table checkpointTs is forward,
	// we can ensure the ddl and ddl ts are both flushed downstream successfully.
	// Thus, when restarting, and we can't find a record for one table, it means the table is dropped.
	err := w.FlushDDLTs(event)
	if err != nil {
		return err
	}

	for _, callback := range event.PostTxnFlushed {
		callback()
	}
	return nil
}

func (w *MysqlWriter) FlushDDLTs(event *commonEvent.DDLEvent) error {
	if !w.ddlTsTableInit {
		// create checkpoint ts table if not exist
		err := w.CreateDDLTsTable()
		if err != nil {
			return err
		}
		w.ddlTsTableInit = true
	}

	err := w.SendDDLTs(event)
	return errors.Trace(err)
}

func (w *MysqlWriter) FlushSyncPointEvent(event *commonEvent.SyncPointEvent) error {
	if !w.syncPointTableInit {
		// create sync point table if not exist
		err := w.CreateSyncTable()
		if err != nil {
			return errors.Trace(err)
		}
		w.syncPointTableInit = true
	}
	err := w.SendSyncPointEvent(event)
	if err != nil {
		return errors.Trace(err)
	}
	for _, callback := range event.PostTxnFlushed {
		callback()
	}
	return nil
}

func (w *MysqlWriter) SendSyncPointEvent(event *commonEvent.SyncPointEvent) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
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
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table; Failed to get tidb_current_ts;"))
	}
	// insert ts map
	var builder strings.Builder
	builder.WriteString("insert ignore into ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.SyncPointTable)
	builder.WriteString(" (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('")
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
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write syncpoint table; Exec Failed; Query is %s", query)))
	}

	// set global tidb_external_ts to secondary ts
	// TiDB supports tidb_external_ts system variable since v6.4.0.
	query = fmt.Sprintf("set global tidb_external_ts = %s", secondaryTs)
	_, err = tx.Exec(query)
	if err != nil {
		if apperror.IsSyncPointIgnoreError(err) {
			// TODO(dongmen): to confirm if we need to log this error.
			log.Warn("set global external ts failed, ignore this error", zap.Error(err))
		} else {
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write syncpoint table", zap.Error(err2))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write syncpoint table; Exec Failed; Query is %s", query)))
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
			log.Error("failed to clean syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err)), zap.Any("query", query))
		} else {
			w.lastCleanSyncPointTime = time.Now()
		}
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table; Commit Fail;"))
}

func (w *MysqlWriter) SendDDLTs(event *commonEvent.DDLEvent) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "ddl ts table: begin Tx fail;"))
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID
	ddlTs := strconv.FormatUint(event.GetCommitTs(), 10)
	var tableIds []int64
	var dropTableIds []int64

	relatedTables := event.GetBlockedTables()

	switch relatedTables.InfluenceType {
	case commonEvent.InfluenceTypeNormal:
		tableIds = append(tableIds, relatedTables.TableIDs...)
	case commonEvent.InfluenceTypeDB:
		ids := w.tableSchemaStore.GetTableIdsByDB(relatedTables.SchemaID)
		tableIds = append(tableIds, ids...)
	case commonEvent.InfluenceTypeAll:
		ids := w.tableSchemaStore.GetAllTableIds()
		tableIds = append(tableIds, ids...)
	}

	dropTables := event.GetNeedDroppedTables()
	if dropTables != nil {
		switch dropTables.InfluenceType {
		case commonEvent.InfluenceTypeNormal:
			dropTableIds = append(dropTableIds, dropTables.TableIDs...)
		case commonEvent.InfluenceTypeDB:
			ids := w.tableSchemaStore.GetTableIdsByDB(dropTables.SchemaID)
			dropTableIds = append(dropTableIds, ids...)
		case commonEvent.InfluenceTypeAll:
			ids := w.tableSchemaStore.GetAllTableIds()
			dropTableIds = append(dropTableIds, ids...)
		}
	}

	addTables := event.GetNeedAddedTables()
	for _, table := range addTables {
		tableIds = append(tableIds, table.TableID)
	}

	if len(tableIds) > 0 {
		// generate query
		//INSERT INTO `tidb_cdc`.`ddl_ts` (ticdc_cluster_id, changefeed, ddl_ts, table_id) values(...) ON DUPLICATE KEY UPDATE ddl_ts=VALUES(ddl_ts), created_at=CURRENT_TIMESTAMP;
		var builder strings.Builder
		builder.WriteString("INSERT INTO ")
		builder.WriteString(filter.TiCDCSystemSchema)
		builder.WriteString(".")
		builder.WriteString(filter.DDLTsTable)
		builder.WriteString(" (ticdc_cluster_id, changefeed, ddl_ts, table_id) VALUES ")

		for idx, tableId := range tableIds {
			builder.WriteString("('")
			builder.WriteString(ticdcClusterID)
			builder.WriteString("', '")
			builder.WriteString(changefeedID)
			builder.WriteString("', '")
			builder.WriteString(ddlTs)
			builder.WriteString("', ")
			builder.WriteString(strconv.FormatInt(tableId, 10))
			builder.WriteString(")")
			if idx < len(tableIds)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(" ON DUPLICATE KEY UPDATE ddl_ts=VALUES(ddl_ts), created_at=CURRENT_TIMESTAMP;")

		query := builder.String()

		_, err = tx.Exec(query)
		if err != nil {
			log.Error("failed to write ddl ts table", zap.Error(err))
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write ddl ts table", zap.Error(err2))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write ddl ts table; Exec Failed; Query is %s", query)))
		}
	} else {
		log.Error("table ids is empty when write ddl ts table, FIX IT", zap.Any("event", event))
	}

	if len(dropTableIds) > 0 {
		// drop item for this tableid
		var builder strings.Builder
		builder.WriteString("DELETE FROM ")
		builder.WriteString(filter.TiCDCSystemSchema)
		builder.WriteString(".")
		builder.WriteString(filter.DDLTsTable)
		builder.WriteString(" WHERE (ticdc_cluster_id, changefeed, table_id) IN (")

		for idx, tableId := range dropTableIds {
			builder.WriteString("('")
			builder.WriteString(ticdcClusterID)
			builder.WriteString("', '")
			builder.WriteString(changefeedID)
			builder.WriteString("', ")
			builder.WriteString(strconv.FormatInt(tableId, 10))
			builder.WriteString(")")
			if idx < len(dropTableIds)-1 {
				builder.WriteString(", ")
			}
		}

		builder.WriteString(")")
		query := builder.String()

		_, err = tx.Exec(query)
		if err != nil {
			log.Error("failed to delete ddl ts item ", zap.Error(err))
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to delete ddl ts item", zap.Error(err2))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
		}
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write ddl ts table; Commit Fail;"))

}

// CheckStartTsList return the startTs list for each table in the tableIDs list.
// For each table,
// If no ddl-ts-v1 table, startTs = 0; -- means the downstream is new
// If have ddl-ts-v1 table, but no row for the changefeed with tableID = 0, startTs = 0; -- means the changefeed is new.
// if have row for the changefeed with tableID = 0, but no this row, startTs =  -1; -- means the table is dropped.(we won't check startTs for a table before it created)
// otherwise, startTs = ddl-ts value
func (w *MysqlWriter) CheckStartTsList(tableIDs []int64) ([]int64, error) {
	retStartTsList := make([]int64, len(tableIDs))
	tableIdIdxMap := make(map[int64]int, 0)
	for i, tableID := range tableIDs {
		tableIdIdxMap[tableID] = i
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID

	var builder strings.Builder
	builder.WriteString("SELECT table_id, ddl_ts FROM ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" WHERE (ticdc_cluster_id, changefeed, table_id) IN (")

	for idx, tableID := range tableIDs {
		builder.WriteString("('")
		builder.WriteString(ticdcClusterID)
		builder.WriteString("', '")
		builder.WriteString(changefeedID)
		builder.WriteString("', ")
		builder.WriteString(strconv.FormatInt(tableID, 10))
		builder.WriteString(")")
		if idx < len(tableIDs)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(")")
	query := builder.String()

	rows, err := w.db.Query(query)
	if err != nil {
		if apperror.IsTableNotExistsErr(err) {
			// If this table is not existed, this means the table is first being synced
			log.Info("ddl ts table is not found",
				zap.String("namespace", w.ChangefeedID.Namespace()),
				zap.String("changefeedID", w.ChangefeedID.Name()),
				zap.Error(err))
			return retStartTsList, nil
		}
		return retStartTsList, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
	}

	defer rows.Close()
	var ddlTs int64
	var tableId int64
	count := 0
	for rows.Next() {
		err := rows.Scan(&tableId, &ddlTs)
		if err != nil {
			return retStartTsList, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
		}
		count += 1
		retStartTsList[tableIdIdxMap[tableId]] = ddlTs
	}

	// if all table does't have this field,
	// we need to check have row for tableID = 0(table trigger event dispatcher)
	// If changefeed has tables, it must have row for tableID = 0;
	if count == 0 {
		builder.Reset()
		builder.WriteString("SELECT ddl_ts FROM ")
		builder.WriteString(filter.TiCDCSystemSchema)
		builder.WriteString(".")
		builder.WriteString(filter.DDLTsTable)
		builder.WriteString(" WHERE (ticdc_cluster_id, changefeed, table_id) IN (")

		builder.WriteString("('")
		builder.WriteString(ticdcClusterID)
		builder.WriteString("', '")
		builder.WriteString(changefeedID)
		builder.WriteString("', ")
		builder.WriteString(strconv.FormatInt(0, 10))
		builder.WriteString(")")
		builder.WriteString(")")
		query = builder.String()

		rows, err = w.db.Query(query)
		if err != nil {
			return retStartTsList, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
		}

		defer rows.Close()
		if rows.Next() {
			// means has record for tableID = 0, but not for this table
			for idx, startTs := range retStartTsList {
				if startTs == 0 {
					retStartTsList[idx] = -1
				}
			}
		}
	}

	return retStartTsList, nil
}

func (w *MysqlWriter) RemoveDDLTsItem() error {
	tx, err := w.db.BeginTx(w.ctx, nil)

	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "select ddl ts table: begin Tx fail;"))
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID

	var builder strings.Builder
	builder.WriteString("DELETE FROM ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" WHERE (ticdc_cluster_id, changefeed) IN (")

	builder.WriteString("('")
	builder.WriteString(ticdcClusterID)
	builder.WriteString("', '")
	builder.WriteString(changefeedID)
	builder.WriteString("')")
	builder.WriteString(")")
	query := builder.String()

	_, err = tx.Exec(query)
	if err != nil {
		log.Error("failed to delete ddl ts item ", zap.Error(err))
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to delete ddl ts item", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
}

func (w *MysqlWriter) isDDLExecuted(tableID int64, ddlTs uint64) (bool, error) {
	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID

	// select * from xx where (ticdc_cluster_id, changefeed, table_id, ddl_ts) in (("xx","xx",x,x));
	var builder strings.Builder
	builder.WriteString("SELECT * FROM ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" WHERE (ticdc_cluster_id, changefeed, table_id, ddl_ts) IN (")

	builder.WriteString("('")
	builder.WriteString(ticdcClusterID)
	builder.WriteString("', '")
	builder.WriteString(changefeedID)
	builder.WriteString("', ")
	builder.WriteString(strconv.FormatInt(tableID, 10))
	builder.WriteString(", ")
	builder.WriteString(strconv.FormatUint(ddlTs, 10))
	builder.WriteString(")")
	builder.WriteString(")")
	query := builder.String()

	rows, err := w.db.Query(query)
	if err != nil {
		return false, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
	}

	defer rows.Close()
	if rows.Next() {
		return true, nil
	}
	return false, nil

}

func (w *MysqlWriter) CreateDDLTsTable() error {
	database := filter.TiCDCSystemSchema
	query := `CREATE TABLE IF NOT EXISTS %s
	(
		ticdc_cluster_id varchar (255),
		changefeed varchar(255),
		ddl_ts varchar(18),
		table_id bigint(21),
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX (ticdc_cluster_id, changefeed, table_id),
		PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
	);`
	query = fmt.Sprintf(query, filter.DDLTsTable)

	return w.CreateTable(database, filter.DDLTsTable, query)
}

func (w *MysqlWriter) CreateSyncTable() error {
	database := filter.TiCDCSystemSchema
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
	return w.CreateTable(database, filter.SyncPointTable, query)
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

	// exchange partition is not Idempotent, so we need to check ddl_ts_table whether the ddl is executed before.
	if timodel.ActionType(event.Type) == timodel.ActionExchangeTablePartition {
		tableID := event.BlockedTables.TableIDs[0]
		ddlTs := event.GetCommitTs()
		flag, err := w.isDDLExecuted(tableID, ddlTs)
		if err != nil {
			return nil
		}
		if flag {
			log.Info("Skip Already Executed DDL", zap.String("sql", event.GetDDLQuery()))
			return nil
		}
	}

	shouldSwitchDB := needSwitchDB(event)

	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(w.ctx, "USE "+common.QuoteName(event.GetDDLSchemaName())+";")
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
	_, err = tx.ExecContext(w.ctx, query)
	if err != nil {
		log.Error("Fail to ExecContext", zap.Any("err", err))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", event.GetDDLQuery()), zap.Error(err))
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", event.GetDDLQuery())))
	}

	log.Info("Exec DDL succeeded", zap.String("sql", event.GetDDLQuery()))
	return nil
}

func (w *MysqlWriter) execDDLWithMaxRetries(event *commonEvent.DDLEvent) error {
	return retry.Do(w.ctx, func() error {
		err := w.statistics.RecordDDLExecution(func() error { return w.execDDL(event) })
		if err != nil {
			if apperror.IsIgnorableMySQLDDLError(err) {
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
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Execute DDL failed, Query info: %s; ", event.GetDDLQuery())))
		}
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(apperror.IsRetryableDDLError))
}

func (w *MysqlWriter) prepareDMLs(events []*commonEvent.DMLEvent) (*preparedDMLs, error) {
	dmls := dmlsPool.Get().(*preparedDMLs)
	dmls.reset()

	for _, event := range events {
		if event.Len() == 0 {
			continue
		}

		dmls.rowCount += int(event.Len())
		dmls.approximateSize += event.GetRowsSize()

		if len(dmls.startTs) == 0 || dmls.startTs[len(dmls.startTs)-1] != event.StartTs {
			dmls.startTs = append(dmls.startTs, event.StartTs)
		}

		translateToInsert := !w.cfg.SafeMode && event.CommitTs > event.ReplicatingTs
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
			var err error

			switch row.RowType {
			case commonEvent.RowTypeUpdate:
				query, args, err = buildUpdate(event.TableInfo, row)
			case commonEvent.RowTypeDelete:
				query, args, err = buildDelete(event.TableInfo, row)
			case commonEvent.RowTypeInsert:
				query, args, err = buildInsert(event.TableInfo, row, translateToInsert)
			}

			if err != nil {
				dmlsPool.Put(dmls) // Return to pool on error
				return nil, errors.Trace(err)
			}

			if query != "" {
				dmls.sqls = append(dmls.sqls, query)
				dmls.values = append(dmls.values, args)
			}
		}
	}

	return dmls, nil
}

func (w *MysqlWriter) Flush(events []*commonEvent.DMLEvent, workerNum int) error {
	w.statistics.ObserveRows(events)
	dmls, err := w.prepareDMLs(events)
	if err != nil {
		return errors.Trace(err)
	}
	defer dmlsPool.Put(dmls) // Return dmls to pool after use

	if dmls.rowCount == 0 {
		return nil
	}

	if !w.cfg.DryRun {
		if err := w.execDMLWithMaxRetries(dmls); err != nil {
			return errors.Trace(err)
		}
	} else {
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

func (w *MysqlWriter) execDMLWithMaxRetries(dmls *preparedDMLs) error {
	if len(dmls.sqls) != len(dmls.values) {
		return cerror.ErrUnexpected.FastGenByArgs(fmt.Sprintf("unexpected number of sqls and values, sqls is %s, values is %s", dmls.sqls, dmls.values))
	}

	// approximateSize is multiplied by 2 because in extreme circustumas, every
	// byte in dmls can be escaped and adds one byte.
	fallbackToSeqWay := dmls.approximateSize*2 > w.maxAllowedPacket

	writeTimeout, _ := time.ParseDuration(w.cfg.WriteTimeout)
	writeTimeout += networkDriftDuration

	tryExec := func() (int, int64, error) {
		tx, err := w.db.BeginTx(w.ctx, nil)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}

		// Set session variables first and then execute the transaction.
		// we try to set write source for each txn,
		// so we can use it to trace the data source
		if err = SetWriteSource(w.cfg, tx); err != nil {
			log.Error("Failed to set write source", zap.Error(err))
			if rbErr := tx.Rollback(); rbErr != nil {
				if errors.Cause(rbErr) != context.Canceled {
					log.Warn("failed to rollback txn", zap.Error(rbErr))
				}
			}
			return 0, 0, err
		}

		if !fallbackToSeqWay {
			err = w.multiStmtExecute(dmls, tx, writeTimeout)
			if err != nil {
				fallbackToSeqWay = true
				return 0, 0, err
			}
		} else {
			err = w.sequenceExecute(dmls, tx, writeTimeout)
			if err != nil {
				return 0, 0, err
			}
		}

		if err = tx.Commit(); err != nil {
			return 0, 0, err
		}
		log.Debug("Exec Rows succeeded")
		return dmls.rowCount, dmls.approximateSize, nil
	}
	return retry.Do(w.ctx, func() error {
		err := w.statistics.RecordBatchExecution(tryExec)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(w.cfg.DMLMaxRetry))
}

func (w *MysqlWriter) sequenceExecute(
	dmls *preparedDMLs, tx *sql.Tx, writeTimeout time.Duration,
) error {
	for i, query := range dmls.sqls {
		args := dmls.values[i]
		log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
		ctx, cancelFunc := context.WithTimeout(w.ctx, writeTimeout)

		var prepStmt *sql.Stmt
		if w.cachePrepStmts {
			if stmt, ok := w.stmtCache.Get(query); ok {
				prepStmt = stmt.(*sql.Stmt)
			} else if stmt, err := w.db.Prepare(query); err == nil {
				prepStmt = stmt
				w.stmtCache.Add(query, stmt)
			} else {
				// Generally it means the downstream database doesn't allow
				// too many preapred statements. So clean some of them.
				w.stmtCache.RemoveOldest()
			}
		}

		var execError error
		if prepStmt == nil {
			_, execError = tx.ExecContext(ctx, query, args...)
		} else {
			//nolint:sqlclosecheck
			_, execError = tx.Stmt(prepStmt).ExecContext(ctx, args...)
		}

		if execError != nil {
			log.Error("ExecContext", zap.Error(execError), zap.Any("dmls", dmls))
			if rbErr := tx.Rollback(); rbErr != nil {
				if errors.Cause(rbErr) != context.Canceled {
					log.Warn("failed to rollback txn", zap.Error(rbErr))
				}
			}
			cancelFunc()
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(execError, fmt.Sprintf("Failed to execute DMLs, query info:%s, args:%v; ", query, args)))
		}
		cancelFunc()
	}
	return nil
}

// execute SQLs in the multi statements way.
func (w *MysqlWriter) multiStmtExecute(
	dmls *preparedDMLs, tx *sql.Tx, writeTimeout time.Duration,
) error {
	var multiStmtArgs []any
	for _, value := range dmls.values {
		multiStmtArgs = append(multiStmtArgs, value...)
	}
	multiStmtSQL := strings.Join(dmls.sqls, ";")

	ctx, cancel := context.WithTimeout(w.ctx, writeTimeout)
	defer cancel()

	_, err := tx.ExecContext(ctx, multiStmtSQL, multiStmtArgs...)
	if err != nil {
		log.Error("ExecContext", zap.Error(err), zap.Any("multiStmtSQL", multiStmtSQL), zap.Any("multiStmtArgs", multiStmtArgs))
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Warn("failed to rollback txn", zap.Error(rbErr))
			}
		}
		cancel()
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Failed to execute DMLs, query info:%s, args:%v; ", multiStmtSQL, multiStmtArgs)))
	}
	return nil
}

func (w *MysqlWriter) CreateTable(dbName string, tableName string, createTableQuery string) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: begin Tx fail;", tableName)))
	}

	// we try to set cdc write source for the ddl
	if err = SetWriteSource(w.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback", zap.Error(err))
			}
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: set write source fail;", tableName)))
	}

	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to create %s table;", tableName)))
	}
	_, err = tx.Exec("USE " + dbName)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: use %s db fail;", tableName, dbName)))
	}

	_, err = tx.Exec(createTableQuery)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: Exec fail; Query is %s", tableName, createTableQuery)))
	}
	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: Commit Failed; Query is %s", tableName, createTableQuery)))
}

func (w *MysqlWriter) Close() {
	if w.stmtCache != nil {
		w.stmtCache.Purge()
	}
}
