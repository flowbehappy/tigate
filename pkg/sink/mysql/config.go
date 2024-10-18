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
	"net/url"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	txnModeOptimistic  = "optimistic"
	txnModePessimistic = "pessimistic"

	// DefaultWorkerCount is the default number of workers.
	DefaultWorkerCount = 16
	// DefaultMaxTxnRow is the default max number of rows in a transaction.
	DefaultMaxTxnRow = 256
	// defaultMaxMultiUpdateRowCount is the default max number of rows in a
	// single multi update SQL.
	defaultMaxMultiUpdateRowCount = 40
	// defaultMaxMultiUpdateRowSize(1KB) defines the default value of MaxMultiUpdateRowSize
	// When row average size is larger MaxMultiUpdateRowSize,
	// disable multi update, otherwise enable multi update.
	defaultMaxMultiUpdateRowSize = 1024
	// The upper limit of max worker counts.
	maxWorkerCount = 1024
	// The upper limit of max txn rows.
	maxMaxTxnRow = 2048
	// The upper limit of max multi update rows in a single SQL.
	maxMaxMultiUpdateRowCount = 256
	// The upper limit of max multi update row size(8KB).
	maxMaxMultiUpdateRowSize = 8192

	defaultTiDBTxnMode    = txnModeOptimistic
	defaultReadTimeout    = "2m"
	defaultWriteTimeout   = "2m"
	defaultDialTimeout    = "2m"
	defaultSafeMode       = false
	defaultTxnIsolationRC = "READ-COMMITTED"
	defaultCharacterSet   = "utf8mb4"

	// BackoffBaseDelay indicates the base delay time for retrying.
	BackoffBaseDelay = 500 * time.Millisecond
	// BackoffMaxDelay indicates the max delay time for retrying.
	BackoffMaxDelay = 60 * time.Second

	defaultBatchDMLEnable  = true
	defaultMultiStmtEnable = true

	// defaultcachePrepStmts is the default value of cachePrepStmts
	defaultCachePrepStmts = true
)

type MysqlConfig struct {
	sinkURI                *url.URL
	WorkerCount            int
	MaxTxnRow              int
	MaxMultiUpdateRowCount int
	MaxMultiUpdateRowSize  int
	tidbTxnMode            string
	ReadTimeout            string
	WriteTimeout           string
	DialTimeout            string
	SafeMode               bool
	Timezone               string
	TLS                    string
	ForceReplicate         bool

	IsTiDB bool // IsTiDB is true if the downstream is TiDB
	// IsBDRModeSupported is true if the downstream is TiDB and write source is existed.
	// write source exists when the downstream is TiDB and version is greater than or equal to v6.5.0.
	IsWriteSourceExisted bool

	SourceID        uint64
	BatchDMLEnable  bool
	MultiStmtEnable bool
	CachePrepStmts  bool
	// DryRun is used to enable dry-run mode. In dry-run mode, the writer will not write data to the downstream.
	DryRun bool

	// sync point
	SyncPointRetention time.Duration
}

// NewConfig returns the default mysql backend config.
func NewMysqlConfig() *MysqlConfig {
	return &MysqlConfig{
		WorkerCount:            DefaultWorkerCount,
		MaxTxnRow:              DefaultMaxTxnRow,
		MaxMultiUpdateRowCount: defaultMaxMultiUpdateRowCount,
		MaxMultiUpdateRowSize:  defaultMaxMultiUpdateRowSize,
		tidbTxnMode:            defaultTiDBTxnMode,
		ReadTimeout:            defaultReadTimeout,
		WriteTimeout:           defaultWriteTimeout,
		DialTimeout:            defaultDialTimeout,
		SafeMode:               defaultSafeMode,
		BatchDMLEnable:         defaultBatchDMLEnable,
		MultiStmtEnable:        defaultMultiStmtEnable,
		CachePrepStmts:         defaultCachePrepStmts,
		SourceID:               config.DefaultTiDBSourceID,
	}
}

func (c *MysqlConfig) Apply(sinkURI *url.URL) error {
	if sinkURI == nil {
		log.Error("empty SinkURI")
		return cerror.ErrMySQLInvalidConfig.GenWithStack("fail to open MySQL sink, empty SinkURI")
	}
	c.sinkURI = sinkURI
	return nil
}

func NewMysqlConfigAndDB(ctx context.Context, sinkURI *url.URL) (*MysqlConfig, *sql.DB, error) {
	log.Info("create db connection", zap.String("sinkURI", sinkURI.String()))
	// create db connection
	cfg := NewMysqlConfig()
	err := cfg.Apply(sinkURI)
	if err != nil {
		return nil, nil, err
	}
	dsnStr, err := GenerateDSN(cfg)
	if err != nil {
		return nil, nil, err
	}

	db, err := CreateMysqlDBConn(dsnStr)
	if err != nil {
		return nil, nil, err
	}

	cfg.IsTiDB = CheckIsTiDB(ctx, db)

	cfg.IsWriteSourceExisted, err = CheckIfBDRModeIsSupported(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	// TODO：param setting
	return cfg, db, nil
}
