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
	"database/sql"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

// 用于给 mysql 类型的下游做 flush
type MysqlWriter struct {
	db  *sql.DB
	cfg *MysqlConfig
}

func NewMysqlWriter(cfg *MysqlConfig) *MysqlWriter {

}

func (w *MysqlWriter) FlushDDLEvent(event *Event) error {
}

func (w *MysqlWriter) Flush(events []*Event) error {
	sqls := w.prepareSQLs(events)

	if err := w.execWithMaxRetries(sqls); err != nil {
		log.Error("execute DMLs failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (w *MysqlWriter) prepareSQLs(events []*Event) *SQLs {
	// TODO: 根据 event 创建 DDL 或者 DMLs
}

func (w *MysqlWriter) execWithMaxRetries(sqls *SQLs) error {
	// TODO: 根据 dmls 向下游执行 SQLs
}
