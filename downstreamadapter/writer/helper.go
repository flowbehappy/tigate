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

	"github.com/ngaut/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type preparedDMLs struct {
	startTs         []model.Ts
	sqls            []string
	values          [][]interface{}
	callbacks       []dmlsink.CallbackFunc
	rowCount        int
	approximateSize int64
}

// return dsn
func generateDSN(cfg *MysqlConfig) string {

}

func createMysqlDBConn(cfg *MysqlConfig) (*sql.DB, error) {
	dsnStr := generateDSN(cfg)
	db, err := sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}

	err = db.PingContext(context.Background())
	if err != nil {
		// close db to recycle resources
		if closeErr := db.Close(); closeErr != nil {
			log.Warn("close db failed", zap.Error(err))
		}
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}
	return db, nil
}

func needSwitchDB(event *Event) bool {
	if len(event.TableInfo.TableName.Schema) == 0 {
		return false
	}
	if event.GetDDLType() == timodel.ActionCreateSchema || event.GetDDLType() == timodel.ActionDropSchema {
		return false
	}
	return true
}
