package schemastore

import (
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tidb/pkg/parser/model"
)

type DDLEvent struct {
	Job *model.Job `json:"ddl_job"`
	// commitTS of the rawKV
	CommitTS common.Ts `json:"commit_ts"`
}

type DatabaseInfo struct {
	Name          string
	Tables        map[common.TableID]bool
	CreateVersion common.Ts
	DeleteVersion common.Ts
}

func (d *DatabaseInfo) needGc(gcTs common.Ts) bool { return d.DeleteVersion < gcTs }

type TableInfoEntry struct {
	Version   int    `json:"version"`
	SchemaID  int64  `json:"schema_id"`
	TableInfo []byte `json:"table_info"`
}
