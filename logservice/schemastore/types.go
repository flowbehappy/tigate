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

type SchemaIDWithVersion struct {
	SchemaID      common.SchemaID
	CreateVersion common.Ts
}

type TableNameWithVersion struct {
	Name          string
	CreateVersion common.Ts
}

type VersionedTableBasicInfo struct {
	SchemaIDs     []SchemaIDWithVersion
	Names         []TableNameWithVersion
	CreateVersion common.Ts
}
