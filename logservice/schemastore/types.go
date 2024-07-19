package schemastore

import (
	"math"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tidb/pkg/parser/model"
)

type DispatcherID string

type TableID int64

type DatabaseID int64

type SchemaID int64

type Filter interface{}

type DDLEvent struct {
	Job *model.Job `json:"ddl_job"`
	// commitTS of the rawKV
	CommitTS common.Ts `json:"commit_ts"`
}

type DispatcherInfo struct {
	tableID TableID
	// filter is used to filter specific event types of the table
	filter Filter
}

type DatabaseInfo struct {
	Name          string
	Tables        []TableID
	CreateVersion common.Ts
	DeleteVersion common.Ts
}

func (d *DatabaseInfo) isDeleted() bool { return d.DeleteVersion != math.MaxUint64 }

type DatabaseInfoMap map[DatabaseID]*DatabaseInfo

type TableInfoStoreMap map[TableID]*versionedTableInfoStore

type DispatcherInfoMap map[DispatcherID]DispatcherInfo
