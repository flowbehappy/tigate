package schemastore

import (
	"math"

	"github.com/pingcap/tidb/pkg/parser/model"
)

type DispatcherID string

type Timestamp uint64

type TableID int64

type DatabaseID int64

type SchemaID int64

type Filter interface{}

type DDLEvent struct {
	Job *model.Job `json:"ddl_job"`
	// commitTS of the rawKV
	CommitTS Timestamp `json:"commit_ts"`
}

type DispatcherInfo struct {
	tableID TableID
	filter  Filter
}

type DatabaseInfo struct {
	ID            int64
	Name          string
	Tables        []TableID
	CreateVersion Timestamp
	DeleteVersion Timestamp
}

func (d *DatabaseInfo) isDeleted() bool { return d.DeleteVersion != math.MaxUint64 }

type DatabaseInfoMap map[DatabaseID]*DatabaseInfo

type TableInfoStoreMap map[TableID]*versionedTableInfoStore

type DispatcherInfoMap map[DispatcherID]DispatcherInfo
