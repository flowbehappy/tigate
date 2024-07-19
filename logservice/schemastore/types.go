package schemastore

import (
	"math"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tidb/pkg/parser/model"
)

type Filter interface{}

type DDLEvent struct {
	Job *model.Job `json:"ddl_job"`
	// commitTS of the rawKV
	CommitTS common.Ts `json:"commit_ts"`
}

type DispatcherInfo struct {
	tableID common.TableID
	// filter is used to filter specific event types of the table
	filter Filter
}

type DatabaseInfo struct {
	Name          string
	Tables        []common.TableID
	CreateVersion common.Ts
	DeleteVersion common.Ts
}

func (d *DatabaseInfo) isDeleted() bool { return d.DeleteVersion != math.MaxUint64 }

type DatabaseInfoMap map[common.DatabaseID]*DatabaseInfo

type TableInfoStoreMap map[common.TableID]*versionedTableInfoStore

type DispatcherInfoMap map[common.DispatcherID]DispatcherInfo
