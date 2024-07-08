package schemastore

import (
	"errors"
	"math"
	"sync"

	"github.com/flowbehappy/tigate/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	datumTypes "github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

// dbInfo interface should be much simpler?
// dbInfo 会变吗？db id 不会被复用吧？所以不存在多版本？

type tableInfoItem struct {
	ts   common.Timestamp
	info *common.TableInfo
}

type versionedTableInfoStore struct {
	mu sync.Mutex

	startTS common.Timestamp

	dispatchers map[common.DispatcherID]common.Timestamp

	// ordered by ts
	infos []tableInfoItem

	deleteVersion common.Timestamp
}

func newEmptyVersionedTableInfoStore() *versionedTableInfoStore {
	return &versionedTableInfoStore{
		dispatchers:   make(map[common.DispatcherID]common.Timestamp),
		infos:         make([]tableInfoItem, 0),
		deleteVersion: math.MaxUint64,
	}
}

// TableInfo can from upstream snapshot or create table ddl
func newVersionedTableInfoStore(startTS common.Timestamp, info *common.TableInfo) *versionedTableInfoStore {
	store := newEmptyVersionedTableInfoStore()
	store.infos = append(store.infos, tableInfoItem{ts: startTS, info: info})
	return store
}

func (v *versionedTableInfoStore) getStartTS() common.Timestamp {
	v.mu.Lock()
	defer v.mu.Unlock()
	// this is not very accurate, it's large than the actual startTS
	return v.startTS
}

func (v *versionedTableInfoStore) getTableInfo(ts common.Timestamp) (*common.TableInfo, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// TODO: 检查边界条件
	left := findTableInfoIndex(v.infos, ts)
	if left == 0 {
		return nil, errors.New("no table info found")
	}
	return v.infos[left-1].info, nil
}

func (v *versionedTableInfoStore) registerDispatcher(dispatcherID common.DispatcherID, ts common.Timestamp) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.dispatchers[dispatcherID]; ok {
		log.Info("dispatcher already registered", zap.String("dispatcherID", string(dispatcherID)))
	}
	v.dispatchers[dispatcherID] = ts
	v.tryGC()
}

// may trigger gc, or totally removed?
func (v *versionedTableInfoStore) unregisterDispatcher(dispatcherID common.DispatcherID) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.dispatchers, dispatcherID)
	if len(v.dispatchers) == 0 {
		return true
	}
	v.tryGC()
	return false
}

// will trigger gc
func (v *versionedTableInfoStore) updateDispatcherCheckpointTS(dispatcherID common.DispatcherID, ts common.Timestamp) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.dispatchers[dispatcherID]; !ok {
		return errors.New("dispatcher not found")
	}
	v.dispatchers[dispatcherID] = ts
	v.tryGC()
	return nil
}

// TODO: find a better function name and param name
// 找到第一个大于 ts 的 index
func findTableInfoIndex(infos []tableInfoItem, ts common.Timestamp) int {
	left, right := 0, len(infos)
	for left < right {
		mid := left + (right-left)/2
		if infos[mid].ts <= ts {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// lock is held by caller
func (v *versionedTableInfoStore) tryGC() {
	var minTS common.Timestamp
	minTS = math.MaxUint64
	for _, ts := range v.dispatchers {
		if ts < minTS {
			minTS = ts
		}
	}
	// only keep one item in infos which have a ts smaller than minTS
	if len(v.infos) == 0 {
		log.Fatal("no table info found")
	}

	// TODO: 检查边界条件
	left := findTableInfoIndex(v.infos, minTS)
	if left == 0 {
		return
	}
	v.infos = v.infos[left-1:]
	v.startTS = minTS
}

func (v *versionedTableInfoStore) getAllRegisteredDispatchers() map[common.DispatcherID]common.Timestamp {
	v.mu.Lock()
	defer v.mu.Unlock()
	result := make(map[common.DispatcherID]common.Timestamp, len(v.dispatchers))
	for k, v := range v.dispatchers {
		result[k] = v
	}
	return result
}

func (v *versionedTableInfoStore) applyDDL(job *model.Job) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.infos) == 0 {
		log.Fatal("no table info found")
	}
	lastTableInfo := v.infos[len(v.infos)-1].info.Clone()

	switch job.Type {
	case model.ActionRenameTable:
		// TODO
	case model.ActionDropTable, model.ActionDropView:
		// TODO
	case model.ActionTruncateTable:
		// just delete this table?
	default:
	}
}

func (v *versionedTableInfoStore) checkAndCopyTailFrom(src *versionedTableInfoStore) {
	v.mu.Lock()
	defer v.mu.Unlock()
	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.infos) == 0 {
		return
	}
	if len(v.infos) == 0 {
		v.infos = append(v.infos, src.infos[len(src.infos)-1])
	}
	firstSrcTS := src.infos[0].ts
	foundFirstSrcTS := false
	startCheckIndex := 0
	// TODO: use binary search
	for i, item := range v.infos {
		if item.ts < firstSrcTS {
			continue
		} else if item.ts == firstSrcTS {
			foundFirstSrcTS = true
			startCheckIndex = i
			break
		} else {
			if !foundFirstSrcTS {
				log.Panic("not found")
			}
			if item.ts != src.infos[i-startCheckIndex].ts {
				log.Panic("not match")
			}
		}
	}
	copyStartIndex := len(src.infos) - (len(v.infos) - startCheckIndex)
	v.infos = append(v.infos, src.infos[copyStartIndex:]...)
}

// GetColumnDefaultValue returns the default definition of a column.
func GetColumnDefaultValue(col *model.ColumnInfo) interface{} {
	defaultValue := col.GetDefaultValue()
	if defaultValue == nil {
		defaultValue = col.GetOriginDefaultValue()
	}
	defaultDatum := datumTypes.NewDatum(defaultValue)
	return defaultDatum.GetValue()
}
