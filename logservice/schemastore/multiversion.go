package schemastore

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/flowbehappy/tigate/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
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
	// this is not very accurate, it's larger than the actual startTS
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
		log.Info("dispatcher already registered", zap.Any("dispatcherID", dispatcherID))
	}
	v.dispatchers[dispatcherID] = ts
	v.tryGC()
}

// may trigger gc, or totally removed?
// return true if the dispatcher is not in the store.
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
		log.Error("dispatcher cannot be found when update checkpoint",
			zap.Any("dispatcherID", dispatcherID), zap.Any("ts", ts))
		return errors.New("dispatcher not found")
	}
	v.dispatchers[dispatcherID] = ts
	v.tryGC()
	return nil
}

// TODO: find a better function name and param name
// 找到第一个大于 ts 的 index
func findTableInfoIndex(infos []tableInfoItem, ts common.Timestamp) int {
	return sort.Search(len(infos), func(i int) bool {
		return infos[i].ts > ts
	})
}

// lock is held by caller
func (v *versionedTableInfoStore) tryGC() {
	// only keep one item in infos which have a ts smaller than minTS
	if len(v.infos) == 0 {
		log.Fatal("no table info found")
	}

	var minTS common.Timestamp
	minTS = math.MaxUint64
	for _, ts := range v.dispatchers {
		if ts < minTS {
			minTS = ts
		}
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

	// TODO
	_ = v.infos[len(v.infos)-1].info.Clone()

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
	src.mu.Lock()
	defer func() {
		v.mu.Unlock()
		src.mu.Unlock()
	}()
	if len(src.infos) == 0 {
		return
	}
	if len(v.infos) == 0 {
		v.infos = append(v.infos, src.infos[len(src.infos)-1])
	}
	firstSrcTS := src.infos[0].ts
	startCheckIndex := sort.Search(len(v.infos), func(i int) bool {
		return v.infos[i].ts == firstSrcTS
	})
	if startCheckIndex == len(v.infos) {
		log.Panic("not found")
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
