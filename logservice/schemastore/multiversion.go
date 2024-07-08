package schemastore

import (
	"errors"
	"math"
	"sync"

	"github.com/flowbehappy/tigate/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	datumTypes "github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

type tableInfoItem struct {
	version Timestamp
	info    *common.TableInfo
}

type versionedTableInfoStore struct {
	mu sync.Mutex

	// dispatcherID -> max ts successfully send to dispatcher
	// gcTS = min(dispatchers[dispatcherID])
	// When gc, just need retain one version <= gcTS
	dispatchers map[DispatcherID]Timestamp

	// ordered by ts
	infos []*tableInfoItem

	deleteVersion Timestamp
}

func newEmptyVersionedTableInfoStore() *versionedTableInfoStore {
	return &versionedTableInfoStore{
		dispatchers:   make(map[DispatcherID]Timestamp),
		infos:         make([]*tableInfoItem, 0),
		deleteVersion: math.MaxUint64,
	}
}

// TableInfo can from upstream snapshot or create table ddl
func newVersionedTableInfoStore(version Timestamp, info *common.TableInfo) *versionedTableInfoStore {
	store := newEmptyVersionedTableInfoStore()
	store.infos = append(store.infos, &tableInfoItem{version: version, info: info})
	return store
}

func (v *versionedTableInfoStore) getFirstVersion() Timestamp {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.infos) == 0 {
		return math.MaxUint64
	}
	return v.infos[0].version
}

func (v *versionedTableInfoStore) getTableInfo(ts Timestamp) (*common.TableInfo, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// TODO: 检查边界条件
	left := findTableInfoIndex(v.infos, ts)
	if left == 0 {
		return nil, errors.New("no table info found")
	}
	return v.infos[left-1].info, nil
}

func (v *versionedTableInfoStore) registerDispatcher(dispatcherID DispatcherID, ts Timestamp) {
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
func (v *versionedTableInfoStore) unregisterDispatcher(dispatcherID DispatcherID) bool {
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
func (v *versionedTableInfoStore) updateDispatcherSendTS(dispatcherID DispatcherID, ts Timestamp) error {
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
func findTableInfoIndex(infos []*tableInfoItem, ts Timestamp) int {
	left, right := 0, len(infos)
	for left < right {
		mid := left + (right-left)/2
		if infos[mid].version <= ts {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// lock is held by caller
func (v *versionedTableInfoStore) tryGC() {
	// only keep one item in infos which have a ts smaller than minTS
	if len(v.infos) == 0 {
		log.Fatal("no table info found")
	}

	var minTS Timestamp
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
}

func (v *versionedTableInfoStore) getAllRegisteredDispatchers() map[DispatcherID]Timestamp {
	v.mu.Lock()
	defer v.mu.Unlock()
	result := make(map[DispatcherID]Timestamp, len(v.dispatchers))
	for k, v := range v.dispatchers {
		result[k] = v
	}
	return result
}

func assertEmpty(infos []*tableInfoItem) {
	if len(infos) != 0 {
		log.Panic("shouldn't happen")
	}
}

func assertNonEmpty(infos []*tableInfoItem) {
	if len(infos) == 0 {
		log.Panic("shouldn't happen")
	}
}

func assertNonDeleted(v *versionedTableInfoStore) {
	if v.isDeleted() {
		log.Panic("shouldn't happen")
	}
}

func (v *versionedTableInfoStore) isDeleted() bool {
	return v.deleteVersion != Timestamp(math.MaxUint64)
}

func (v *versionedTableInfoStore) applyDDL(job *model.Job) {
	v.mu.Lock()
	defer v.mu.Unlock()
	switch job.Type {
	case model.ActionCreateTable:
		assertEmpty(v.infos)
		info := common.WrapTableInfo(job.SchemaID, job.SchemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)
		v.infos = append(v.infos, &tableInfoItem{version: Timestamp(job.BinlogInfo.FinishedTS), info: info})
	case model.ActionRenameTable:
		assertNonEmpty(v.infos)
		info := common.WrapTableInfo(job.SchemaID, job.SchemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)
		v.infos = append(v.infos, &tableInfoItem{version: Timestamp(job.BinlogInfo.FinishedTS), info: info})
	case model.ActionDropTable, model.ActionTruncateTable:
		assertNonDeleted(v)
		v.deleteVersion = Timestamp(job.BinlogInfo.FinishedTS)
	default:
		// TODO: idenitify unexpected ddl or specify all expected ddl
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
	firstSrcTS := src.infos[0].version
	foundFirstSrcTS := false
	startCheckIndex := 0
	// TODO: use binary search
	for i, item := range v.infos {
		if item.version < firstSrcTS {
			continue
		} else if item.version == firstSrcTS {
			foundFirstSrcTS = true
			startCheckIndex = i
			break
		} else {
			if !foundFirstSrcTS {
				log.Panic("not found")
			}
			if item.version != src.infos[i-startCheckIndex].version {
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
