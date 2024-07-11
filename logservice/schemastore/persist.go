package schemastore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// The parent folder to store schema data
const dataDir = "schema_store"

type persistentStorage struct {
	gcRunning atomic.Bool
	// only store ddl event which finished ts is larger than gcTS
	// TODO: > gcTS or >= gcTS
	gcTS atomic.Uint64

	db *pebble.DB
}

type schemaMetaTS struct {
	finishedDDLTS Timestamp
	schemaVersion Timestamp
	resolvedTS    Timestamp
}

func newPersistentStorage(
	root string, storage kv.Storage, currentGCTS Timestamp,
) (*persistentStorage, schemaMetaTS, DatabaseInfoMap) {
	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	// TODO: update pebble options
	// TODO: close pebble db at exit
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}

	// TODO: cleanObseleteData?

	dataStorage, metaTS, databaseMap := loadPersistentStorage(db, currentGCTS)

	if dataStorage != nil {
		return dataStorage, metaTS, databaseMap
	}

	// TODO: create a fresh db instance
	databaseMap, err = writeSchemaSnapshotToDisk(db, storage, currentGCTS)
	if err != nil {
		log.Fatal("write schema snapshot failed", zap.Error(err))
	}
	dataStorage = &persistentStorage{
		gcRunning: atomic.Bool{},
		gcTS:      atomic.Uint64{},
		db:        db,
	}
	dataStorage.gcRunning.Store(false)
	dataStorage.gcTS.Store(uint64(currentGCTS))
	// TODO: check whether the following values are correct
	metaTS = schemaMetaTS{
		finishedDDLTS: currentGCTS,
		schemaVersion: currentGCTS,
		resolvedTS:    currentGCTS,
	}

	batch := db.NewBatch()
	writeTSToBatch(batch, gcTSKey(), currentGCTS)
	writeTSToBatch(batch, metaTSKey(), metaTS.resolvedTS, metaTS.finishedDDLTS, metaTS.schemaVersion)
	batch.Commit(pebble.NoSync)

	return dataStorage, metaTS, databaseMap
}

func loadPersistentStorage(db *pebble.DB, minRequiredTS Timestamp) (*persistentStorage, schemaMetaTS, DatabaseInfoMap) {
	snap := db.NewSnapshot()
	defer snap.Close()

	dataStorage := &persistentStorage{
		gcRunning: atomic.Bool{},
		gcTS:      atomic.Uint64{},
		db:        db,
	}
	dataStorage.gcRunning.Store(false)
	values, err := readTSFromSnapshot(snap, gcTSKey())
	// TODO: distiguish between non exist key error and other error
	if err != nil || len(values) != 1 {
		return nil, schemaMetaTS{}, nil
	}
	dataStorage.gcTS.Store(uint64(values[0]))

	var metaTS schemaMetaTS
	values, err = readTSFromSnapshot(snap, metaTSKey())
	if err != nil || len(values) != 3 {
		return nil, schemaMetaTS{}, nil
	}
	metaTS.resolvedTS = values[0]
	metaTS.finishedDDLTS = values[1]
	metaTS.schemaVersion = values[2]

	// gcTS cannot go back
	if minRequiredTS < Timestamp(dataStorage.gcTS.Load()) {
		log.Panic("shouldn't happend")
	}
	// FIXME: > or >=?
	if minRequiredTS > metaTS.resolvedTS {
		return nil, schemaMetaTS{}, nil
	}

	databaseMap := make(DatabaseInfoMap)

	// TODO: read database map from disk
	snapshotLowerBound, err := snapshotSchemaKey(dataStorage.getGCTS(), 0)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	snapshotUpperBound, err := snapshotSchemaKey(dataStorage.getGCTS(), SchemaID(math.MaxInt64))
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: snapshotLowerBound,
		UpperBound: snapshotUpperBound,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		dbInfo := &model.DBInfo{}
		err := json.Unmarshal(snapIter.Value(), dbInfo)
		if err != nil {
			log.Fatal("get db info failed", zap.Error(err))
		}
		databaseMap[DatabaseID(dbInfo.ID)] = &DatabaseInfo{
			Name:          dbInfo.Name.O,
			Tables:        make([]TableID, 0),
			CreateVersion: Timestamp(dataStorage.getGCTS()),
			DeleteVersion: Timestamp(math.MaxUint64),
		}
	}

	ddlJobLowerBound, err := ddlJobSchemaKey(dataStorage.getGCTS(), 0)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	ddlJobUpperBound, err := ddlJobSchemaKey(Timestamp(math.MaxUint64), SchemaID(math.MaxInt64))
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	ddlJobIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: ddlJobLowerBound,
		UpperBound: ddlJobUpperBound,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer ddlJobIter.Close()
	for ddlJobIter.First(); ddlJobIter.Valid(); ddlJobIter.Next() {
		ddlJob := &model.Job{}
		err := json.Unmarshal(ddlJobIter.Value(), ddlJob)
		if err != nil {
			log.Fatal("get db info failed", zap.Error(err))
		}
		handleResolvedDDLJob(ddlJob, databaseMap, nil)
	}

	return dataStorage, metaTS, databaseMap
}

func (p *persistentStorage) writeDDLEvent(ddlEvent DDLEvent) error {
	ddlValue, err := json.Marshal(ddlEvent)
	if err != nil {
		return err
	}
	batch := p.db.NewBatch()
	switch ddlEvent.Job.Type {
	case model.ActionCreateSchema, model.ActionModifySchemaCharsetAndCollate, model.ActionDropSchema:
		ddlKey, err := ddlJobSchemaKey(Timestamp(ddlEvent.Job.BinlogInfo.FinishedTS), SchemaID(ddlEvent.Job.SchemaID))
		if err != nil {
			return err
		}
		batch.Set(ddlKey, ddlValue, pebble.NoSync)
		return batch.Commit(pebble.NoSync)
	default:
		// TODO: for cross table ddl, need write two events(may be we need a table_id -> name map?)
		ddlKey, err := ddlJobTableKey(Timestamp(ddlEvent.Job.BinlogInfo.FinishedTS), TableID(ddlEvent.Job.TableID))
		if err != nil {
			return err
		}

		batch.Set(ddlKey, ddlValue, pebble.NoSync)
		indexDDLKey, err := indexDDLJobKey(TableID(ddlEvent.Job.TableID), Timestamp(ddlEvent.Job.BinlogInfo.FinishedTS))
		if err != nil {
			return err
		}
		batch.Set(indexDDLKey, nil, pebble.NoSync)
		return batch.Commit(pebble.NoSync)
	}
}

func (p *persistentStorage) updateStoreMeta(resolvedTS Timestamp, finishedDDLTS Timestamp, schemaVersion Timestamp) error {
	batch := p.db.NewBatch()
	err := writeTSToBatch(batch, metaTSKey(), resolvedTS, finishedDDLTS, schemaVersion)
	if err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func tryReadTableInfoFromSnapshot(
	snap *pebble.Snapshot,
	tableID TableID,
	startTS Timestamp,
	getSchemaName func(schemaID SchemaID) (string, error),
) (*common.TableInfo, error) {
	lowerBound, err := generateKey(indexSnapshotKeyPrefix, uint64(tableID))
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	upperBound, err := generateKey(indexSnapshotKeyPrefix, uint64(tableID+1))
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer iter.Close()
	for iter.Last(); iter.Valid(); iter.Prev() {
		_, version, schemaID, err := parseIndexSnapshotKey(iter.Key())
		if err != nil {
			log.Fatal("parse index key failed", zap.Any("key", iter.Key()), zap.Error(err))
		}
		if version > startTS {
			continue
		}
		targetKey, err := snapshotTableKey(version, tableID)
		if err != nil {
			return nil, err
		}
		value, closer, err := snap.Get(targetKey)
		if err != nil {
			return nil, err
		}
		defer closer.Close()

		tableInfo := &model.TableInfo{}
		err = json.Unmarshal(value, tableInfo)
		if err != nil {
			return nil, err
		}
		schemaName, err := getSchemaName(schemaID)
		if err != nil {
			return nil, err
		}
		return common.WrapTableInfo(int64(schemaID), schemaName, uint64(version), tableInfo), nil
	}
	return nil, nil
}

func readDDLJobTimestampForTable(snap *pebble.Snapshot, tableID TableID, endTS Timestamp) []Timestamp {
	lowerBound, err := generateKey(indexDDLJobKeyPrefix, uint64(tableID))
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	upperBound, err := generateKey(indexDDLJobKeyPrefix, uint64(tableID), uint64(endTS+1))
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer iter.Close()
	result := make([]Timestamp, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		_, version, err := parseIndexDDLJobKey(iter.Key())
		if err != nil {
			log.Fatal("parse index key failed", zap.Error(err))
		}
		result = append(result, version)
	}
	return result
}

// build a versionedTableInfoStore within the time range (startTS, endTS]
func (p *persistentStorage) buildVersionedTableInfoStore(
	store *versionedTableInfoStore,
	startTS Timestamp,
	endTS Timestamp,
	getSchemaName func(schemaID SchemaID) (string, error),
) error {
	tableID := store.getTableID()
	snap := p.db.NewSnapshot()
	defer snap.Close()
	tableInfoFromSnap, err := tryReadTableInfoFromSnapshot(snap, tableID, startTS, getSchemaName)
	if err != nil {
		return err
	}
	if tableInfoFromSnap != nil {
		store.addInitialTableInfo(tableInfoFromSnap)
	}
	allDDLJobTS := readDDLJobTimestampForTable(snap, tableID, endTS)
	for _, ts := range allDDLJobTS {
		if tableInfoFromSnap != nil && ts <= Timestamp(tableInfoFromSnap.Version) {
			continue
		}
		ddlKey, err := ddlJobTableKey(ts, tableID)
		if err != nil {
			log.Fatal("generate ddl key failed", zap.Error(err))
		}
		value, closer, err := snap.Get(ddlKey)
		if err != nil {
			log.Fatal("get ddl job failed", zap.Error(err))
		}
		defer closer.Close()
		var ddlEvent DDLEvent
		err = json.Unmarshal(value, &ddlEvent)
		if err != nil {
			log.Fatal("unmarshal ddl job failed", zap.Error(err))
		}
		schemaName, err := getSchemaName(SchemaID(ddlEvent.Job.SchemaID))
		if err != nil {
			log.Fatal("get schema name failed", zap.Error(err))
		}
		ddlEvent.Job.SchemaName = schemaName
		store.applyDDL(ddlEvent.Job)
	}
	return nil
}

func (p *persistentStorage) getGCTS() Timestamp {
	return Timestamp(p.gcTS.Load())
}

func (p *persistentStorage) gc(gcTS Timestamp) error {
	if p.gcRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer p.gcRunning.Store(false)
	p.gcTS.Store(uint64(gcTS))
	// TODO: write snapshot(schema and table) to disk(don't need to be in the same batch) and maintain the key that need be deleted(or just write it to a delete batch)

	// update gcTS in disk, must do it before delete any data
	batch := p.db.NewBatch()
	if err := writeTSToBatch(batch, gcTSKey(), gcTS); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	// TODO: delete old data(including index data, so we need to read data one by one)
	// may be write and delete in the same batch?

	return nil
}

func getSnapshotMeta(tiStore kv.Storage, ts uint64) *meta.Meta {
	snapshot := tiStore.GetSnapshot(kv.NewVersion(ts))
	return meta.NewSnapshotMeta(snapshot)
}

const mTablePrefix = "Table"

func isTableRawKey(key []byte) bool {
	return strings.HasPrefix(string(key), mTablePrefix)
}

const snapshotSchemaKeyPrefix = "ss_"
const snapshotTableKeyPrefix = "st_"

const ddlJobSchemaKeyPrefix = "ds_"
const ddlJobTableKeyPrefix = "dt_"

// table_id -> timestamp
const indexSnapshotKeyPrefix = "is_"

// table_id -> timestamp
const indexDDLJobKeyPrefix = "id_"

func gcTSKey() []byte {
	return []byte("gc")
}

func metaTSKey() []byte {
	return []byte("me")
}

// key format: <prefix><values[0]><values[1]>...
func generateKey(prefix string, values ...uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.WriteString(prefix)
	if err != nil {
		return nil, err
	}
	for _, v := range values {
		err = binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func checkAndParseKey(key []byte, prefix string) ([]uint64, error) {
	if !strings.HasPrefix(string(key), prefix) {
		return nil, fmt.Errorf("invalid key prefix: %s", string(key))
	}
	buf := bytes.NewBuffer(key)
	buf.Next(len(prefix))
	var values []uint64
	for {
		var v uint64
		err := binary.Read(buf, binary.BigEndian, &v)
		if err != nil {
			if err == io.EOF {
				return values, nil
			}
			return nil, err
		}
		values = append(values, v)
	}
}

func snapshotSchemaKey(ts Timestamp, schemaID SchemaID) ([]byte, error) {
	return generateKey(snapshotSchemaKeyPrefix, uint64(ts), uint64(schemaID))
}

func snapshotTableKey(ts Timestamp, tableID TableID) ([]byte, error) {
	return generateKey(snapshotTableKeyPrefix, uint64(ts), uint64(tableID))
}

func ddlJobSchemaKey(ts Timestamp, schemaID SchemaID) ([]byte, error) {
	return generateKey(ddlJobSchemaKeyPrefix, uint64(ts), uint64(schemaID))
}

func ddlJobTableKey(ts Timestamp, tableID TableID) ([]byte, error) {
	return generateKey(ddlJobTableKeyPrefix, uint64(ts), uint64(tableID))
}

func indexSnapshotKey(tableID TableID, commitTS Timestamp, schemaID SchemaID) ([]byte, error) {
	return generateKey(indexSnapshotKeyPrefix, uint64(tableID), uint64(commitTS), uint64(schemaID))
}

func indexDDLJobKey(tableID TableID, commitTS Timestamp) ([]byte, error) {
	return generateKey(indexDDLJobKeyPrefix, uint64(tableID), uint64(commitTS))
}

func parseIndexSnapshotKey(key []byte) (TableID, Timestamp, SchemaID, error) {
	values, err := checkAndParseKey(key, indexSnapshotKeyPrefix)
	if err != nil || len(values) != 3 {
		log.Fatal("parse index key failed",
			zap.Any("key", key),
			zap.Any("keyLength", len(key)),
			zap.Any("values", values),
			zap.Error(err))
	}
	return TableID(values[0]), Timestamp(values[1]), SchemaID(values[2]), nil
}

func parseIndexDDLJobKey(key []byte) (TableID, Timestamp, error) {
	values, err := checkAndParseKey(key, indexDDLJobKeyPrefix)
	if err != nil || len(values) != 2 {
		log.Fatal("parse index key failed", zap.Error(err))
	}
	return TableID(values[0]), Timestamp(values[1]), nil
}

func writeTSToBatch(batch *pebble.Batch, key []byte, ts ...Timestamp) error {
	buf := new(bytes.Buffer)
	for _, t := range ts {
		err := binary.Write(buf, binary.BigEndian, t)
		if err != nil {
			return err
		}
	}
	batch.Set(key, buf.Bytes(), pebble.NoSync)
	return nil
}

func readTSFromSnapshot(snap *pebble.Snapshot, key []byte) ([]Timestamp, error) {
	value, closer, err := snap.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	buf := bytes.NewBuffer(value)
	var values []Timestamp
	for {
		var ts Timestamp
		err := binary.Read(buf, binary.BigEndian, &ts)
		if err != nil {
			if err == io.EOF {
				return values, nil
			}
			return nil, err
		}
		values = append(values, ts)
	}
}

func writeSchemaSnapshotToDisk(db *pebble.DB, tiStore kv.Storage, ts Timestamp) (DatabaseInfoMap, error) {
	meta := getSnapshotMeta(tiStore, uint64(ts))
	start := time.Now()
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		log.Fatal("list databases failed", zap.Error(err))
	}

	// TODO: split multiple batches
	batch := db.NewBatch()
	defer batch.Close()

	databaseMap := make(DatabaseInfoMap, len(dbinfos))
	for _, dbinfo := range dbinfos {
		databaseInfo := &DatabaseInfo{
			Name:          dbinfo.Name.O,
			Tables:        make([]TableID, 0),
			CreateVersion: ts,
			DeleteVersion: Timestamp(math.MaxUint64),
		}
		databaseMap[DatabaseID(dbinfo.ID)] = databaseInfo
		schemaKey, err := snapshotSchemaKey(ts, SchemaID(dbinfo.ID))
		if err != nil {
			log.Fatal("generate schema key failed", zap.Error(err))
		}
		schemaValue, err := json.Marshal(dbinfo)
		if err != nil {
			log.Fatal("marshal schema failed", zap.Error(err))
		}
		batch.Set(schemaKey, schemaValue, pebble.NoSync)
		rawTables, err := meta.GetMetasByDBID(dbinfo.ID)
		if err != nil {
			log.Fatal("get tables failed", zap.Error(err))
		}
		for _, rawTable := range rawTables {
			if !isTableRawKey(rawTable.Field) {
				continue
			}
			tbName := &model.TableNameInfo{}
			err := json.Unmarshal(rawTable.Value, tbName)
			if err != nil {
				log.Fatal("get table info failed", zap.Error(err))
			}
			databaseInfo.Tables = append(databaseInfo.Tables, TableID(tbName.ID))
			tableKey, err := snapshotTableKey(ts, TableID(tbName.ID))
			if err != nil {
				log.Fatal("generate table key failed", zap.Error(err))
			}
			batch.Set(tableKey, rawTable.Value, pebble.NoSync)
			indexKey, err := indexSnapshotKey(TableID(tbName.ID), ts, SchemaID(dbinfo.ID))
			if err != nil {
				log.Fatal("generate index key failed", zap.Error(err))
			}
			batch.Set(indexKey, nil, pebble.NoSync)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return nil, err
	}

	log.Info("finish write schema snapshot",
		zap.Any("duration", time.Since(start).Seconds()))
	return databaseMap, nil
}
