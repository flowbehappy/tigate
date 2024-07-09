package schemastore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// The parent folder to store schema store data
const dataDir = "schema_store"

type persistentStorage struct {
	ch chan interface{}

	db *pebble.DB
}

func newPersistentStorage(
	root string, storage kv.Storage, minRequiredTS Timestamp,
) (*persistentStorage, Timestamp, Timestamp) {
	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	// TODO: update pebble options
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}

	// TODO: cleanObseleteData?

	// check whether meta info exists
	gcTS, resolvedTS, err := loadDataTimeRange(db)
	if err != nil {
		log.Fatal("get meta failed", zap.Error(err))
	}

	if minRequiredTS < gcTS {
		log.Panic("shouldn't happend")
	}

	// Not enough data in schema store, rebuild it
	// FIXME: > or >=?
	if minRequiredTS > resolvedTS {
		// write a new snapshot at minRequiredTS
		err = writeSchemaSnapshotToDisk(db, storage, minRequiredTS)
		// TODO: write index
		if err != nil {
			log.Fatal("write schema snapshot failed", zap.Error(err))
		}

		// update meta in memory and disk
		gcTS = minRequiredTS
		// FIXME: minRequiredTS or minRequiredTS - 1
		resolvedTS = minRequiredTS
		// must update gcTS first
		err = writeTimestampToDisk(db, gcTSKey(), gcTS)
		if err != nil {
			log.Fatal("write gc ts failed", zap.Error(err))
		}
		err = writeTimestampToDisk(db, resolvedTSKey(), resolvedTS)
		if err != nil {
			log.Fatal("write resolved ts failed", zap.Error(err))
		}
	}

	return &persistentStorage{
		ch: make(chan interface{}, 1024),
		db: db,
	}, gcTS, resolvedTS
}

func (p *persistentStorage) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case data := <-p.ch:
			switch v := data.(type) {
			case DDLEvent:
				// TODO: batch ddl event
				// TODO: write index
				err := writeDDLEventToDisk(p.db, v.CommitTS, v)
				if err != nil {
					log.Fatal("write ddl event failed", zap.Error(err))
				}
			case Timestamp:
				err := writeTimestampToDisk(p.db, resolvedTSKey(), v)
				if err != nil {
					log.Fatal("write resolved ts failed", zap.Error(err))
				}
			default:
				log.Fatal("unknown data type")
			}
		}
	}
}

func (p *persistentStorage) writeDDLEvent(ddlEvent DDLEvent) {
	p.ch <- ddlEvent
}

func (p *persistentStorage) updateResolvedTS(resolvedTS Timestamp) {
	p.ch <- resolvedTS
}

func nextPrefix(prefix []byte) []byte {
	upperBound := make([]byte, len(prefix))
	copy(upperBound, prefix)
	for i := len(upperBound) - 1; i >= 0; i-- {
		upperBound[i]++
		if upperBound[i] != 0 {
			break
		}
	}
	return upperBound
}

// TODO: not sure the range is [startTS, endTS] or [startTS, endTS)
func (p *persistentStorage) buildVersionedTableInfoStore(tableID TableID, startTS Timestamp, endTS Timestamp, fillSchemaName func(job *model.Job) error) *versionedTableInfoStore {
	lowerBound, err := indexKey(tableID, startTS)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	upperBound, err := indexKey(tableID, endTS) // endTS + 1?
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer iter.Close()

	// TODO: read from snapshot
	store := newEmptyVersionedTableInfoStore(100)

	for iter.First(); iter.Valid(); iter.Next() {
		// TODO: check whether the key is valid
		tableID, commitTS, err := parseIndexKey(iter.Key())
		if err != nil {
			log.Fatal("parse index key failed", zap.Error(err))
		}

		if commitTS < startTS || commitTS > endTS {
			continue
		}

		ddlKey, err := ddlJobKey(commitTS, tableID)
		if err != nil {
			log.Fatal("generate ddl key failed", zap.Error(err))
		}
		value, closer, err := p.db.Get(ddlKey)
		if err != nil {
			log.Fatal("get ddl job failed", zap.Error(err))
		}
		defer closer.Close()

		var ddlEvent DDLEvent
		err = json.Unmarshal(value, &ddlEvent)
		if err != nil {
			log.Fatal("unmarshal ddl job failed", zap.Error(err))
		}
		err = fillSchemaName(ddlEvent.Job)
		if err != nil {
			log.Fatal("fill schema name failed", zap.Error(err))
		}
		store.applyDDL(ddlEvent.Job)
	}
	if err := iter.Error(); err != nil {
		log.Fatal("iterator error", zap.Error(err))
	}
	return store
}

func (p *persistentStorage) gc(gcTS Timestamp) {
	// TODO
	// add a variable to make sure there is just one gc task
	// create a snapshot of db
	// read schema, for every table, read its incremental data, apply it, and get the final table info and write it.
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
const ddlJobKeyPrefix = "d_"
const indexKeyPrefix = "i_"

func gcTSKey() []byte {
	return []byte("gc")
}

func resolvedTSKey() []byte {
	return []byte("re")
}

func writeTimestampToDisk(db *pebble.DB, key []byte, ts Timestamp) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, ts)
	if err != nil {
		return err
	}
	return db.Set(key, buf.Bytes(), pebble.NoSync)
}

func readTimestampFromDisk(db *pebble.DB, key []byte) (Timestamp, error) {
	value, closer, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	defer closer.Close()

	buf := bytes.NewBuffer(value)
	var ts Timestamp
	err = binary.Read(buf, binary.BigEndian, &ts)
	if err != nil {
		return 0, err
	}
	return ts, nil
}

// load the time range for valid data in db.
// valid data includes
// 1. a schema snapshot at gcTS
// 2. incremental ddl change in the range [gcTS, resolvedTS]
func loadDataTimeRange(db *pebble.DB) (gcTS Timestamp, resolvedTS Timestamp, err error) {
	gcTS, err = readTimestampFromDisk(db, gcTSKey())
	if err != nil {
		return 0, 0, err
	}

	resolvedTS, err = readTimestampFromDisk(db, resolvedTSKey())
	if err != nil {
		return 0, 0, err
	}

	return gcTS, resolvedTS, nil
}

// key format: ss_<ts><schemaID>
func snapshotSchemaKey(ts Timestamp, schemaID SchemaID) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.WriteString(snapshotSchemaKeyPrefix)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, ts)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, schemaID)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// key format: st_<ts><tableID>
func snapshotTableKey(ts Timestamp, tableID TableID) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.WriteString(snapshotTableKeyPrefix)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, ts)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, tableID)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// key format: d_<ts><tableID>
// TODO: is commitTS + tableID a unique identifier?
func ddlJobKey(ts Timestamp, tableID TableID) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.WriteString(ddlJobKeyPrefix)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, ts)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, tableID)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// key format: i_<tableID><commitTS>
// TODO: is commitTS + tableID a unique identifier?
func indexKey(tableID TableID, commitTS Timestamp) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.WriteString(ddlJobKeyPrefix)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, tableID)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, commitTS)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func parseIndexKey(key []byte) (TableID, Timestamp, error) {
	buf := bytes.NewBuffer(key)
	var tableID TableID
	err := binary.Read(buf, binary.BigEndian, &tableID)
	if err != nil {
		return 0, 0, err
	}

	var commitTS Timestamp
	err = binary.Read(buf, binary.BigEndian, &commitTS)
	if err != nil {
		return 0, 0, err
	}

	return tableID, commitTS, nil
}

func writeSchemaSnapshotToDisk(db *pebble.DB, tiStore kv.Storage, ts Timestamp) error {
	meta := getSnapshotMeta(tiStore, uint64(ts))
	start := time.Now()
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		log.Fatal("list databases failed", zap.Error(err))
	}

	// TODO: split multiple batches
	batch := db.NewBatch()
	defer batch.Close()

	for _, dbinfo := range dbinfos {
		// TODO: schema name to id in memory
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
			// TODO: may be we need the whole table info and initialize some struct?
			// or we need more info for partition tables?
			tbName := &model.TableNameInfo{}
			err := json.Unmarshal(rawTable.Value, tbName)
			if err != nil {
				log.Fatal("get table info failed", zap.Error(err))
			}
			tableKey, err := snapshotTableKey(ts, TableID(tbName.ID))
			if err != nil {
				log.Fatal("generate table key failed", zap.Error(err))
			}
			batch.Set(tableKey, rawTable.Value, pebble.NoSync)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	log.Info("finish write schema snapshot",
		zap.Any("duration", time.Since(start).Seconds()))
	return nil
}

func writeDDLEventToDisk(db *pebble.DB, ts Timestamp, ddlEvent DDLEvent) error {
	ddlKey, err := ddlJobKey(ts, TableID(ddlEvent.Job.TableID))
	if err != nil {
		return err
	}
	// commit ts is both encoded in key and value
	ddlValue, err := json.Marshal(ddlEvent)
	if err != nil {
		return err
	}
	return db.Set(ddlKey, ddlValue, pebble.NoSync)
}
