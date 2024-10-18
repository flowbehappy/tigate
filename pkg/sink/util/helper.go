package util

import (
	"net/url"
	"sync"

	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	ticonfig "github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
)

// GetEncoderConfig returns the encoder config and validates the config.
func GetEncoderConfig(
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	protocol config.Protocol,
	sinkConfig *ticonfig.SinkConfig,
	maxMsgBytes int,
) (*common.Config, error) {
	encoderConfig := common.NewConfig(protocol)
	if err := encoderConfig.Apply(sinkURI, sinkConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}
	// Always set encoder's `MaxMessageBytes` equal to producer's `MaxMessageBytes`
	// to prevent that the encoder generate batched message too large
	// then cause producer meet `message too large`.
	encoderConfig = encoderConfig.
		WithMaxMessageBytes(maxMsgBytes).
		WithChangefeedID(changefeedID)

	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}
	encoderConfig.TimeZone = tz

	if err := encoderConfig.Validate(); err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}

	return encoderConfig, nil
}

// TableSchemaStore is store some schema info for dispatchers.
// It is responsible for
// 1. [By TableNameStore]provide all the table name of the specified ts(only support incremental ts), mainly for generate topic for kafka sink when send watermark.
// 2. [By TableIDStore]provide the tableids based on schema-id or all tableids when send ddl ts in mysql sink.
//
// TableSchemaStore only exists in the table trigger event dispatcher, and the same instance's sink of this changefeed,
// which means each changefeed only has one TableSchemaStore.
// TODO:其实是个二选一的关系，后面根据 sink 类型更新一下这个logic
type TableSchemaStore struct {
	tableNameStore *TableNameStore
	tableIDStore   *TableIDStore
}

func NewTableSchemaStore() *TableSchemaStore {
	return &TableSchemaStore{
		tableNameStore: &TableNameStore{
			existingTables:     make(map[string]map[string]*commonEvent.SchemaTableInfo),
			latestTableChanges: &LatestTableChanges{m: make(map[uint64]*commonEvent.TableChange)},
		},
		tableIDStore: &TableIDStore{
			schemaIDToTableIDs: make(map[int64]map[int64]interface{}),
			tableIDToSchemaID:  make(map[int64]int64),
		},
	}
}

func (s *TableSchemaStore) AddEvent(event *commonEvent.DDLEvent) {
	s.tableNameStore.AddEvent(event)
	s.tableIDStore.AddEvent(event)
}

func (s *TableSchemaStore) GetTableIdsByDB(schemaID int64) []int64 {
	return s.tableIDStore.GetTableIdsByDB(schemaID)
}

func (s *TableSchemaStore) GetAllTableIds() []int64 {
	return s.tableIDStore.GetAllTableIds()
}

// GetAllTableNames only will be called when maintainer send message to ask dispatcher to write checkpointTs to downstream.
// So the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableSchemaStore) GetAllTableNames(ts uint64) []*commonEvent.SchemaTableInfo {
	return s.tableNameStore.GetAllTableNames(ts)
}

type LatestTableChanges struct {
	mutex sync.Mutex
	m     map[uint64]*commonEvent.TableChange
}

func (l *LatestTableChanges) Add(ddlEvent *commonEvent.DDLEvent) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.m[ddlEvent.GetCommitTs()] = ddlEvent.TableChange
}

type TableNameStore struct {
	// store all the existing table which existed at the latest query ts
	existingTables map[string]map[string]*commonEvent.SchemaTableInfo // databaseName -> {tableName -> SchemaTableName}
	// store the change of table name from the latest query ts to now(latest event)
	latestTableChanges *LatestTableChanges
}

func (s *TableNameStore) AddEvent(event *commonEvent.DDLEvent) {
	if event.TableChange != nil {
		s.latestTableChanges.Add(event)
	}
}

// GetAllTableNames only will be called when maintainer send message to ask dispatcher to write checkpointTs to downstream.
// So the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableNameStore) GetAllTableNames(ts uint64) []*commonEvent.SchemaTableInfo {
	s.latestTableChanges.mutex.Lock()
	if len(s.latestTableChanges.m) > 0 {
		// update the existingTables with the latest table changes <= ts
		for commitTs, tableChange := range s.latestTableChanges.m {
			if commitTs <= ts {
				if tableChange.DropDatabase != nil {
					delete(s.existingTables, tableChange.DropDatabase.SchemaName)
				} else {
					for _, addName := range tableChange.AddTable {
						if s.existingTables[addName.SchemaName] == nil {
							s.existingTables[addName.SchemaName] = make(map[string]*commonEvent.SchemaTableInfo, 0)
						}
						s.existingTables[addName.SchemaName][addName.TableName] = &addName
					}
					for _, dropName := range tableChange.DropTable {
						delete(s.existingTables[dropName.SchemaName], dropName.TableName)
						if len(s.existingTables[dropName.SchemaName]) == 0 {
							delete(s.existingTables, dropName.SchemaName)
						}
					}
				}
			}
			delete(s.latestTableChanges.m, commitTs)
		}
	}

	s.latestTableChanges.mutex.Unlock()

	tableNames := make([]*commonEvent.SchemaTableInfo, 0)
	for _, tables := range s.existingTables {
		for _, tableName := range tables {
			tableNames = append(tableNames, tableName)
		}
	}

	return tableNames
}

type TableIDStore struct {
	mutex              sync.Mutex
	schemaIDToTableIDs map[int64]map[int64]interface{} // schemaID -> tableIDs
	tableIDToSchemaID  map[int64]int64                 // tableID -> schemaID
}

func (s *TableIDStore) AddEvent(event *commonEvent.DDLEvent) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if event.TableChange != nil {
		for _, addTable := range event.TableChange.AddTable {
			if s.schemaIDToTableIDs[addTable.SchemaID] == nil {
				s.schemaIDToTableIDs[addTable.SchemaID] = make(map[int64]interface{})
			}
			s.schemaIDToTableIDs[addTable.SchemaID][addTable.TableID] = nil
			s.tableIDToSchemaID[addTable.TableID] = addTable.SchemaID
		}
		for _, dropTable := range event.TableChange.DropTable {
			delete(s.schemaIDToTableIDs[dropTable.SchemaID], dropTable.TableID)
			if len(s.schemaIDToTableIDs[dropTable.SchemaID]) == 0 {
				delete(s.schemaIDToTableIDs, dropTable.SchemaID)
			}
			delete(s.tableIDToSchemaID, dropTable.TableID)
		}
		if event.TableChange.DropDatabase != nil {
			tables := s.schemaIDToTableIDs[event.TableChange.DropDatabase.SchemaID]
			for tableID := range tables {
				delete(s.tableIDToSchemaID, tableID)
			}
			delete(s.schemaIDToTableIDs, event.TableChange.DropDatabase.SchemaID)
		}
	}

	if event.UpdatedSchemas != nil {
		for _, schemaIDChange := range event.UpdatedSchemas {
			delete(s.schemaIDToTableIDs[schemaIDChange.OldSchemaID], schemaIDChange.TableID)
			if len(s.schemaIDToTableIDs[schemaIDChange.OldSchemaID]) == 0 {
				delete(s.schemaIDToTableIDs, schemaIDChange.OldSchemaID)
			}

			if s.schemaIDToTableIDs[schemaIDChange.NewSchemaID] == nil {
				s.schemaIDToTableIDs[schemaIDChange.NewSchemaID] = make(map[int64]interface{})
			}
			s.schemaIDToTableIDs[schemaIDChange.NewSchemaID][schemaIDChange.TableID] = nil
			s.tableIDToSchemaID[schemaIDChange.TableID] = schemaIDChange.NewSchemaID
		}
	}

}

func (s *TableIDStore) GetTableIdsByDB(schemaID int64) []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	tables := s.schemaIDToTableIDs[schemaID]
	tableIds := make([]int64, len(tables))
	for tableID := range tables {
		tableIds = append(tableIds, tableID)
	}
	return tableIds
}

func (s *TableIDStore) GetAllTableIds() []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	tableIds := make([]int64, len(s.tableIDToSchemaID))
	for tableID := range s.tableIDToSchemaID {
		tableIds = append(tableIds, tableID)
	}
	return tableIds
}
