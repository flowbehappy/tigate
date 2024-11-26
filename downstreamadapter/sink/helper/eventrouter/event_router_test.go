package eventrouter

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter/partition"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter/topic"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/stretchr/testify/require"
)

func newSinkConfig4Test() *config.SinkConfig {
	return &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			// rule-0
			{
				Matcher:       []string{"test_default1.*"},
				PartitionRule: "default",
			},
			// rule-1
			{
				Matcher:       []string{"test_default2.*"},
				PartitionRule: "unknown-dispatcher",
			},
			// rule-2
			{
				Matcher:       []string{"test_table.*"},
				PartitionRule: "table",
				TopicRule:     "hello_{schema}_world",
			},
			// rule-3
			{
				Matcher:       []string{"test_index_value.*"},
				PartitionRule: "index-value",
				TopicRule:     "{schema}_world",
			},
			// rule-4
			{
				Matcher:       []string{"test.*"},
				PartitionRule: "rowid",
				TopicRule:     "hello_{schema}",
			},
			// rule-5
			{
				Matcher:       []string{"*.*", "!*.test"},
				PartitionRule: "ts",
				TopicRule:     "{schema}_{table}",
			},
			// rule-6: hard code the topic
			{
				Matcher:       []string{"hard_code_schema.*"},
				PartitionRule: "default",
				TopicRule:     "hard_code_topic",
			},
		},
	}
}

func TestEventRouter(t *testing.T) {
	t.Parallel()

	sinkConfig := &config.SinkConfig{}
	d, err := NewEventRouter(sinkConfig, config.ProtocolCanalJSON, "test", sink.KafkaScheme)
	require.NoError(t, err)
	require.Equal(t, "test", d.GetDefaultTopic())

	partitionDispatcher := d.GetPartitionDispatcher("test", "test")
	topicDispatcher := d.matchTopicGenerator("test", "test")
	require.IsType(t, &topic.StaticTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.TablePartitionGenerator{}, partitionDispatcher)

	actual := topicDispatcher.Substitute("test", "test")
	require.Equal(t, d.defaultTopic, actual)

	sinkConfig = newSinkConfig4Test()
	d, err = NewEventRouter(sinkConfig, config.ProtocolCanalJSON, "", sink.KafkaScheme)
	require.NoError(t, err)

	// no matched, use the default
	partitionDispatcher = d.GetPartitionDispatcher("sbs", "test")
	topicDispatcher = d.matchTopicGenerator("sbs", "test")
	require.IsType(t, &topic.StaticTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.TablePartitionGenerator{}, partitionDispatcher)

	// match rule-0
	partitionDispatcher = d.GetPartitionDispatcher("test_default1", "test")
	topicDispatcher = d.matchTopicGenerator("test_default1", "test")
	require.IsType(t, &topic.StaticTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.TablePartitionGenerator{}, partitionDispatcher)

	// match rule-1
	partitionDispatcher = d.GetPartitionDispatcher("test_default2", "test")
	topicDispatcher = d.matchTopicGenerator("test_default2", "test")
	require.IsType(t, &topic.StaticTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.TablePartitionGenerator{}, partitionDispatcher)

	// match rule-2
	partitionDispatcher = d.GetPartitionDispatcher("test_table", "test")
	topicDispatcher = d.matchTopicGenerator("test_table", "test")
	require.IsType(t, &topic.DynamicTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.TablePartitionGenerator{}, partitionDispatcher)

	// match rule-4
	partitionDispatcher = d.GetPartitionDispatcher("test_index_value", "test")
	topicDispatcher = d.matchTopicGenerator("test_index_value", "test")
	require.IsType(t, &topic.DynamicTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.IndexValuePartitionGenerator{}, partitionDispatcher)

	// match rule-4
	partitionDispatcher = d.GetPartitionDispatcher("test", "table1")
	topicDispatcher = d.matchTopicGenerator("test", "table1")
	require.IsType(t, &topic.DynamicTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.IndexValuePartitionGenerator{}, partitionDispatcher)

	// match rule-5
	partitionDispatcher = d.GetPartitionDispatcher("sbs", "table2")
	topicDispatcher = d.matchTopicGenerator("sbs", "table2")
	require.IsType(t, &topic.DynamicTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.TsPartitionGenerator{}, partitionDispatcher)

	// match rule-6
	partitionDispatcher = d.GetPartitionDispatcher("hard_code_schema", "test")
	topicDispatcher = d.matchTopicGenerator("hard_code_schema", "test")
	require.IsType(t, &topic.StaticTopicGenerator{}, topicDispatcher)
	require.IsType(t, &partition.TablePartitionGenerator{}, partitionDispatcher)
}

func TestGetActiveTopics(t *testing.T) {
	t.Parallel()

	sinkConfig := newSinkConfig4Test()
	d, err := NewEventRouter(sinkConfig, config.ProtocolCanalJSON, "test", sink.KafkaScheme)
	require.NoError(t, err)
	names := []*commonEvent.SchemaTableName{
		{SchemaName: "test_default1", TableName: "table"},
		{SchemaName: "test_default2", TableName: "table"},
		{SchemaName: "test_table", TableName: "table"},
		{SchemaName: "test_index_value", TableName: "table"},
		{SchemaName: "test", TableName: "table"},
		{SchemaName: "sbs", TableName: "table"},
	}
	topics := d.GetActiveTopics(names)
	require.Equal(t, []string{"test", "hello_test_table_world", "test_index_value_world", "hello_test", "sbs_table"}, topics)
}

func TestGetTopicForRowChange(t *testing.T) {
	t.Parallel()

	sinkConfig := newSinkConfig4Test()
	d, err := NewEventRouter(sinkConfig, config.ProtocolCanalJSON, "test", "kafka")
	require.NoError(t, err)

	topicName := d.GetTopicForRowChange(&common.TableInfo{
		TableName: common.TableName{Schema: "test_default1", Table: "table"},
	})
	require.Equal(t, "test", topicName)

	topicName = d.GetTopicForRowChange(&common.TableInfo{
		TableName: common.TableName{Schema: "test_default2", Table: "table"},
	})
	require.Equal(t, "test", topicName)

	topicName = d.GetTopicForRowChange(&common.TableInfo{
		TableName: common.TableName{Schema: "test_table", Table: "table"},
	})
	require.Equal(t, "hello_test_table_world", topicName)

	topicName = d.GetTopicForRowChange(&common.TableInfo{
		TableName: common.TableName{Schema: "test_index_value", Table: "table"},
	})
	require.Equal(t, "test_index_value_world", topicName)

	topicName = d.GetTopicForRowChange(&common.TableInfo{
		TableName: common.TableName{Schema: "a", Table: "table"},
	})
	require.Equal(t, "a_table", topicName)
}

func TestGetPartitionForRowChange(t *testing.T) {
	t.Parallel()

	sinkConfig := newSinkConfig4Test()
	d, err := NewEventRouter(sinkConfig, config.ProtocolCanalJSON, "test", sink.KafkaScheme)
	require.NoError(t, err)

	// default partition
	tableInfo := &common.TableInfo{
		TableName: common.TableName{Schema: "test_default1", Table: "table"},
	}
	partitionGenerator := d.GetPartitionGeneratorForRowChange(tableInfo)
	p, _, err := partitionGenerator.GeneratePartitionIndexAndKey(&commonEvent.RowChange{}, 16, tableInfo, 0)
	require.NoError(t, err)
	require.Equal(t, int32(14), p)

	// default partition
	tableInfo = &common.TableInfo{
		TableName: common.TableName{Schema: "test_default2", Table: "table"},
	}
	partitionGenerator = d.GetPartitionGeneratorForRowChange(tableInfo)
	p, _, err = partitionGenerator.GeneratePartitionIndexAndKey(&commonEvent.RowChange{}, 16, tableInfo, 0)
	require.NoError(t, err)
	require.Equal(t, int32(0), p)

	//table partition
	tableInfo = &common.TableInfo{
		TableName: common.TableName{Schema: "test_table", Table: "table"},
	}
	partitionGenerator = d.GetPartitionGeneratorForRowChange(tableInfo)
	p, _, err = partitionGenerator.GeneratePartitionIndexAndKey(&commonEvent.RowChange{}, 16, tableInfo, 1)
	require.NoError(t, err)
	require.Equal(t, int32(15), p)

	// index partition
	tableInfo = &common.TableInfo{
		TableName: common.TableName{Schema: "test_index_value", Table: "table"},
	}

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("create database test_index_value")
	helper.Tk().MustExec("use test_index_value")
	createTableSQL := "create table table1 (a int primary key, b int);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test_index_value", "table1", "insert into table1 values (11, 22)")
	dmlEvent.CommitTs = 2

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	partitionGenerator = d.GetPartitionGeneratorForRowChange(dmlEvent.TableInfo)
	p, _, err = partitionGenerator.GeneratePartitionIndexAndKey(&row, 10, dmlEvent.TableInfo, 2)
	require.NoError(t, err)
	require.Equal(t, int32(9), p)

	// ts partition
	tableInfo = &common.TableInfo{
		TableName: common.TableName{Schema: "a", Table: "table"},
	}
	partitionGenerator = d.GetPartitionGeneratorForRowChange(tableInfo)
	p, _, err = partitionGenerator.GeneratePartitionIndexAndKey(&commonEvent.RowChange{}, 2, tableInfo, 1)
	require.NoError(t, err)
	require.Equal(t, int32(1), p)
}

func TestGetTopicForDDL(t *testing.T) {
	t.Parallel()

	sinkConfig := &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{
				Matcher:       []string{"test.*"},
				PartitionRule: "table",
				TopicRule:     "hello_{schema}",
			},
			{
				Matcher:       []string{"*.*", "!*.test"},
				PartitionRule: "ts",
				TopicRule:     "{schema}_{table}",
			},
		},
	}

	d, err := NewEventRouter(sinkConfig, config.ProtocolDefault, "test", "kafka")
	require.NoError(t, err)

	tests := []struct {
		ddl           *commonEvent.DDLEvent
		expectedTopic string
	}{
		{
			ddl: &commonEvent.DDLEvent{
				SchemaName: "test",
			},
			expectedTopic: "test",
		},
		{
			ddl: &commonEvent.DDLEvent{
				SchemaName: "test",
				TableName:  "tb1",
			},
			expectedTopic: "hello_test",
		},
		{
			ddl: &commonEvent.DDLEvent{
				SchemaName: "test1",
				TableName:  "view1",
			},
			expectedTopic: "test1_view1",
		},
		{
			ddl: &commonEvent.DDLEvent{
				SchemaName: "test1",
				TableName:  "tb1",
			},
			expectedTopic: "test1_tb1",
		},
		{
			ddl: &commonEvent.DDLEvent{
				PrevSchemaName: "test1",
				PrevTableName:  "tb1",
				SchemaName:     "test1",
				TableName:      "tb2",
			},
			expectedTopic: "test1_tb1",
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expectedTopic, d.GetTopicForDDL(test.ddl))
	}
}
