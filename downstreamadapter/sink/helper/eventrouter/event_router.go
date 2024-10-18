// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package eventrouter

import (
	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/eventrouter/partition"
	"github.com/flowbehappy/tigate/downstreamadapter/sink/helper/eventrouter/topic"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	ticonfig "github.com/flowbehappy/tigate/pkg/config"
	"github.com/pingcap/log"
	tableFilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type Rule struct {
	partitionDispatcher partition.PartitionGenerator
	topicGenerator      topic.TopicGenerator
	tableFilter.Filter
}

// EventRouter is a router, it determines which topic and which partition
// an event should be dispatched to.
type EventRouter struct {
	defaultTopic string
	rules        []Rule
}

// NewEventRouter creates a new EventRouter.
func NewEventRouter(sinkConfig *ticonfig.SinkConfig, protocol config.Protocol, defaultTopic, scheme string) (*EventRouter, error) {
	// If an event does not match any dispatching rules in the config file,
	// it will be dispatched by the default partition dispatcher and
	// static topic dispatcher because it matches *.* rule.
	ruleConfigs := append(sinkConfig.DispatchRules, &ticonfig.DispatchRule{
		Matcher:       []string{"*.*"},
		PartitionRule: "default",
		TopicRule:     "",
	})

	rules := make([]Rule, 0, len(ruleConfigs))

	for _, ruleConfig := range ruleConfigs {
		f, err := tableFilter.Parse(ruleConfig.Matcher)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, ruleConfig.Matcher)
		}
		if !sinkConfig.CaseSensitive {
			f = tableFilter.CaseInsensitive(f)
		}

		d := partition.GetPartitionGenerator(ruleConfig.PartitionRule, scheme, ruleConfig.IndexName, ruleConfig.Columns)

		topicGenerator, err := topic.GetTopicGenerator(ruleConfig.TopicRule, defaultTopic, protocol, scheme)
		if err != nil {
			return nil, err
		}
		rules = append(rules, Rule{partitionDispatcher: d, topicGenerator: topicGenerator, Filter: f})
	}

	return &EventRouter{
		defaultTopic: defaultTopic,
		rules:        rules,
	}, nil
}

// GetTopicForRowChange returns the target topic for row changes.
func (s *EventRouter) GetTopicForRowChange(tableInfo *common.TableInfo) string {
	topicGenerator := s.matchTopicGenerator(tableInfo.TableName.Schema, tableInfo.TableName.Table)
	return topicGenerator.Substitute(tableInfo.TableName.Schema, tableInfo.TableName.Table)
}

// GetTopicForDDL returns the target topic for DDL.
func (s *EventRouter) GetTopicForDDL(ddl *commonEvent.DDLEvent) string {
	schema := ddl.SchemaName
	table := ddl.TableName

	// TODO: fix this
	//var schema, table string
	// if ddl.PreTableInfo != nil {
	// 	if ddl.PreTableInfo.TableName.Table == "" {
	// 		return s.defaultTopic
	// 	}
	// 	schema = ddl.PreTableInfo.TableName.Schema
	// 	table = ddl.PreTableInfo.TableName.Table
	// } else {
	// 	if ddl.TableInfo.TableName.Table == "" {
	// 		return s.defaultTopic
	// 	}
	// 	schema = ddl.TableInfo.TableName.Schema
	// 	table = ddl.TableInfo.TableName.Table
	// }

	topicGenerator := s.matchTopicGenerator(schema, table)
	return topicGenerator.Substitute(schema, table)
}

// GetActiveTopics returns a list of the corresponding topics
// for the tables that are actively synchronized.
func (s *EventRouter) GetActiveTopics(activeTables []*commonEvent.SchemaTableInfo) []string {
	topics := make([]string, 0)
	topicsMap := make(map[string]bool, len(activeTables))
	for _, tableName := range activeTables {
		topicDispatcher := s.matchTopicGenerator(tableName.SchemaName, tableName.TableName)
		if topicDispatcher.TopicGeneratorType() == topic.StaticTopicGeneratorType {
			continue
		}
		topicName := topicDispatcher.Substitute(tableName.SchemaName, tableName.TableName)
		if !topicsMap[topicName] {
			topicsMap[topicName] = true
			topics = append(topics, topicName)
		}
	}

	// We also need to add the default topic.
	if !topicsMap[s.defaultTopic] {
		topics = append(topics, s.defaultTopic)
	}

	return topics
}

// GetPartitionForRowChange returns the target partition for row changes.
func (s *EventRouter) GetPartitionGeneratorForRowChange(tableInfo *common.TableInfo) partition.PartitionGenerator {
	return s.GetPartitionDispatcher(tableInfo.GetSchemaName(), tableInfo.GetTableName())
}

// GetPartitionDispatcher returns the partition dispatcher for a specific table.
func (s *EventRouter) GetPartitionDispatcher(schema, table string) partition.PartitionGenerator {
	partitionGenerator := s.matchPartitonGenerator(schema, table)
	return partitionGenerator
}

// GetDefaultTopic returns the default topic name.
func (s *EventRouter) GetDefaultTopic() string {
	return s.defaultTopic
}

func (s *EventRouter) matchTopicGenerator(schema, table string) topic.TopicGenerator {
	for _, rule := range s.rules {
		if !rule.MatchTable(schema, table) {
			continue
		}
		return rule.topicGenerator
	}
	log.Panic("the dispatch rule must cover all tables")
	return nil
}

func (s *EventRouter) matchPartitonGenerator(schema, table string) partition.PartitionGenerator {
	for _, rule := range s.rules {
		if !rule.MatchTable(schema, table) {
			continue
		}
		return rule.partitionDispatcher
	}
	log.Panic("the dispatch rule must cover all tables")
	return nil
}
