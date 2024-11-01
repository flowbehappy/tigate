// Copyright 2023 PingCAP, Inc.
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

package common

import (
	ticonfig "github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/pkg/errors"
)

type Selector interface {
	Select(colInfo *model.ColumnInfo) bool
}

type DefaultColumnSelector struct{}

func NewDefaultColumnSelector() *DefaultColumnSelector {
	return &DefaultColumnSelector{}
}

func (d *DefaultColumnSelector) Select(colInfo *model.ColumnInfo) bool {
	return true
}

type ColumnSelector struct {
	tableF  filter.Filter
	columnM filter.ColumnFilter
}

func newColumnSelector(
	rule *ticonfig.ColumnSelector, caseSensitive bool,
) (*ColumnSelector, error) {
	tableM, err := filter.Parse(rule.Matcher)
	if err != nil {
		return nil, errors.WrapError(errors.ErrFilterRuleInvalid, err, rule.Matcher)
	}
	if !caseSensitive {
		tableM = filter.CaseInsensitive(tableM)
	}
	columnM, err := filter.ParseColumnFilter(rule.Columns)
	if err != nil {
		return nil, errors.WrapError(errors.ErrFilterRuleInvalid, err, rule.Columns)
	}

	return &ColumnSelector{
		tableF:  tableM,
		columnM: columnM,
	}, nil
}

// Match implements Transformer interface
func (s *ColumnSelector) match(schema, table string) bool {
	return s.tableF.MatchTable(schema, table)
}

// Select decide whether the col should be encoded or not.
func (s *ColumnSelector) Select(colInfo *model.ColumnInfo) bool {
	colName := colInfo.Name.O
	return s.columnM.MatchColumn(colName)
}

// ColumnSelectors manages an array of selectors, the first selector match the given
// event is used to select out columns.
type ColumnSelectors struct {
	selectors []*ColumnSelector
}

// New return a column selectors
func NewColumnSelectors(sinkConfig *ticonfig.SinkConfig) (*ColumnSelectors, error) {
	selectors := make([]*ColumnSelector, 0, len(sinkConfig.ColumnSelectors))
	for _, r := range sinkConfig.ColumnSelectors {
		selector, err := newColumnSelector(r, sinkConfig.CaseSensitive)
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, selector)
	}

	return &ColumnSelectors{
		selectors: selectors,
	}, nil
}

func (c *ColumnSelectors) GetSelector(schema, table string) Selector {
	for _, s := range c.selectors {
		if s.match(schema, table) {
			return s
		}
	}
	return NewDefaultColumnSelector()
}
