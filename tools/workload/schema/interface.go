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

package schema

type Workload interface {
	// BuildCreateTableStatement returns the create-table sql of the table n
	BuildCreateTableStatement(n int) string
	// BuildInsertSql returns the insert sql statement of the tableN, insert batchSize records
	BuildInsertSql(tableN int, batchSize int) string
	// BuildUpdateSql return the update sql statement based on the update option
	BuildUpdateSql(opt UpdateOption) string
}

type UpdateOption struct {
	Table           int
	Batch           int
	IsSpecialUpdate bool
}
