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

import (
	"bytes"
	"fmt"
	"math/rand"
)

const createTable = `
CREATE TABLE if not exists sbtest%d (
id bigint NOT NULL,
k bigint NOT NULL DEFAULT '0',
c char(30) NOT NULL DEFAULT '',
pad char(20) NOT NULL DEFAULT '',
PRIMARY KEY (id),
KEY k_1 (k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type SysbenchWorkload struct{}

func NewSysbenchWorkload() Workload {
	return &SysbenchWorkload{}
}

// BuildCreateTableStatement returns the create-table sql of the table n
func (c *SysbenchWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createTable, n)
}

func (c *SysbenchWorkload) BuildInsertSql(tableN int, rowCount int) string {
	n := rand.Int63()
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("insert into sbtest%d (id, k, c, pad) values(%d, %d, 'abcdefghijklmnopsrstuvwxyzabcd', 'abcdefghijklmnopsrst')", tableN, n, n))

	for r := 1; r < rowCount; r++ {
		n = rand.Int63()
		buf.WriteString(fmt.Sprintf(",(%d, %d, 'abcdefghijklmnopsrstuvwxyzabcd', 'abcdefghijklmnopsrst')", n, n))
	}
	return buf.String()
}

func GetAddIndexStatement(n int) string {
	return fmt.Sprintf("alter table sbtest%d add index k2(k);", n)
}

func (c *SysbenchWorkload) BuildUpdateSql(opts UpdateOption) string {
	panic("unimplemented")
}
