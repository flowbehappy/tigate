package schema

import (
	"fmt"
	mrand "math/rand"
	"strings"
	"sync"
	"sync/atomic"
)

const createDataTableFormat = `
CREATE TABLE IF NOT EXISTS Data%d (
    model_id bigint(20) unsigned NOT NULL,
    object_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    object_value longblob,
    version int(11) unsigned NOT NULL,
    PRIMARY KEY(object_id)
);
`

const createIndexTableFormat = `
CREATE TABLE IF NOT EXISTS index_Data%d (
    object_id bigint(20) unsigned NOT NULL,
    reference_id bigint(20) DEFAULT NULL,
    guid varbinary(767) DEFAULT NULL,
    version int(11) unsigned NOT NULL,
    INDEX IndexOnGuid(guid, object_id),
    INDEX IndexOnReferenceId(reference_id, object_id),
    PRIMARY KEY(object_id)
);
`

type UUUWorkload struct{}

func NewUUUWorkload() Workload {
	return &UUUWorkload{}
}

// BuildCreateTableStatement returns the create-table sql for both Data and index_Data tables
func (c *UUUWorkload) BuildCreateTableStatement(n int) string {
	if n%2 == 0 {
		return fmt.Sprintf(createDataTableFormat, n)
	}
	return fmt.Sprintf(createIndexTableFormat, n)
}

var count atomic.Int64

func (c *UUUWorkload) BuildInsertSql(tableN int, batchSize int) string {
	panic("unimplemented")
}

// BuildInsertSql returns two insert statements for Data and index_Data tables
func (c *UUUWorkload) BuildInsertSqlWithValues(tableN int, batchSize int) (string, []interface{}) {
	var sql string
	values := make([]interface{}, 0, batchSize*4) // 预分配空间：每行4个值
	count.Add(1)

	if count.Load()%2 == 0 {
		// Data table insert
		sql = fmt.Sprintf("INSERT INTO Data%d (model_id, object_id, object_value, version) VALUES ", tableN)
		placeholders := make([]string, batchSize)
		for r := 0; r < batchSize; r++ {
			n := mrand.Int63()
			values = append(values, n, n, generateString(), 1)
			placeholders[r] = "(?,?,?,?)"
		}
		return sql + strings.Join(placeholders, ","), values
	} else {
		// Index table insert
		sql = fmt.Sprintf("INSERT INTO index_Data%d (object_id, reference_id, guid, version) VALUES ", tableN)
		placeholders := make([]string, batchSize)

		for r := 0; r < batchSize; r++ {
			n := mrand.Int63()
			values = append(values, n, n, generateString(), 1)
			placeholders[r] = "(?,?,?,?)"
		}
		return sql + strings.Join(placeholders, ","), values
	}
}

func (c *UUUWorkload) BuildUpdateSql(opts UpdateOption) string {
	panic("unimplemented")
}

var (
	preGeneratedString string
	once               sync.Once
)

const columnLen = 720

func generateString() string {
	once.Do(func() {
		builder := strings.Builder{}
		for i := 0; i < columnLen; i++ {
			builder.WriteString(fmt.Sprintf("%d", i))
		}
		preGeneratedString = builder.String()
	})
	return preGeneratedString
}
