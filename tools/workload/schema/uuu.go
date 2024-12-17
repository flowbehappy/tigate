package schema

import (
	"bytes"
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
	return fmt.Sprintf(createDataTableFormat, n)
}

var count atomic.Int64

// BuildInsertSql returns two insert statements for Data and index_Data tables
func (c *UUUWorkload) BuildInsertSql(tableN int, batchSize int) string {
	var dataBuf, indexBuf bytes.Buffer
	n := mrand.Int63()
	count.Add(1)
	if count.Load()%2 == 0 {
		// Data table insert
		dataBuf.WriteString(fmt.Sprintf("INSERT INTO Data%d (model_id, object_id, object_value, version) VALUES(%d, %d, %s, 1)", tableN, n, n, generateString()))
		for r := 1; r < batchSize; r++ {
			n := mrand.Int63()
			dataBuf.WriteString(fmt.Sprintf(",(%d, %d, %s, 1)", n, n, generateString()))
		}
		return dataBuf.String()
	} else {
		indexBuf.WriteString(fmt.Sprintf("INSERT INTO index_Data%d (object_id, reference_id, guid, version) VALUES(%d, %d, %s, 1)", tableN, n, n, generateString()))
		for r := 1; r < batchSize; r++ {
			n := mrand.Int63()
			indexBuf.WriteString(fmt.Sprintf(",(%d, %d, %s, 1)", n, n, generateString()))
		}
		return indexBuf.String()
	}
}

func (c *UUUWorkload) BuildUpdateSql(opts UpdateOption) string {
	panic("unimplemented")
}

var (
	preGeneratedString string
	once               sync.Once
)

func generateString() string {
	once.Do(func() {
		builder := strings.Builder{}
		for i := 0; i < 1000; i++ {
			builder.WriteString(fmt.Sprintf("%d", i))
		}
		preGeneratedString = builder.String()
	})
	return preGeneratedString
}
