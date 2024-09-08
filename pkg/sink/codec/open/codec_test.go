package open

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/mounter"
	"github.com/pingcap/tiflow/pkg/config"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

// test 每个 type 的解码，test column selector， test callback, handle only

// Test Column Type: TinyInt, Tinyint(null), Bool, Bool(null), SmallInt, SmallInt(null)
func TestBasicType(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint, c bool, d bool, e smallint, f smallint)`)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,c, e) values (1, true, -1)`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	tableInfo := helper.GetTableInfo(job)

	rowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	protocolConfig := ticommon.NewConfig(config.ProtocolOpen)
	key, value, err := encodeRowChangeEventWithoutCompress(rowEvent, protocolConfig, false, "")
	require.NoError(t, err)
	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(key))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":1,"f":65,"v":null},"c":{"t":1,"f":65,"v":1},"d":{"t":1,"f":65,"v":null},"e":{"t":2,"f":65,"v":-1},"f":{"t":2,"f":65,"v":null}}}`, string(value))
}
