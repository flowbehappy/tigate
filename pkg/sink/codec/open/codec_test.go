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
// TODO:ColumnNull 是什么类型？
// Test Column Type: TinyInt, Tinyint(null), Bool, Bool(null), SmallInt, SmallInt(null), Int, Int(null), Float, Float(nulll), Double, Double(null),
// Timestamp, Timestamp(null), BigInt, BigInt(null), MediumInt, MediumInt(null), Date, Date(null), Time, Time(null)
func TestBasicType(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint, c bool, d bool, e smallint, f smallint, g int, h int, i float, j float, k double, l double, m timestamp, n timestamp, o bigint, p bigint, q mediumint, r mediumint, s date, t date, u time, v time)`)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,c,e,g,i,k,m,o,q,s,u) values (1, true, -1, 123, 153.123,153.123,"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59")`)
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
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":1,"f":65,"v":null},"c":{"t":1,"f":65,"v":1},"d":{"t":1,"f":65,"v":null},"e":{"t":2,"f":65,"v":-1},"f":{"t":2,"f":65,"v":null},"g":{"t":3,"f":65,"v":123},"h":{"t":3,"f":65,"v":null},"i":{"t":4,"f":65,"v":153.123},"j":{"t":4,"f":65,"v":null},"k":{"t":5,"f":65,"v":153.123},"l":{"t":5,"f":65,"v":null},"m":{"t":7,"f":65,"v":"1973-12-30 15:30:00"},"n":{"t":7,"f":65,"v":null},"o":{"t":8,"f":65,"v":123},"p":{"t":8,"f":65,"v":null},"q":{"t":9,"f":65,"v":123},"r":{"t":9,"f":65,"v":null},"s":{"t":10,"f":65,"v":"2000-01-01"},"t":{"t":10,"f":65,"v":null},"u":{"t":11,"f":65,"v":"23:59:59"},"v":{"t":11,"f":65,"v":null}}}`, string(value))

}
