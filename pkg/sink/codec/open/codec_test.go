package open

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/mounter"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/pkg/config"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

// TODO:E2E 测一下double/float nan 和inf 的问题

// test 每个 type 的解码，test column selector， test callback, handle only

// Test Column Type: TinyInt, Tinyint(null), Bool, Bool(null), SmallInt, SmallInt(null), Int, Int(null), Float, Float(nulll), Double, Double(null),
// Timestamp, Timestamp(null), BigInt, BigInt(null), MediumInt, MediumInt(null), Date, Date(null), Time, Time(null), Datetime, Datetime(null), Year, Year(null),
// Varchar, Varchar(null), VarBinary, VarBinary(null), Bit, Bit(null), Json, Json(null), Decimal, Decimal(null), Enum, Enum(null), Set, Set(null), TinyText, TinyText(null), TinyBlob, TinyBlob(null), MediumText, MediumText(null), MediumBlob, MediumBlob(null),LongText, LongText(null),LongBlob, LongBlob(null), Text, Text(null), Blob, Blob(null), char, char(null), binary, binary(null)
func TestBasicType(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint, c bool, d bool, e smallint, f smallint, g int, h int, i float, j float, k double, l double, m timestamp, n timestamp, o bigint, p bigint, q mediumint, r mediumint, s date, t date, u time, v time, w datetime, x datetime, y year, z year, aa varchar(10), ab varchar(10), ac varbinary(10), ad varbinary(10), ae bit(10), af bit(10), ag json, ah json, ai decimal(10,2), aj decimal(10,2), ak enum('a','b','c'), al enum('a','b','c'), am set('a','b','c'), an set('a','b','c'), ao tinytext, ap tinytext, aq tinyblob, ar tinyblob, as1 mediumtext, at mediumtext, au mediumblob, av mediumblob, aw longtext, ax longtext, ay longblob, az longblob, ba text, bb text, bc blob, bd blob, be char(10), bf char(10), bg binary(10), bh binary(10))`)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai,ak,am,ao,aq,as1,au,aw,ay,ba,bc,be,bg) values (1, true, -1, 123, 153.123,153.123,"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59","2015-12-20 23:58:58",1970,"测试",0x0102030405060708090A,81,'{"key1": "value1"}', 129012.12, 'a', 'b', "5rWL6K+VdGV4dA==", 0x89504E470D0A1A0A,"5rWL6K+VdGV4dA==",0x4944330300000000,"5rWL6K+VdGV4dA==",0x504B0304140000000800,"5rWL6K+VdGV4dA==",0x255044462D312E34,"Alice",0x0102030405060708090A)`)
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
	key, value, _, err := encodeRowChangedEvent(rowEvent, protocolConfig, false, "")
	require.NoError(t, err)
	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(key))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":1,"f":65,"v":null},"c":{"t":1,"f":65,"v":1},"d":{"t":1,"f":65,"v":null},"e":{"t":2,"f":65,"v":-1},"f":{"t":2,"f":65,"v":null},"g":{"t":3,"f":65,"v":123},"h":{"t":3,"f":65,"v":null},"i":{"t":4,"f":65,"v":153.123},"j":{"t":4,"f":65,"v":null},"k":{"t":5,"f":65,"v":153.123},"l":{"t":5,"f":65,"v":null},"m":{"t":7,"f":65,"v":"1973-12-30 15:30:00"},"n":{"t":7,"f":65,"v":null},"o":{"t":8,"f":65,"v":123},"p":{"t":8,"f":65,"v":null},"q":{"t":9,"f":65,"v":123},"r":{"t":9,"f":65,"v":null},"s":{"t":10,"f":65,"v":"2000-01-01"},"t":{"t":10,"f":65,"v":null},"u":{"t":11,"f":65,"v":"23:59:59"},"v":{"t":11,"f":65,"v":null},"w":{"t":12,"f":65,"v":"2015-12-20 23:58:58"},"x":{"t":12,"f":65,"v":null},"y":{"t":13,"f":193,"v":1970},"z":{"t":13,"f":193,"v":null},"aa":{"t":15,"f":64,"v":"测试"},"ab":{"t":15,"f":64,"v":null},"ac":{"t":15,"f":65,"v":"\\x01\\x02\\x03\\x04\\x05\\x06\\a\\b\\t\\n"},"ad":{"t":15,"f":65,"v":null},"ae":{"t":16,"f":193,"v":81},"af":{"t":16,"f":193,"v":null},"ag":{"t":245,"f":65,"v":"{\"key1\": \"value1\"}"},"ah":{"t":245,"f":65,"v":null},"ai":{"t":246,"f":65,"v":"129012.12"},"aj":{"t":246,"f":65,"v":null},"ak":{"t":247,"f":64,"v":1},"al":{"t":247,"f":64,"v":null},"am":{"t":248,"f":64,"v":2},"an":{"t":248,"f":64,"v":null},"ao":{"t":249,"f":64,"v":"NXJXTDZLK1ZkR1Y0ZEE9PQ=="},"ap":{"t":249,"f":64,"v":null},"aq":{"t":249,"f":65,"v":"iVBORw0KGgo="},"ar":{"t":249,"f":65,"v":null},"as1":{"t":250,"f":64,"v":"NXJXTDZLK1ZkR1Y0ZEE9PQ=="},"at":{"t":250,"f":64,"v":null},"au":{"t":250,"f":65,"v":"SUQzAwAAAAA="},"av":{"t":250,"f":65,"v":null},"aw":{"t":251,"f":64,"v":"NXJXTDZLK1ZkR1Y0ZEE9PQ=="},"ax":{"t":251,"f":64,"v":null},"ay":{"t":251,"f":65,"v":"UEsDBBQAAAAIAA=="},"az":{"t":251,"f":65,"v":null},"ba":{"t":252,"f":64,"v":"NXJXTDZLK1ZkR1Y0ZEE9PQ=="},"bb":{"t":252,"f":64,"v":null},"bc":{"t":252,"f":65,"v":"JVBERi0xLjQ="},"bd":{"t":252,"f":65,"v":null},"be":{"t":254,"f":64,"v":"Alice"},"bf":{"t":254,"f":64,"v":null},"bg":{"t":254,"f":65,"v":"\\x01\\x02\\x03\\x04\\x05\\x06\\a\\b\\t\\n"},"bh":{"t":254,"f":65,"v":null}}}`, string(value))
}

// Including insert / update / delete
func TestDMLEvent(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	protocolConfig := ticommon.NewConfig(config.ProtocolOpen)

	// Insert
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	insertRowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	key, value, length, err := encodeRowChangedEvent(insertRowEvent, protocolConfig, false, "")
	require.NoError(t, err)

	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(key))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":3,"f":65,"v":123}}}`, string(value))
	require.Equal(t, len(string(key))+len(string(value))+ticommon.MaxRecordOverhead+16+8, length)

	// Update
	dmlEvent = helper.DML2Event("test", "t", `update test.t set b = 456 where a = 1`)
	require.NotNil(t, dmlEvent)
	updateRow, ok := dmlEvent.GetNextRow()
	updateRow.PreRow = insertRow.Row
	require.True(t, ok)

	updateRowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       2,
		Event:          updateRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	key, value, _, err = encodeRowChangedEvent(updateRowEvent, protocolConfig, false, "")
	require.NoError(t, err)

	require.Equal(t, `{"ts":2,"scm":"test","tbl":"t","t":1}`, string(key))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":3,"f":65,"v":456}},"p":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":3,"f":65,"v":123}}}`, string(value))

	// delete
	{
		deleteRow := updateRow
		deleteRow.PreRow = updateRow.Row
		deleteRow.Row = chunk.Row{}
		require.True(t, ok)

		updateRowEvent := &common.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       3,
			Event:          deleteRow,
			ColumnSelector: common.NewDefaultColumnSelector(),
			Callback:       func() {}}

		key, value, _, err := encodeRowChangedEvent(updateRowEvent, protocolConfig, false, "")
		require.NoError(t, err)

		require.Equal(t, `{"ts":3,"scm":"test","tbl":"t","t":1}`, string(key))
		require.Equal(t, `{"d":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":3,"f":65,"v":456}}}`, string(value))
	}

}

func TestOnlyOutputUpdatedEvent(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	protocolConfig := ticommon.NewConfig(config.ProtocolOpen)
	protocolConfig.OnlyOutputUpdatedColumns = true

	{
		job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int, c decimal(10,2), d json, e char(10), f binary(10), g blob)`)
		event := helper.DML2Event("test", "t", `insert into test.t values (1, 123, 123.12, '{"key1": "value1"}',"Alice",0x0102030405060708090A,0x4944330300000000)`)
		eventNew := helper.DML2Event("test", "t", `update test.t set b = 456,c = 456.45 where a = 1`)
		tableInfo := helper.GetTableInfo(job)

		preRow, _ := event.GetNextRow()
		row, _ := eventNew.GetNextRow()
		row.PreRow = preRow.Row

		updateRowEvent := &common.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       1,
			Event:          row,
			ColumnSelector: common.NewDefaultColumnSelector(),
			Callback:       func() {}}

		_, value, _, err := encodeRowChangedEvent(updateRowEvent, protocolConfig, false, "")
		require.NoError(t, err)

		require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":3,"f":65,"v":456},"c":{"t":246,"f":65,"v":"456.45"},"d":{"t":245,"f":65,"v":"{\"key1\": \"value1\"}"},"e":{"t":254,"f":64,"v":"Alice"},"f":{"t":254,"f":65,"v":"\\x01\\x02\\x03\\x04\\x05\\x06\\a\\b\\t\\n"},"g":{"t":252,"f":65,"v":"SUQzAwAAAAA="}},"p":{"b":{"t":3,"f":65,"v":123},"c":{"t":246,"f":65,"v":"123.12"}}}`, string(value))

	}
}

func TestHandleOnlyEvent(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	protocolConfig := ticommon.NewConfig(config.ProtocolOpen)

	// Insert
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	insertRowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	key, value, _, err := encodeRowChangedEvent(insertRowEvent, protocolConfig, true, "")
	require.NoError(t, err)

	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(key))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1}}}`, string(value))
}

func TestDDLEvent(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	protocolConfig := ticommon.NewConfig(config.ProtocolOpen)

	ddlEvent := &common.DDLEvent{
		Job:      job,
		CommitTS: 1,
	}

	key, value, err := encodeDDLEvent(ddlEvent, protocolConfig)
	require.NoError(t, err)

	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":2}`, string(key)[16:])
	require.Equal(t, `{"q":"create table test.t(a tinyint primary key, b int)","t":3}`, string(value)[8:]) // ?

}

func TestResolvedTsEvent(t *testing.T) {
	key, value, err := encodeResolvedTs(12345678)
	require.NoError(t, err)

	require.Equal(t, `{"ts":12345678,"t":3}`, string(key)[16:])
	require.Equal(t, 8, len(string(value)))

}

func TestEncodeWithColumnSelector(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.*"},
			Columns: []string{"a*"},
		},
	}
	selectors, err := common.NewColumnSelectors(replicaConfig)
	require.NoError(t, err)
	selector := selectors.GetSelector("test", "t")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	protocolConfig := ticommon.NewConfig(config.ProtocolOpen)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	insertRowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: selector,
		Callback:       func() {}}

	key, value, _, err := encodeRowChangedEvent(insertRowEvent, protocolConfig, false, "")
	require.NoError(t, err)

	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(key))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1}}}`, string(value))

	// todo: column selector 匹配后没有 handle 列报错
}
