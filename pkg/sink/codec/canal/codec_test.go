package canal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	pevent "github.com/flowbehappy/tigate/pkg/common/event"
	newconfig "github.com/flowbehappy/tigate/pkg/config"
	newcommon "github.com/flowbehappy/tigate/pkg/sink/codec/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	ticonfig "github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TODO: claim check

func TestAllTypes(t *testing.T) {
	helper := pevent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(
		a tinyint primary key, b tinyint,
		c bool, d bool,
		e smallint, f smallint,
		g int, h int,
		i float, j float,
		k double, l double,
		m timestamp, n timestamp,
		o bigint, p bigint,
		q mediumint, r mediumint,
		s date, t date,
		u time, v time,
		w datetime, x datetime,
		y year, z year,
		aa varchar(10), ab varchar(10),
		ac varbinary(10), ad varbinary(10),
		ae bit(10), af bit(10),
		ag json, ah json,
		ai decimal(10,2), aj decimal(10,2),
		ak enum('a','b','c'), al enum('a','b','c'),
		am set('a','b','c'), an set('a','b','c'),
		ao tinytext, ap tinytext,
		aq tinyblob, ar tinyblob,
		as1 mediumtext, at mediumtext,
		au mediumblob, av mediumblob,
		aw longtext, ax longtext,
		ay longblob, az longblob,
		ba text, bb text,
		bc blob, bd blob,
		be char(10), bf char(10),
		bg binary(10), bh binary(10))`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(
		a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai,ak,am,ao,aq,as1,au,aw,ay,ba,bc,be,bg) values (
			1, true, -1, 123, 153.123,153.123,
			"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59",
			"2015-12-20 23:58:58",1970,"测试",0x0102030405060708090A,81,
			'{"key1": "value1"}', 129012.12, 'a', 'b', "5rWL6K+VdGV4dA==",
			0x89504E470D0A1A0A,"5rWL6K+VdGV4dA==",0x4944330300000000,
			"5rWL6K+VdGV4dA==",0x504B0304140000000800,"5rWL6K+VdGV4dA==",
			0x255044462D312E34,"Alice",0x0102030405060708090A)`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	tableInfo := helper.GetTableInfo(job)

	rowEvent := &pevent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
	value, err := newJSONMessageForDML(rowEvent, protocolConfig, false, "")
	require.NoError(t, err)

	var message JSONMessage

	err = json.Unmarshal(value, &message)
	require.NoError(t, err)

	require.Equal(t, int64(0), message.ID)
	require.Equal(t, "test", message.Schema)
	require.Equal(t, "t", message.Table)
	require.Equal(t, []string{"a"}, message.PKNames)
	require.Equal(t, false, message.IsDDL)
	require.Equal(t, "INSERT", message.EventType)
	require.Equal(t, "", message.Query)

	sqlValue, err := json.Marshal(message.SQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":-6,"aa":12,"ab":12,"ac":2004,"ad":2004,"ae":-7,"af":-7,"ag":12,"ah":12,"ai":3,"aj":3,"ak":4,"al":4,"am":-7,"an":-7,"ao":2005,"ap":2005,"aq":2004,"ar":2004,"as1":2005,"at":2005,"au":2004,"av":2004,"aw":2005,"ax":2005,"ay":2004,"az":2004,"b":-6,"ba":2005,"bb":2005,"bc":2004,"bd":2004,"be":1,"bf":1,"bg":2004,"bh":2004,"c":-6,"d":-6,"e":5,"f":5,"g":4,"h":4,"i":7,"j":7,"k":8,"l":8,"m":93,"n":93,"o":-5,"p":-5,"q":4,"r":4,"s":91,"t":91,"u":92,"v":92,"w":93,"x":93,"y":12,"z":12}`, string(sqlValue))

	mysqlValue, err := json.Marshal(message.MySQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":"tinyint","aa":"varchar","ab":"varchar","ac":"varbinary","ad":"varbinary","ae":"bit","af":"bit","ag":"json","ah":"json","ai":"decimal","aj":"decimal","ak":"enum","al":"enum","am":"set","an":"set","ao":"tinytext","ap":"tinytext","aq":"tinyblob","ar":"tinyblob","as1":"mediumtext","at":"mediumtext","au":"mediumblob","av":"mediumblob","aw":"longtext","ax":"longtext","ay":"longblob","az":"longblob","b":"tinyint","ba":"text","bb":"text","bc":"blob","bd":"blob","be":"char","bf":"char","bg":"binary","bh":"binary","c":"tinyint","d":"tinyint","e":"smallint","f":"smallint","g":"int","h":"int","i":"float","j":"float","k":"double","l":"double","m":"timestamp","n":"timestamp","o":"bigint","p":"bigint","q":"mediumint","r":"mediumint","s":"date","t":"date","u":"time","v":"time","w":"datetime","x":"datetime","y":"year","z":"year"}`, string(mysqlValue))

	oldValue, err := json.Marshal(message.Old)
	require.NoError(t, err)
	require.Equal(t, "null", string(oldValue))

	newValue, err := json.Marshal(message.Data)
	require.NoError(t, err)
	require.Equal(t, `[{"a":"1","aa":"测试","ab":null,"ac":"\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n","ad":null,"ae":"81","af":null,"ag":"{\"key1\": \"value1\"}","ah":null,"ai":"129012.12","aj":null,"ak":"1","al":null,"am":"2","an":null,"ao":"5rWL6K+VdGV4dA==","ap":null,"aq":"PNG\r\n\u001a\n","ar":null,"as1":"5rWL6K+VdGV4dA==","at":null,"au":"ID3\u0003\u0000\u0000\u0000\u0000","av":null,"aw":"5rWL6K+VdGV4dA==","ax":null,"ay":"PK\u0003\u0004\u0014\u0000\u0000\u0000\b\u0000","az":null,"b":null,"ba":"5rWL6K+VdGV4dA==","bb":null,"bc":"%PDF-1.4","bd":null,"be":"Alice","bf":null,"bg":"\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n","bh":null,"c":"1","d":null,"e":"-1","f":null,"g":"123","h":null,"i":"153.123","j":null,"k":"153.123","l":null,"m":"1973-12-30 15:30:00","n":null,"o":"123","p":null,"q":"123","r":null,"s":"2000-01-01","t":null,"u":"23:59:59","v":null,"w":"2015-12-20 23:58:58","x":null,"y":"1970","z":null}]`, string(newValue))
}

func TestGeneralDMLEvent(t *testing.T) {
	// columnSelector
	{
		helper := pevent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")
		job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint)`)

		dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a) values (1)`)
		require.NotNil(t, dmlEvent)
		row, ok := dmlEvent.GetNextRow()
		require.True(t, ok)
		tableInfo := helper.GetTableInfo(job)

		replicaConfig := newconfig.GetDefaultReplicaConfig()
		replicaConfig.Sink.ColumnSelectors = []*newconfig.ColumnSelector{
			{
				Matcher: []string{"test.*"},
				Columns: []string{"a"},
			},
		}
		selectors, err := common.NewColumnSelectors(replicaConfig.Sink)
		require.NoError(t, err)

		rowEvent := &pevent.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       1,
			Event:          row,
			ColumnSelector: selectors.GetSelector("test", "t"),
			Callback:       func() {}}

		protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
		value, err := newJSONMessageForDML(rowEvent, protocolConfig, false, "")
		require.NoError(t, err)

		var message JSONMessage

		err = json.Unmarshal(value, &message)
		require.NoError(t, err)

		require.Equal(t, int64(0), message.ID)
		require.Equal(t, "test", message.Schema)
		require.Equal(t, "t", message.Table)
		require.Equal(t, []string{"a"}, message.PKNames)
		require.Equal(t, false, message.IsDDL)
		require.Equal(t, "INSERT", message.EventType)
		require.Equal(t, "", message.Query)

		sqlValue, err := json.Marshal(message.SQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":-6}`, string(sqlValue))

		mysqlValue, err := json.Marshal(message.MySQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":"tinyint"}`, string(mysqlValue))

		oldValue, err := json.Marshal(message.Old)
		require.NoError(t, err)
		require.Equal(t, "null", string(oldValue))

		newValue, err := json.Marshal(message.Data)
		require.NoError(t, err)
		require.Equal(t, `[{"a":"1"}]`, string(newValue))
	}
	// EnableTiDBExtension
	{
		helper := pevent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")
		job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint)`)

		dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a) values (1)`)
		require.NotNil(t, dmlEvent)
		row, ok := dmlEvent.GetNextRow()
		require.True(t, ok)
		tableInfo := helper.GetTableInfo(job)

		rowEvent := &pevent.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       1,
			Event:          row,
			ColumnSelector: common.NewDefaultColumnSelector(),
			Callback:       func() {}}

		protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
		protocolConfig.EnableTiDBExtension = true
		value, err := newJSONMessageForDML(rowEvent, protocolConfig, false, "")
		require.NoError(t, err)

		var message canalJSONMessageWithTiDBExtension

		err = json.Unmarshal(value, &message)
		require.NoError(t, err)

		require.Equal(t, int64(0), message.ID)
		require.Equal(t, "test", message.Schema)
		require.Equal(t, "t", message.Table)
		require.Equal(t, []string{"a"}, message.PKNames)
		require.Equal(t, false, message.IsDDL)
		require.Equal(t, "INSERT", message.EventType)
		require.Equal(t, "", message.Query)

		sqlValue, err := json.Marshal(message.SQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))

		mysqlValue, err := json.Marshal(message.MySQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))

		oldValue, err := json.Marshal(message.Old)
		require.NoError(t, err)
		require.Equal(t, "null", string(oldValue))

		newValue, err := json.Marshal(message.Data)
		require.NoError(t, err)
		require.Equal(t, `[{"a":"1","b":null}]`, string(newValue))

		require.Equal(t, uint64(1), message.Extensions.CommitTs)
		require.Equal(t, false, message.Extensions.OnlyHandleKey)
		require.Equal(t, "", message.Extensions.ClaimCheckLocation)
	}
	// multi pk
	{
		helper := pevent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")
		job := helper.DDL2Job(`create table test.t(a tinyint, c int, b tinyint, PRIMARY KEY (a, b))`)

		dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1,2,3)`)
		require.NotNil(t, dmlEvent)
		row, ok := dmlEvent.GetNextRow()
		require.True(t, ok)
		tableInfo := helper.GetTableInfo(job)

		rowEvent := &pevent.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       1,
			Event:          row,
			ColumnSelector: common.NewDefaultColumnSelector(),
			Callback:       func() {}}

		protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
		value, err := newJSONMessageForDML(rowEvent, protocolConfig, false, "")
		require.NoError(t, err)

		var message JSONMessage

		err = json.Unmarshal(value, &message)
		require.NoError(t, err)

		require.Equal(t, []string{"a", "b"}, message.PKNames)

		sqlValue, err := json.Marshal(message.SQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":-6,"b":-6,"c":4}`, string(sqlValue))

		mysqlValue, err := json.Marshal(message.MySQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":"tinyint","b":"tinyint","c":"int"}`, string(mysqlValue))

		oldValue, err := json.Marshal(message.Old)
		require.NoError(t, err)
		require.Equal(t, "null", string(oldValue))

		newValue, err := json.Marshal(message.Data)
		require.NoError(t, err)
		require.Equal(t, `[{"a":"1","b":"3","c":"2"}]`, string(newValue))
	}
	// message large
	{
		helper := pevent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")
		job := helper.DDL2Job(`create table test.t(
			a tinyint primary key, b tinyint,
			c bool, d bool,
			e smallint, f smallint,
			g int, h int,
			i float, j float,
			k double, l double,
			m timestamp, n timestamp,
			o bigint, p bigint,
			q mediumint, r mediumint,
			s date, t date,
			u time, v time,
			w datetime, x datetime,
			y year, z year,
			aa varchar(10), ab varchar(10),
			ac varbinary(10), ad varbinary(10),
			ae bit(10), af bit(10),
			ag json, ah json,
			ai decimal(10,2), aj decimal(10,2),
			ak enum('a','b','c'), al enum('a','b','c'),
			am set('a','b','c'), an set('a','b','c'),
			ao tinytext, ap tinytext,
			aq tinyblob, ar tinyblob,
			as1 mediumtext, at mediumtext,
			au mediumblob, av mediumblob,
			aw longtext, ax longtext,
			ay longblob, az longblob,
			ba text, bb text,
			bc blob, bd blob,
			be char(10), bf char(10),
			bg binary(10), bh binary(10))`)

		dmlEvent := helper.DML2Event("test", "t", `insert into test.t(
			a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai,ak,am,ao,aq,as1,au,aw,ay,ba,bc,be,bg) values (
				1, true, -1, 123, 153.123,153.123,
				"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59",
				"2015-12-20 23:58:58",1970,"测试",0x0102030405060708090A,81,
				'{"key1": "value1"}', 129012.12, 'a', 'b', "5rWL6K+VdGV4dA==",
				0x89504E470D0A1A0A,"5rWL6K+VdGV4dA==",0x4944330300000000,
				"5rWL6K+VdGV4dA==",0x504B0304140000000800,"5rWL6K+VdGV4dA==",
				0x255044462D312E34,"Alice",0x0102030405060708090A)`)
		require.NotNil(t, dmlEvent)
		row, ok := dmlEvent.GetNextRow()
		require.True(t, ok)
		tableInfo := helper.GetTableInfo(job)

		rowEvent := &pevent.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       1,
			Event:          row,
			ColumnSelector: common.NewDefaultColumnSelector(),
			Callback:       func() {}}

		protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
		protocolConfig = protocolConfig.WithMaxMessageBytes(300)
		protocolConfig.EnableTiDBExtension = true
		encoder, err := NewJSONRowEventEncoder(context.Background(), protocolConfig)
		require.NoError(t, err)
		err = encoder.AppendRowChangedEvent(context.Background(), "", rowEvent)
		require.ErrorIs(t, err, cerror.ErrMessageTooLarge)
	}
	// message large + handle only
	{
		helper := pevent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")
		job := helper.DDL2Job(`create table test.t(
			a tinyint primary key, b tinyint,
			c bool, d bool,
			e smallint, f smallint,
			g int, h int,
			i float, j float,
			k double, l double,
			m timestamp, n timestamp,
			o bigint, p bigint,
			q mediumint, r mediumint,
			s date, t date,
			u time, v time,
			w datetime, x datetime,
			y year, z year,
			aa varchar(10), ab varchar(10),
			ac varbinary(10), ad varbinary(10),
			ae bit(10), af bit(10),
			ag json, ah json,
			ai decimal(10,2), aj decimal(10,2),
			ak enum('a','b','c'), al enum('a','b','c'),
			am set('a','b','c'), an set('a','b','c'),
			ao tinytext, ap tinytext,
			aq tinyblob, ar tinyblob,
			as1 mediumtext, at mediumtext,
			au mediumblob, av mediumblob,
			aw longtext, ax longtext,
			ay longblob, az longblob,
			ba text, bb text,
			bc blob, bd blob,
			be char(10), bf char(10),
			bg binary(10), bh binary(10))`)

		dmlEvent := helper.DML2Event("test", "t", `insert into test.t(
			a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai,ak,am,ao,aq,as1,au,aw,ay,ba,bc,be,bg) values (
				1, true, -1, 123, 153.123,153.123,
				"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59",
				"2015-12-20 23:58:58",1970,"测试",0x0102030405060708090A,81,
				'{"key1": "value1"}', 129012.12, 'a', 'b', "5rWL6K+VdGV4dA==",
				0x89504E470D0A1A0A,"5rWL6K+VdGV4dA==",0x4944330300000000,
				"5rWL6K+VdGV4dA==",0x504B0304140000000800,"5rWL6K+VdGV4dA==",
				0x255044462D312E34,"Alice",0x0102030405060708090A)`)
		require.NotNil(t, dmlEvent)
		row, ok := dmlEvent.GetNextRow()
		require.True(t, ok)
		tableInfo := helper.GetTableInfo(job)

		rowEvent := &pevent.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       1,
			Event:          row,
			ColumnSelector: common.NewDefaultColumnSelector(),
			Callback:       func() {}}

		protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
		protocolConfig = protocolConfig.WithMaxMessageBytes(300)
		protocolConfig.LargeMessageHandle.LargeMessageHandleOption = ticonfig.LargeMessageHandleOptionHandleKeyOnly
		protocolConfig.EnableTiDBExtension = true
		encoder, err := NewJSONRowEventEncoder(context.Background(), protocolConfig)
		require.NoError(t, err)
		err = encoder.AppendRowChangedEvent(context.Background(), "", rowEvent)
		require.NoError(t, err)

		messages := encoder.Build()
		require.Equal(t, 1, len(messages))
		require.Equal(t, uint64(1), messages[0].Ts)
		require.Equal(t, "test", *messages[0].Schema)
		require.Equal(t, "t", *messages[0].Table)
		require.Equal(t, model.MessageTypeRow, messages[0].Type)
		require.NotNil(t, messages[0].Callback)

		value := messages[0].Value
		var message canalJSONMessageWithTiDBExtension

		err = json.Unmarshal(value, &message)
		require.NoError(t, err)

		sqlValue, err := json.Marshal(message.SQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":-6}`, string(sqlValue))

		mysqlValue, err := json.Marshal(message.MySQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":"tinyint"}`, string(mysqlValue))

		oldValue, err := json.Marshal(message.Old)
		require.NoError(t, err)
		require.Equal(t, "null", string(oldValue))

		newValue, err := json.Marshal(message.Data)
		require.NoError(t, err)
		require.Equal(t, `[{"a":"1"}]`, string(newValue))

		require.Equal(t, true, message.Extensions.OnlyHandleKey)
	}
}

// insert / update(with only updated or not) / delete
func TestDMLTypeEvent(t *testing.T) {
	helper := pevent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint)`)

	protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
	encoder, err := NewJSONRowEventEncoder(context.Background(), protocolConfig)
	require.NoError(t, err)
	tableInfo := helper.GetTableInfo(job)

	// insert event

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,b) values (1,3)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	log.Info("insertRow", zap.Any("row", insertRow), zap.Any("preRow", insertRow.PreRow.IsEmpty()), zap.Any("Row", insertRow.Row.IsEmpty()))

	rowEvent := &pevent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	err = encoder.AppendRowChangedEvent(context.Background(), "", rowEvent)
	require.NoError(t, err)

	messages := encoder.Build()
	require.Equal(t, 1, len(messages))
	require.Equal(t, uint64(1), messages[0].Ts)
	require.Equal(t, "test", *messages[0].Schema)
	require.Equal(t, "t", *messages[0].Table)
	require.Equal(t, model.MessageTypeRow, messages[0].Type)

	var value JSONMessage

	err = json.Unmarshal(messages[0].Value, &value)
	require.NoError(t, err)

	require.Equal(t, int64(0), value.ID)
	require.Equal(t, "test", value.Schema)
	require.Equal(t, "t", value.Table)
	require.Equal(t, []string{"a"}, value.PKNames)
	require.Equal(t, false, value.IsDDL)
	require.Equal(t, "INSERT", value.EventType)
	require.Equal(t, "", value.Query)

	sqlValue, err := json.Marshal(value.SQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))

	mysqlValue, err := json.Marshal(value.MySQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))

	oldValue, err := json.Marshal(value.Old)
	require.NoError(t, err)
	require.Equal(t, "null", string(oldValue))

	newValue, err := json.Marshal(value.Data)
	require.NoError(t, err)
	require.Equal(t, `[{"a":"1","b":"3"}]`, string(newValue))

	// update
	dmlEvent = helper.DML2Event("test", "t", `update test.t set b = 2 where a = 1`)
	require.NotNil(t, dmlEvent)
	updateRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	updateRow.PreRow = insertRow.Row

	updateRowEvent := &pevent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       2,
		Event:          updateRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	err = encoder.AppendRowChangedEvent(context.Background(), "", updateRowEvent)
	require.NoError(t, err)

	messages = encoder.Build()
	require.Equal(t, 1, len(messages))
	require.Equal(t, uint64(2), messages[0].Ts)
	require.Equal(t, "test", *messages[0].Schema)
	require.Equal(t, "t", *messages[0].Table)
	require.Equal(t, model.MessageTypeRow, messages[0].Type)

	err = json.Unmarshal(messages[0].Value, &value)
	require.NoError(t, err)

	require.Equal(t, int64(0), value.ID)
	require.Equal(t, "test", value.Schema)
	require.Equal(t, "t", value.Table)
	require.Equal(t, []string{"a"}, value.PKNames)
	require.Equal(t, false, value.IsDDL)
	require.Equal(t, "UPDATE", value.EventType)
	require.Equal(t, "", value.Query)

	sqlValue, err = json.Marshal(value.SQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))

	mysqlValue, err = json.Marshal(value.MySQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))

	oldValue, err = json.Marshal(value.Old)
	require.NoError(t, err)
	require.Equal(t, `[{"a":"1","b":"3"}]`, string(oldValue))

	newValue, err = json.Marshal(value.Data)
	require.NoError(t, err)
	require.Equal(t, `[{"a":"1","b":"2"}]`, string(newValue))

	// delete
	deleteRow := updateRow
	deleteRow.PreRow = updateRow.Row
	deleteRow.Row = chunk.Row{}

	deleteRowEvent := &pevent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       3,
		Event:          deleteRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	err = encoder.AppendRowChangedEvent(context.Background(), "", deleteRowEvent)
	require.NoError(t, err)

	messages = encoder.Build()
	require.Equal(t, 1, len(messages))
	require.Equal(t, uint64(3), messages[0].Ts)
	require.Equal(t, "test", *messages[0].Schema)
	require.Equal(t, "t", *messages[0].Table)
	require.Equal(t, model.MessageTypeRow, messages[0].Type)

	err = json.Unmarshal(messages[0].Value, &value)
	require.NoError(t, err)

	require.Equal(t, int64(0), value.ID)
	require.Equal(t, "test", value.Schema)
	require.Equal(t, "t", value.Table)
	require.Equal(t, []string{"a"}, value.PKNames)
	require.Equal(t, false, value.IsDDL)
	require.Equal(t, "DELETE", value.EventType)
	require.Equal(t, "", value.Query)

	sqlValue, err = json.Marshal(value.SQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))

	mysqlValue, err = json.Marshal(value.MySQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))

	oldValue, err = json.Marshal(value.Old)
	require.NoError(t, err)
	require.Equal(t, "null", string(oldValue))

	newValue, err = json.Marshal(value.Data)
	require.NoError(t, err)
	require.Equal(t, `[{"a":"1","b":"2"}]`, string(newValue))

	// update with only updated columns
	protocolConfig.OnlyOutputUpdatedColumns = true
	encoder, err = NewJSONRowEventEncoder(context.Background(), protocolConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(context.Background(), "", updateRowEvent)
	require.NoError(t, err)

	messages = encoder.Build()
	require.Equal(t, 1, len(messages))
	require.Equal(t, uint64(2), messages[0].Ts)
	require.Equal(t, "test", *messages[0].Schema)
	require.Equal(t, "t", *messages[0].Table)
	require.Equal(t, model.MessageTypeRow, messages[0].Type)

	err = json.Unmarshal(messages[0].Value, &value)
	require.NoError(t, err)

	require.Equal(t, int64(0), value.ID)
	require.Equal(t, "test", value.Schema)
	require.Equal(t, "t", value.Table)
	require.Equal(t, []string{"a"}, value.PKNames)
	require.Equal(t, false, value.IsDDL)
	require.Equal(t, "UPDATE", value.EventType)
	require.Equal(t, "", value.Query)

	sqlValue, err = json.Marshal(value.SQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))

	mysqlValue, err = json.Marshal(value.MySQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))

	oldValue, err = json.Marshal(value.Old)
	require.NoError(t, err)
	require.Equal(t, `[{"b":"3"}]`, string(oldValue))

	newValue, err = json.Marshal(value.Data)
	require.NoError(t, err)
	require.Equal(t, `[{"a":"1","b":"2"}]`, string(newValue))
}

// ddl
func TestDDLTypeEvent(t *testing.T) {
	helper := pevent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
	encoder, err := NewJSONRowEventEncoder(context.Background(), protocolConfig)
	require.NoError(t, err)

	ddlEvent := &pevent.DDLEvent{
		Query:      job.Query,
		Type:       byte(job.Type),
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 1,
	}

	message, err := encoder.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)
	require.Equal(t, uint64(1), message.Ts)
	require.Equal(t, "test", *message.Schema)
	require.Equal(t, "t", *message.Table)
	require.Equal(t, model.MessageTypeDDL, message.Type)

	var value JSONMessage
	err = json.Unmarshal(message.Value, &value)
	require.NoError(t, err)

	require.Equal(t, int64(0), value.ID)
	require.Equal(t, "test", value.Schema)
	require.Equal(t, "t", value.Table)
	require.Equal(t, true, value.IsDDL)
	require.Equal(t, "CREATE", value.EventType)
	require.Equal(t, 1>>18, value.ExecutionTime)
	require.Equal(t, job.Query, value.Query)

	// extension tidb
	protocolConfig.EnableTiDBExtension = true
	encoder, err = NewJSONRowEventEncoder(context.Background(), protocolConfig)
	require.NoError(t, err)

	message, err = encoder.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	var extensionValue canalJSONMessageWithTiDBExtension
	err = json.Unmarshal(message.Value, &extensionValue)
	require.NoError(t, err)

	require.Equal(t, uint64(1), extensionValue.Extensions.CommitTs)
}

// checkpointTs
func TestCheckpointTs(t *testing.T) {
	helper := pevent.NewEventTestHelper(t)
	defer helper.Close()

	protocolConfig := newcommon.NewConfig(config.ProtocolCanalJSON)
	encoder, err := NewJSONRowEventEncoder(context.Background(), protocolConfig)
	require.NoError(t, err)

	message, err := encoder.EncodeCheckpointEvent(1)
	require.NoError(t, err)
	require.Nil(t, message)

	// with extension
	protocolConfig.EnableTiDBExtension = true
	encoder, err = NewJSONRowEventEncoder(context.Background(), protocolConfig)
	require.NoError(t, err)
	message, err = encoder.EncodeCheckpointEvent(1)
	require.NoError(t, err)

	require.Equal(t, config.ProtocolCanalJSON, message.Protocol)
	require.Nil(t, message.Schema)
	require.Nil(t, message.Table)
	require.Equal(t, uint64(1), message.Ts)

	var value canalJSONMessageWithTiDBExtension
	err = json.Unmarshal(message.Value, &value)
	require.NoError(t, err)

	require.Equal(t, 0, value.ID)
	require.Equal(t, false, value.IsDDL)
	require.Equal(t, tidbWaterMarkType, value.EventType)
	require.Equal(t, 1>>18, value.ExecutionTime)
	require.Equal(t, 1, value.Extensions.WatermarkTs)
}
