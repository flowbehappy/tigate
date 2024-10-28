package canal

import (
	"encoding/json"
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	pevent "github.com/flowbehappy/tigate/pkg/common/event"
	newcommon "github.com/flowbehappy/tigate/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

// basic type
// large message
// insert
// update(with only update column)
// delete
// ddl
// checkpointTs

func TestBasicType(t *testing.T) {
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
	value, err := newJSONMessageForDML(rowEvent, protocolConfig, false, "", charmap.ISO8859_1.NewDecoder())
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
	require.Equal(t, `[{"a":"1","aa":"测试","ab":null,"ac":"\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\t\n","ad":null,"ae":"81","af":null,"ag":"{\"key1\": \"value1\"}","ah":null,"ai":"129012.12","aj":null,"ak":"1","al":null,"am":"2","an":null,"ao":"5rWL6K+VdGV4dA==","ap":null,"aq":"PNG\r\n\u001a\n","ar":null,"as1":"5rWL6K+VdGV4dA==","at":null,"au":"ID3\u0003\u0000\u0000\u0000\u0000","av":null,"aw":"5rWL6K+VdGV4dA==","ax":null,"ay":"PK\u0003\u0004\u0014\u0000\u0000\u0000\u0008\u0000","az":null,"b":null,"ba":"5rWL6K+VdGV4dA==","bb":null,"bc":"%PDF-1.4","bd":null,"be":"Alice","bf":null,"bg":"\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\t\n","bh":null,"c":"1","d":null,"e":"-1","f":null,"g":"123","h":null,"i":"153.123","j":null,"k":"153.123","l":null,"m":"1973-12-30 15:30:00","n":null,"o":"123","p":null,"q":"123","r":null,"s":"2000-01-01","t":null,"u":"23:59:59","v":null,"w":"2015-12-20 23:58:58","x":null,"y":"1970","z":null}]`, string(newValue))
}
