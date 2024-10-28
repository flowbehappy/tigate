package canal

import (
	"encoding/json"
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	pevent "github.com/flowbehappy/tigate/pkg/common/event"
	newcommon "github.com/flowbehappy/tigate/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

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
		y year, z year)
		`)
	// job := helper.DDL2Job(`create table test.t(
	// 	a tinyint primary key, b tinyint,
	// 	c bool, d bool,
	// 	e smallint, f smallint,
	// 	g int, h int,
	// 	i float, j float,
	// 	k double, l double,
	// 	m timestamp, n timestamp,
	// 	o bigint, p bigint,
	// 	q mediumint, r mediumint,
	// 	s date, t date,
	// 	u time, v time,
	// 	w datetime, x datetime,
	// 	y year, z year,
	// 	aa varchar(10), ab varchar(10),
	// 	ac varbinary(10), ad varbinary(10),
	// 	ae bit(10), af bit(10),
	// 	ag json, ah json,
	// 	ai decimal(10,2), aj decimal(10,2),
	// 	ak enum('a','b','c'), al enum('a','b','c'),
	// 	am set('a','b','c'), an set('a','b','c'),
	// 	ao tinytext, ap tinytext,
	// 	aq tinyblob, ar tinyblob,
	// 	as1 mediumtext, at mediumtext,
	// 	au mediumblob, av mediumblob,
	// 	aw longtext, ax longtext,
	// 	ay longblob, az longblob,
	// 	ba text, bb text,
	// 	bc blob, bd blob,
	// 	be char(10), bf char(10),
	// 	bg binary(10), bh binary(10))
	// 	`)
	// dmlEvent := helper.DML2Event("test", "t", `insert into test.t(
	// 	a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai,ak,am,ao,aq,as1,au,aw,ay,ba,bc,be,bg) values (
	// 		1, true, -1, 123, 153.123,153.123,
	// 		"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59",
	// 		"2015-12-20 23:58:58",1970,"测试",0x0102030405060708090A,81,
	// 		'{"key1": "value1"}', 129012.12, 'a', 'b', "5rWL6K+VdGV4dA==",
	// 		0x89504E470D0A1A0A,"5rWL6K+VdGV4dA==",0x4944330300000000,
	// 		"5rWL6K+VdGV4dA==",0x504B0304140000000800,"5rWL6K+VdGV4dA==",
	// 		0x255044462D312E34,"Alice",0x0102030405060708090A)`)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(
		a,c,e,g,i,k,m,o,q,s,u,w,y) values (
			1, true, -1, 123, 153.123,153.123,
			"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59",
			"2015-12-20 23:58:58",1970)`)
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

	// 解析 JSON 数据
	err = json.Unmarshal(value, &message)

	require.Equal(t, int64(0), message.ID)
	require.Equal(t, "test", message.Schema)
	require.Equal(t, "t", message.Table)
	require.Equal(t, []string{"a"}, message.PKNames)
	require.Equal(t, false, message.IsDDL)
	require.Equal(t, "INSERT", message.EventType)
	require.Equal(t, "", message.Query)

	sqlValue, err := json.Marshal(message.SQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":-6,"b":-6,"c":-6,"d":-6,"e":5,"f":5,"g":4,"h":4,"i":7,"j":7,"k":8,"l":8,"m":93,"n":93,"o":-5,"p":-5,"q":4,"r":4,"s":91,"t":91,"u":92,"v":92,"w":93,"x":93,"y":12,"z":12}`, string(sqlValue))

	mysqlValue, err := json.Marshal(message.MySQLType)
	require.NoError(t, err)
	require.Equal(t, `{"a":"tinyint","b":"tinyint","l":"double","o":"bigint","r":"mediumint","u":"time","z":"year","e":"smallint","h":"int","i":"float","c":"tinyint","f":"smallint","n":"timestamp","p":"bigint","q":"mediumint","s":"date","w":"datetime","x":"datetime","y":"year","d":"tinyint","g":"int","j":"float","k":"double","m":"timestamp","t":"date","v":"time"}`, string(mysqlValue))

	oldValue, err := json.Marshal(message.Old)
	require.NoError(t, err)
	require.Equal(t, "null", string(oldValue))

	newValue, err := json.Marshal(message.Data)
	require.NoError(t, err)
	require.Equal(t, `[{"a":"1","b":null,"c":"1","d":null,"e":"-1","f":null,"g":"123","h":null,"i":"153.123","j":null,"k":"153.123","l":null,"m":"1973-12-30 15:30:00","n":null,"o":"123","p":null,"q":"123","r":null,"s":"2000-01-01","t":null,"u":"23:59:59","v":null,"w":"2015-12-20 23:58:58","x":null,"y":"1970","z":null}]`, string(newValue))
}

/*
func TestGetMySQLType4IntTypes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1 (
    	a int primary key,
    	b tinyint,
    	c smallint,
    	d mediumint,
    	e bigint)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok := tableInfo.GetColumnInfo(tableInfo.Columns[0].ID)
	require.True(t, ok)

	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int", mysqlType)
	// mysql type with the default type length
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(11)", mysqlType)

	flag := *tableInfo.ColumnsFlag[tableInfo.Columns[0].ID]
	javaType, err := getJavaSQLType(int64(2147483647), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(4)", mysqlType)

	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[1].ID]
	javaType, err = getJavaSQLType(int64(127), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeTINYINT, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(6)", mysqlType)

	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[2].ID]
	javaType, err = getJavaSQLType(int64(32767), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[3].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[3].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(9)", mysqlType)
	javaType, err = getJavaSQLType(int64(8388607), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[4].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[4].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bigint(20)", mysqlType)
	javaType, err = getJavaSQLType(int64(9223372036854775807), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)

	sql = `create table test.t2 (
    	a int unsigned primary key,
    	b tinyint unsigned,
    	c smallint unsigned,
    	d mediumint unsigned,
    	e bigint unsigned)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[0].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[0].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int unsigned", mysqlType)
	// mysql type with the default type length
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(10) unsigned", mysqlType)

	javaType, err = getJavaSQLType(uint64(2147483647), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType(uint64(2147483648), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[1].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(3) unsigned", mysqlType)

	javaType, err = getJavaSQLType(uint64(127), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeTINYINT, javaType)
	javaType, err = getJavaSQLType(uint64(128), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeTINYINT, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeTINYINT, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(5) unsigned", mysqlType)
	javaType, err = getJavaSQLType(uint64(32767), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)
	javaType, err = getJavaSQLType(uint64(32768), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[3].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[3].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(8) unsigned", mysqlType)
	javaType, err = getJavaSQLType(uint64(8388607), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType(uint64(8388608), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[4].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[4].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bigint(20) unsigned", mysqlType)
	javaType, err = getJavaSQLType(uint64(9223372036854775807), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)
	javaType, err = getJavaSQLType(uint64(9223372036854775808), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDECIMAL, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)

	sql = `create table test.t3 (
    	a int(10) primary key,
    	b tinyint(3) ,
    	c smallint(5),
    	d mediumint(8),
    	e bigint(19))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[0].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(10)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(3)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(5)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(8)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[4].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bigint(19)", mysqlType)

	sql = `create table test.t4 (
    	a int(10) unsigned primary key,
    	b tinyint(3) unsigned,
    	c smallint(5) unsigned,
    	d mediumint(8) unsigned,
    	e bigint(19) unsigned)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[0].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(10) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(3) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(5) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(8) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[4].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bigint(19) unsigned", mysqlType)

	sql = `create table test.t5 (
    	a int zerofill primary key,
    	b tinyint zerofill,
    	c smallint unsigned zerofill,
    	d mediumint zerofill,
    	e bigint zerofill)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[0].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(10) unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(3) unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(5) unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(8) unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[4].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bigint(20) unsigned zerofill", mysqlType)

	sql = `create table test.t6(
		a int primary key,
		b bit,
		c bit(3),
		d bool)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bit", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bit(1)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bit", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bit(3)", mysqlType)
	javaType, err = getJavaSQLType(uint64(65), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIT, javaType)

	// bool is identical to tinyint in the TiDB.
	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(1)", mysqlType)
}

func TestGetMySQLType4FloatType(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(
		a int primary key,
		b float,
		c double)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID := tableInfo.Columns[1].ID
	columnInfo, ok := tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[columnID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float", mysqlType)
	javaType, err := getJavaSQLType(3.14, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeREAL, javaType)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "double", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "double", mysqlType)
	javaType, err = getJavaSQLType(2.71, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDOUBLE, javaType)

	sql = `create table test.t2(a int primary key, b float(10, 3), c float(10))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID = tableInfo.Columns[1].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float(10,3)", mysqlType)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float", mysqlType)

	sql = `create table test.t3(a int primary key, b double(20, 3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID = tableInfo.Columns[1].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "double", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "double(20,3)", mysqlType)

	sql = `create table test.t4(
    	a int primary key,
    	b float unsigned,
    	c double unsigned,
    	d float zerofill,
    	e double zerofill)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID = tableInfo.Columns[1].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float unsigned", mysqlType)
	javaType, err = getJavaSQLType(3.14, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeREAL, javaType)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "double unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "double unsigned", mysqlType)
	javaType, err = getJavaSQLType(2.71, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDOUBLE, javaType)

	columnID = tableInfo.Columns[3].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float unsigned zerofill", mysqlType)

	columnID = tableInfo.Columns[4].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "double unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "double unsigned zerofill", mysqlType)
}

func TestGetMySQLType4Decimal(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b decimal, c numeric)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok := tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(10,0)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(10,0)", mysqlType)

	sql = `create table test.t2(a int primary key, b decimal(5), c decimal(5, 2))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(5,0)", mysqlType)

	columnID := tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(5,2)", mysqlType)
	javaType, err := getJavaSQLType("2333", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDECIMAL, javaType)

	sql = `create table test.t3(a int primary key, b decimal unsigned, c decimal zerofill)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(10,0) unsigned", mysqlType)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(10,0) unsigned zerofill", mysqlType)
	javaType, err = getJavaSQLType("2333", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDECIMAL, javaType)
}

func TestGetMySQLType4TimeTypes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b time, c time(3))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok := tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "time", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "time", mysqlType)

	columnID := tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "time", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "time(3)", mysqlType)
	javaType, err := getJavaSQLType("02:20:20", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeTIME)

	sql = `create table test.t2(a int primary key, b datetime, c datetime(3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "datetime", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "datetime", mysqlType)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "datetime", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "datetime(3)", mysqlType)
	javaType, err = getJavaSQLType("2020-02-20 02:20:20", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeTIMESTAMP)

	sql = `create table test.t3(a int primary key, b timestamp, c timestamp(3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "timestamp", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "timestamp", mysqlType)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "timestamp", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "timestamp(3)", mysqlType)
	javaType, err = getJavaSQLType("2020-02-20 02:20:20", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeTIMESTAMP)

	sql = `create table test.t4(a int primary key, b date)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID = tableInfo.Columns[1].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "date", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "date", mysqlType)
	javaType, err = getJavaSQLType("2020-02-20", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeDATE)

	sql = `create table test.t5(a int primary key, b year, c year(4))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "year", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "year(4)", mysqlType)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "year", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "year(4)", mysqlType)
	javaType, err = getJavaSQLType("2020", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeVARCHAR)
}

func TestGetMySQLType4Char(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b char, c char(123))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok := tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "char", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "char(1)", mysqlType)

	columnID := tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "char", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "char(123)", mysqlType)
	javaType, err := getJavaSQLType([]uint8("测试char"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCHAR)

	sql = `create table test.t1(a int primary key, b varchar(123))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID = tableInfo.Columns[1].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "varchar", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "varchar(123)", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试varchar"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeVARCHAR)
}

func TestGetMySQLType4TextTypes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b text, c tinytext, d mediumtext, e longtext)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID := tableInfo.Columns[1].ID
	columnInfo, ok := tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[columnID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "text", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "text", mysqlType)
	javaType, err := getJavaSQLType([]uint8("测试text"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCLOB)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinytext", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinytext", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试tinytext"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCLOB)

	columnID = tableInfo.Columns[3].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumtext", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumtext", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试mediumtext"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCLOB)

	columnID = tableInfo.Columns[4].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "longtext", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "longtext", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试longtext"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCLOB)
}

func TestGetMySQLType4BinaryType(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b binary, c binary(10))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok := tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[tableInfo.Columns[1].ID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "binary", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "binary(1)", mysqlType)
	javaType, err := getJavaSQLType([]uint8("测试binary"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "binary", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "binary(10)", mysqlType)

	sql = `create table test.t2(a int primary key, b varbinary(23))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[1].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "varbinary", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "varbinary(23)", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试varbinary"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBLOB, javaType)
}

func TestGetMySQLType4BlobType(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b blob, c tinyblob, d mediumblob, e longblob)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnInfo, ok := tableInfo.GetColumnInfo(tableInfo.Columns[1].ID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[tableInfo.Columns[1].ID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "blob", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "blob", mysqlType)
	javaType, err := getJavaSQLType([]uint8("测试blob"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[2].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyblob", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyblob", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试tinyblob"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[3].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[3].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumblob", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumblob", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试mediumblob"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(tableInfo.Columns[4].ID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[tableInfo.Columns[4].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "longblob", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "longblob", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试longblob"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)
}

func TestGetMySQLType4EnumAndSet(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b enum('a', 'b', 'c'), c set('a', 'b', 'c'))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID := tableInfo.Columns[1].ID
	columnInfo, ok := tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[columnID]

	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "enum", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "enum('a','b','c')", mysqlType)

	javaType, err := getJavaSQLType(uint64(1), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnID = tableInfo.Columns[2].ID
	columnInfo, ok = tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag = *tableInfo.ColumnsFlag[columnID]

	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "set", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "set('a','b','c')", mysqlType)

	javaType, err = getJavaSQLType(uint64(2), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIT, javaType)
}

func TestGetMySQLType4JSON(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b json)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	columnID := tableInfo.Columns[1].ID
	columnInfo, ok := tableInfo.GetColumnInfo(columnID)
	require.True(t, ok)
	flag := *tableInfo.ColumnsFlag[columnID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "json", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "json", mysqlType)

	javaType, err := getJavaSQLType("{\"key1\": \"value1\"}", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeVARCHAR, javaType)

	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeVARCHAR, javaType)
}
*/
