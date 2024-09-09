package writer

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/mounter"
	"github.com/stretchr/testify/require"
)

// This table has 45 columns
var createTableSQL = `create table t (
	id          int primary key auto_increment,
 
	c_tinyint   tinyint   null,
	c_smallint  smallint  null,
	c_mediumint mediumint null,
	c_int       int       null,
	c_bigint    bigint    null,
 
	c_unsigned_tinyint   tinyint   unsigned null,
	c_unsigned_smallint  smallint  unsigned null,
	c_unsigned_mediumint mediumint unsigned null,
	c_unsigned_int       int       unsigned null,
	c_unsigned_bigint    bigint    unsigned null,
 
	c_float   float   null,
	c_double  double  null,
	c_decimal decimal null,
	c_decimal_2 decimal(10, 4) null,
 
	c_unsigned_float     float unsigned   null,
	c_unsigned_double    double unsigned  null,
	c_unsigned_decimal   decimal unsigned null,
	c_unsigned_decimal_2 decimal(10, 4) unsigned null,
 
	c_date      date      null,
	c_datetime  datetime  null,
	c_timestamp timestamp null,
	c_time      time      null,
	c_year      year      null,
 
	c_tinytext   tinytext      null,
	c_text       text          null,
	c_mediumtext mediumtext    null,
	c_longtext   longtext      null,
 
	c_tinyblob   tinyblob      null,
	c_blob       blob          null,
	c_mediumblob mediumblob    null,
	c_longblob   longblob      null,
 
	c_char       char(16)      null,
	c_varchar    varchar(16)   null,
	c_binary     binary(16)    null,
	c_varbinary  varbinary(16) null,
 
	c_enum enum ('a','b','c') null,
	c_set  set ('a','b','c')  null,
	c_bit  bit(64)            null,
	c_json json               null,
 
 -- gbk dmls
	name varchar(128) CHARACTER SET gbk,
	country char(32) CHARACTER SET gbk,
	city varchar(64),
	description text CHARACTER SET gbk,
	image tinyblob
 );`

func TestBuildInsert(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	_ = helper.DDL2Job(createTableSQL)

	insertDataSQL := `insert into t values (
		2,
		1, 2, 3, 4, 5,
		1, 2, 3, 4, 5,
		2020.0202, 2020.0303,
		  2020.0404, 2021.1208,
		3.1415, 2.7182, 8000, 179394.233,
		'2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
		'89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
		x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
		'89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
		'b', 'b,c', b'1000001', '{
   "key1": "value1",
   "key2": "value2",
   "key3": "123"
   }',
		'测试', "中国", "上海", "你好,世界", 0xC4E3BAC3CAC0BDE7
   );`

	event := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, event)
	row, ok := event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)

	exportedArgs := []interface{}{
		// int
		int64(2),
		// tinyint, smallint, mediumint, int, bigint
		int64(1), int64(2), int64(3), int64(4), int64(5),
		// unsigned tinyint, smallint, mediumint, int, bigint
		uint64(1), uint64(2), uint64(3), uint64(4), uint64(5),
		// float, double, decimal, decimal(10, 4)
		float32(2020.0201), float64(2020.0303), "2020", "2021.1208",
		// unsigned float, double, decimal, decimal(10, 4)
		float32(3.1415), float64(2.7182), "8000", "179394.2330",
		// date, datetime, timestamp, time, year
		"2020-02-20", "2020-02-20 02:20:20", "2020-02-20 02:20:20", "02:20:20", int64(2020),
		// tinytext, text, mediumtext, longtext
		"89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A",
		// tinyblob, blob, mediumblob, longblob
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		// char(16), varchar(16), binary(16), varbinary(16)
		"89504E470D0A1A0A", "89504E470D0A1A0A",
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		// enum, set, bit,
		uint64(2), uint64(6), uint64(65),
		// json
		"{\"key1\": \"value1\", \"key2\": \"value2\", \"key3\": \"123\"}",
		// gbk: varchar, char, text, text, tinyblob
		"测试", "中国", "上海", "你好,世界", []uint8{0xc4, 0xe3, 0xba, 0xc3, 0xca, 0xc0, 0xbd, 0xe7}}

	// case 1: Convert to INSERT INTO
	exportedSQL := "INSERT INTO `test`.`t` (`id`,`c_tinyint`,`c_smallint`,`c_mediumint`,`c_int`,`c_bigint`,`c_unsigned_tinyint`,`c_unsigned_smallint`,`c_unsigned_mediumint`,`c_unsigned_int`,`c_unsigned_bigint`,`c_float`,`c_double`,`c_decimal`,`c_decimal_2`,`c_unsigned_float`,`c_unsigned_double`,`c_unsigned_decimal`,`c_unsigned_decimal_2`,`c_date`,`c_datetime`,`c_timestamp`,`c_time`,`c_year`,`c_tinytext`,`c_text`,`c_mediumtext`,`c_longtext`,`c_tinyblob`,`c_blob`,`c_mediumblob`,`c_longblob`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_enum`,`c_set`,`c_bit`,`c_json`,`name`,`country`,`city`,`description`,`image`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	sql, args := buildInsert(event.TableInfo, row, true, false)
	require.Equal(t, exportedSQL, sql)
	require.Len(t, args, 45)
	require.Equal(t, exportedArgs, args)

	// case 2: Convert to REPLACE INTO
	exportedSQL = "REPLACE INTO `test`.`t` (`id`,`c_tinyint`,`c_smallint`,`c_mediumint`,`c_int`,`c_bigint`,`c_unsigned_tinyint`,`c_unsigned_smallint`,`c_unsigned_mediumint`,`c_unsigned_int`,`c_unsigned_bigint`,`c_float`,`c_double`,`c_decimal`,`c_decimal_2`,`c_unsigned_float`,`c_unsigned_double`,`c_unsigned_decimal`,`c_unsigned_decimal_2`,`c_date`,`c_datetime`,`c_timestamp`,`c_time`,`c_year`,`c_tinytext`,`c_text`,`c_mediumtext`,`c_longtext`,`c_tinyblob`,`c_blob`,`c_mediumblob`,`c_longblob`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_enum`,`c_set`,`c_bit`,`c_json`,`name`,`country`,`city`,`description`,`image`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	sql, args = buildInsert(event.TableInfo, row, true, true)
	require.Equal(t, exportedSQL, sql)
	require.Len(t, args, 45)
	require.Equal(t, exportedArgs, args)
}

func TestBuildDelete(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	// case 1: delete data from table that has in column primary key
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL := "insert into t values (1, 'test');"
	event := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, event)
	row, ok := event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	// Manually change row type to delete and set PreRow
	// We do this because the helper does not support delete operation
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL := "DELETE FROM `test`.`t` WHERE `id` = ? LIMIT 1"
	expectedArgs := []interface{}{int64(1)}

	sql, args := buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 1)
	require.Equal(t, expectedArgs, args)

	// case 2: delete data from table that has explicit primary key
	createTableSQL = "create table t2 (id int, name varchar(32), primary key (id));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)
	insertDataSQL = "insert into t2 values (1, 'test');"
	event = helper.DML2Event("test", "t2", insertDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL = "DELETE FROM `test`.`t2` WHERE `id` = ? LIMIT 1"
	expectedArgs = []interface{}{int64(1)}

	sql, args = buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 1)
	require.Equal(t, expectedArgs, args)

	// case 3: delete data from table that has composite primary key
	createTableSQL = "create table t3 (id int, name varchar(32), primary key (id, name));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL = "insert into t3 values (1, 'test');"
	event = helper.DML2Event("test", "t3", insertDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL = "DELETE FROM `test`.`t3` WHERE `id` = ? AND `name` = ? LIMIT 1"
	expectedArgs = []interface{}{int64(1), "test"}

	sql, args = buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 2)
	require.Equal(t, expectedArgs, args)

	// case 4: delete data from table that has composite not null uk
	createTableSQL = "create table t4 (id int, name varchar(32) not null, age int not null, unique key (age, name));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL = "insert into t4 values (1, 'test', 20);"
	event = helper.DML2Event("test", "t4", insertDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL = "DELETE FROM `test`.`t4` WHERE `name` = ? AND `age` = ? LIMIT 1"
	expectedArgs = []interface{}{"test", int64(20)}

	sql, args = buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 2)
	require.Equal(t, expectedArgs, args)

	// case 5: delete data from table that has composite nullable uk
	createTableSQL = "create table t5 (id int, name varchar(32), age int, unique key (age, name));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL = "insert into t5 values (1, 'test', 20);"
	event = helper.DML2Event("test", "t5", insertDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL = "DELETE FROM `test`.`t5` WHERE `id` = ? AND `name` = ? AND `age` = ? LIMIT 1"
	expectedArgs = []interface{}{int64(1), "test", int64(20)}

	sql, args = buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 3)
	require.Equal(t, expectedArgs, args)
}

func TestBuildUpdate(t *testing.T) {
	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	// case 1: table has primary key
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL := "insert into t values (1, 'test');"
	event := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, event)
	oldRow, ok := event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, oldRow)

	updateDataSQL := "update t set name = 'test2' where id = 1;"
	event = helper.DML2Event("test", "t", updateDataSQL)
	require.NotNil(t, event)
	row, ok := event.GetNextRow()
	require.True(t, ok)
	// Manually change row type to update and set PreRow
	row.PreRow = oldRow.Row
	row.RowType = common.RowTypeUpdate

	expectedSQL := "UPDATE `test`.`t` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1"
	expectedArgs := []interface{}{int64(1), "test2", int64(1)}
	sql, args := buildUpdate(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 3)
	require.Equal(t, expectedArgs, args)

	// case 2: table has not null uk
	createTableSQL = "create table t2 (id int, name varchar(32) not null, age int not null, unique key (age, name));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL = "insert into t2 values (1, 'test', 20);"
	event = helper.DML2Event("test", "t2", insertDataSQL)
	require.NotNil(t, event)
	oldRow, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, oldRow)

	updateDataSQL = "update t2 set name = 'test2' where id = 1;"
	event = helper.DML2Event("test", "t2", updateDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	// Manually change row type to update and set PreRow
	row.PreRow = oldRow.Row
	row.RowType = common.RowTypeUpdate

	expectedSQL = "UPDATE `test`.`t2` SET `id` = ?, `name` = ?, `age` = ? WHERE `name` = ? AND `age` = ? LIMIT 1"
	expectedArgs = []interface{}{int64(1), "test2", int64(20), "test", int64(20)}
	sql, args = buildUpdate(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 5)
	require.Equal(t, expectedArgs, args)

}
