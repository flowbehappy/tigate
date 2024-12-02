package event

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

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

var insertDataSQL = `insert into t values (
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

// We use OriginDefaultValue instead of DefaultValue in the ut, pls ref to
// https://github.com/pingcap/tiflow/issues/4048
// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
func TestGetDefaultZeroValue(t *testing.T) {
	// Check following MySQL type, ref to:
	// https://github.com/pingcap/tidb/blob/master/parser/mysql/type.go

	// mysql flag null
	ftNull := types.NewFieldType(mysql.TypeUnspecified)

	// mysql.TypeTiny + notnull
	ftTinyIntNotNull := types.NewFieldType(mysql.TypeTiny)
	ftTinyIntNotNull.AddFlag(mysql.NotNullFlag)

	// mysql.TypeTiny + notnull +  unsigned
	ftTinyIntNotNullUnSigned := types.NewFieldType(mysql.TypeTiny)
	ftTinyIntNotNullUnSigned.SetFlag(mysql.NotNullFlag)
	ftTinyIntNotNullUnSigned.AddFlag(mysql.UnsignedFlag)

	// mysql.TypeTiny + null
	ftTinyIntNull := types.NewFieldType(mysql.TypeTiny)

	// mysql.TypeShort + notnull
	ftShortNotNull := types.NewFieldType(mysql.TypeShort)
	ftShortNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLong + notnull
	ftLongNotNull := types.NewFieldType(mysql.TypeLong)
	ftLongNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLonglong + notnull
	ftLongLongNotNull := types.NewFieldType(mysql.TypeLonglong)
	ftLongLongNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeInt24 + notnull
	ftInt24NotNull := types.NewFieldType(mysql.TypeInt24)
	ftInt24NotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeFloat + notnull
	ftTypeFloatNotNull := types.NewFieldType(mysql.TypeFloat)
	ftTypeFloatNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeFloat + notnull + unsigned
	ftTypeFloatNotNullUnSigned := types.NewFieldType(mysql.TypeFloat)
	ftTypeFloatNotNullUnSigned.SetFlag(mysql.NotNullFlag | mysql.UnsignedFlag)

	// mysql.TypeFloat + null
	ftTypeFloatNull := types.NewFieldType(mysql.TypeFloat)

	// mysql.TypeDouble + notnull
	ftTypeDoubleNotNull := types.NewFieldType(mysql.TypeDouble)
	ftTypeDoubleNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeNewDecimal + notnull
	ftTypeNewDecimalNull := types.NewFieldType(mysql.TypeNewDecimal)
	ftTypeNewDecimalNull.SetFlen(5)
	ftTypeNewDecimalNull.SetDecimal(2)

	// mysql.TypeNewDecimal + notnull
	ftTypeNewDecimalNotNull := types.NewFieldType(mysql.TypeNewDecimal)
	ftTypeNewDecimalNotNull.SetFlag(mysql.NotNullFlag)
	ftTypeNewDecimalNotNull.SetFlen(5)
	ftTypeNewDecimalNotNull.SetDecimal(2)

	// mysql.TypeNull
	ftTypeNull := types.NewFieldType(mysql.TypeNull)

	// mysql.TypeTimestamp + notnull
	ftTypeTimestampNotNull := types.NewFieldType(mysql.TypeTimestamp)
	ftTypeTimestampNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeTimestamp + notnull
	ftTypeTimestampNull := types.NewFieldType(mysql.TypeTimestamp)

	// mysql.TypeDate + notnull
	ftTypeDateNotNull := types.NewFieldType(mysql.TypeDate)
	ftTypeDateNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeDuration + notnull
	ftTypeDurationNotNull := types.NewFieldType(mysql.TypeDuration)
	ftTypeDurationNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeDatetime + notnull
	ftTypeDatetimeNotNull := types.NewFieldType(mysql.TypeDatetime)
	ftTypeDatetimeNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeYear + notnull
	ftTypeYearNotNull := types.NewFieldType(mysql.TypeYear)
	ftTypeYearNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeNewDate + notnull
	ftTypeNewDateNotNull := types.NewFieldType(mysql.TypeNewDate)
	ftTypeNewDateNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeVarchar + notnull
	ftTypeVarcharNotNull := types.NewFieldType(mysql.TypeVarchar)
	ftTypeVarcharNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeTinyBlob + notnull
	ftTypeTinyBlobNotNull := types.NewFieldType(mysql.TypeTinyBlob)
	ftTypeTinyBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeMediumBlob + notnull
	ftTypeMediumBlobNotNull := types.NewFieldType(mysql.TypeMediumBlob)
	ftTypeMediumBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLongBlob + notnull
	ftTypeLongBlobNotNull := types.NewFieldType(mysql.TypeLongBlob)
	ftTypeLongBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeBlob + notnull
	ftTypeBlobNotNull := types.NewFieldType(mysql.TypeBlob)
	ftTypeBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeVarString + notnull
	ftTypeVarStringNotNull := types.NewFieldType(mysql.TypeVarString)
	ftTypeVarStringNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeString + notnull
	ftTypeStringNotNull := types.NewFieldType(mysql.TypeString)
	ftTypeStringNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeBit + notnull
	ftTypeBitNotNull := types.NewFieldType(mysql.TypeBit)
	ftTypeBitNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeJSON + notnull
	ftTypeJSONNotNull := types.NewFieldType(mysql.TypeJSON)
	ftTypeJSONNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeEnum + notnull + nodefault
	ftTypeEnumNotNull := types.NewFieldType(mysql.TypeEnum)
	ftTypeEnumNotNull.SetFlag(mysql.NotNullFlag)
	ftTypeEnumNotNull.SetElems([]string{"e0", "e1"})

	// mysql.TypeEnum + null
	ftTypeEnumNull := types.NewFieldType(mysql.TypeEnum)

	// mysql.TypeSet + notnull
	ftTypeSetNotNull := types.NewFieldType(mysql.TypeSet)
	ftTypeSetNotNull.SetFlag(mysql.NotNullFlag)
	ftTypeSetNotNull.SetElems([]string{"1", "e"})

	// mysql.TypeGeometry + notnull
	ftTypeGeometryNotNull := types.NewFieldType(mysql.TypeGeometry)
	ftTypeGeometryNotNull.SetFlag(mysql.NotNullFlag)

	testCases := []struct {
		Name    string
		ColInfo timodel.ColumnInfo
		Res     interface{}
	}{
		{
			Name:    "mysql flag null",
			ColInfo: timodel.ColumnInfo{FieldType: *ftNull},
			Res:     nil,
		},
		{
			Name:    "mysql.TypeTiny + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNotNull.Clone()},
			Res:     int64(0),
		},
		{
			Name: "mysql.TypeTiny + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "-128",
				FieldType:          *ftTinyIntNotNull,
			},
			Res: int64(-128),
		},
		{
			Name:    "mysql.TypeTiny + notnull + default + unsigned",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNotNullUnSigned},
			Res:     uint64(0),
		},
		{
			Name:    "mysql.TypeTiny + notnull + unsigned",
			ColInfo: timodel.ColumnInfo{OriginDefaultValue: "127", FieldType: *ftTinyIntNotNullUnSigned},
			Res:     uint64(127),
		},
		{
			Name: "mysql.TypeTiny + null + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "-128",
				FieldType:          *ftTinyIntNull,
			},
			Res: int64(-128),
		},
		{
			Name:    "mysql.TypeTiny + null + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNull},
			Res:     nil,
		},
		{
			Name:    "mysql.TypeShort, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftShortNotNull},
			Res:     int64(0),
		},
		{
			Name:    "mysql.TypeLong, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftLongNotNull},
			Res:     int64(0),
		},
		{
			Name:    "mysql.TypeLonglong, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftLongLongNotNull},
			Res:     int64(0),
		},
		{
			Name:    "mysql.TypeInt24, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftInt24NotNull},
			Res:     int64(0),
		},
		{
			Name:    "mysql.TypeFloat + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeFloatNotNull},
			Res:     float32(0),
		},
		{
			Name: "mysql.TypeFloat + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: float32(-3.1415),
				FieldType:          *ftTypeFloatNotNull,
			},
			Res: float32(-3.1415),
		},
		{
			Name: "mysql.TypeFloat + notnull + default + unsigned",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: float32(3.1415),
				FieldType:          *ftTypeFloatNotNullUnSigned,
			},
			Res: float32(3.1415),
		},
		{
			Name: "mysql.TypeFloat + notnull + unsigned",
			ColInfo: timodel.ColumnInfo{
				FieldType: *ftTypeFloatNotNullUnSigned,
			},
			Res: float32(0),
		},
		{
			Name: "mysql.TypeFloat + null + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: float32(-3.1415),
				FieldType:          *ftTypeFloatNull,
			},
			Res: float32(-3.1415),
		},
		{
			Name: "mysql.TypeFloat + null + nodefault",
			ColInfo: timodel.ColumnInfo{
				FieldType: *ftTypeFloatNull,
			},
			Res: nil,
		},
		{
			Name:    "mysql.TypeDouble, other testCases same as float",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDoubleNotNull},
			Res:     float64(0),
		},
		{
			Name:    "mysql.TypeNewDecimal + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDecimalNotNull},
			Res:     "0", // related with Flen and Decimal
		},
		{
			Name:    "mysql.TypeNewDecimal + null + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDecimalNull},
			Res:     nil,
		},
		{
			Name:    "mysql.TypeNull",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNull},
			Res:     nil,
		},
		{
			Name:    "mysql.TypeTimestamp + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeTimestampNotNull},
			Res:     "0000-00-00 00:00:00",
		},
		{
			Name:    "mysql.TypeDate, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDateNotNull},
			Res:     "0000-00-00",
		},
		{
			Name:    "mysql.TypeDuration, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDurationNotNull},
			Res:     "00:00:00",
		},
		{
			Name:    "mysql.TypeDatetime, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDatetimeNotNull},
			Res:     "0000-00-00 00:00:00",
		},
		{
			Name:    "mysql.TypeYear + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeYearNotNull},
			Res:     int64(0),
		},
		{
			Name: "mysql.TypeYear + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "2021",
				FieldType:          *ftTypeYearNotNull,
			},
			Res: int64(2021),
		},
		{
			Name:    "mysql.TypeNewDate",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDateNotNull},
			Res:     nil, // [TODO] seems not support by TiDB, need check
		},
		{
			Name:    "mysql.TypeVarchar + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeVarcharNotNull},
			Res:     []byte{},
		},
		{
			Name: "mysql.TypeVarchar + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "e0",
				FieldType:          *ftTypeVarcharNotNull,
			},
			Res: []byte("e0"),
		},
		{
			Name:    "mysql.TypeTinyBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeTinyBlobNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeMediumBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeMediumBlobNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeLongBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeLongBlobNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeBlobNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeVarString",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeVarStringNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeString",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeStringNotNull},
			Res:     []byte{},
		},
		{
			Name:    "mysql.TypeBit",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeBitNotNull},
			Res:     uint64(0),
		},
		// BLOB, TEXT, GEOMETRY or JSON column can't have a default value
		{
			Name:    "mysql.TypeJSON",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeJSONNotNull},
			Res:     "null",
		},
		{
			Name:    "mysql.TypeEnum + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeEnumNotNull},
			// TypeEnum value will be a string and then translate to []byte
			// NotNull && no default will choose first element
			Res: uint64(1),
		},
		{
			Name: "mysql.TypeEnum + null",
			ColInfo: timodel.ColumnInfo{
				FieldType: *ftTypeEnumNull,
			},
			Res: nil,
		},
		{
			Name:    "mysql.TypeSet + notnull",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeSetNotNull},
			Res:     uint64(0),
		},
		{
			Name:    "mysql.TypeGeometry",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeGeometryNotNull},
			Res:     nil, // not support yet
		},
	}

	tz := time.Local
	for _, tc := range testCases {
		_, val, _, _, _ := getDefaultOrZeroValue(&tc.ColInfo, tz)
		require.Equal(t, tc.Res, val, tc.Name)
	}

	colInfo := timodel.ColumnInfo{
		OriginDefaultValue: "-3.14", // no float
		FieldType:          *ftTypeNewDecimalNotNull,
	}
	_, val, _, _, _ := getDefaultOrZeroValue(&colInfo, tz)
	decimal := new(types.MyDecimal)
	err := decimal.FromString([]byte("-3.14"))
	require.NoError(t, err)
	require.Equal(t, decimal.String(), val, "mysql.TypeNewDecimal + notnull + default")

	colInfo = timodel.ColumnInfo{
		OriginDefaultValue: "2020-11-19 12:12:12",
		FieldType:          *ftTypeTimestampNotNull,
	}
	_, val, _, _, _ = getDefaultOrZeroValue(&colInfo, tz)
	expected, err := types.ParseTimeFromFloatString(
		types.DefaultStmtNoWarningContext,
		"2020-11-19 20:12:12", colInfo.FieldType.GetType(), colInfo.FieldType.GetDecimal())
	require.NoError(t, err)
	require.Equal(t, expected.String(), val, "mysql.TypeTimestamp + notnull + default")

	colInfo = timodel.ColumnInfo{
		OriginDefaultValue: "2020-11-19 12:12:12",
		FieldType:          *ftTypeTimestampNull,
	}
	_, val, _, _, _ = getDefaultOrZeroValue(&colInfo, tz)
	expected, err = types.ParseTimeFromFloatString(
		types.DefaultStmtNoWarningContext,
		"2020-11-19 20:12:12", colInfo.FieldType.GetType(), colInfo.FieldType.GetDecimal())
	require.NoError(t, err)
	require.Equal(t, expected.String(), val, "mysql.TypeTimestamp + null + default")

	colInfo = timodel.ColumnInfo{
		OriginDefaultValue: "e1",
		FieldType:          *ftTypeEnumNotNull,
	}
	_, val, _, _, _ = getDefaultOrZeroValue(&colInfo, tz)
	expectedEnum, err := types.ParseEnumName(colInfo.FieldType.GetElems(), "e1", colInfo.FieldType.GetCollate())
	require.NoError(t, err)
	require.Equal(t, expectedEnum.Value, val, "mysql.TypeEnum + notnull + default")

	colInfo = timodel.ColumnInfo{
		OriginDefaultValue: "1,e",
		FieldType:          *ftTypeSetNotNull,
	}
	_, val, _, _, _ = getDefaultOrZeroValue(&colInfo, tz)
	expectedSet, err := types.ParseSetName(colInfo.FieldType.GetElems(), "1,e", colInfo.FieldType.GetCollate())
	require.NoError(t, err)
	require.Equal(t, expectedSet.Value, val, "mysql.TypeSet + notnull + default")
}

func TestTimezoneDefaultValue(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	_ = helper.DDL2Job(`create table test.t(a int primary key, b timestamp default '2023-02-09 13:00:00')`)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a) values (1)`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	require.Equal(t, RowTypeInsert, row.RowType)
	require.Equal(t, int64(1), row.Row.GetInt64(0))
	require.Equal(t, "2023-02-09 13:00:00", row.Row.GetTime(1).String())
}

func TestAllTypes(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	_ = helper.DDL2Job(createTableSQL)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	require.Equal(t, RowTypeInsert, row.RowType)
	require.Equal(t, int64(2), row.Row.GetInt64(0))
	require.Equal(t, int64(1), row.Row.GetInt64(1))

	require.Equal(t, int64(2), row.Row.GetInt64(2))
	require.Equal(t, int64(3), row.Row.GetInt64(3))
	require.Equal(t, int64(4), row.Row.GetInt64(4))
	require.Equal(t, int64(5), row.Row.GetInt64(5))

	require.Equal(t, uint64(1), row.Row.GetUint64(6))
	require.Equal(t, uint64(2), row.Row.GetUint64(7))
	require.Equal(t, uint64(3), row.Row.GetUint64(8))
	require.Equal(t, uint64(4), row.Row.GetUint64(9))
	require.Equal(t, uint64(5), row.Row.GetUint64(10))

	require.Equal(t, float32(2020.0202), row.Row.GetFloat32(11))
	require.Equal(t, float64(2020.0303), row.Row.GetFloat64(12))
	// This column's definition is decimal, so the value after the decimal point will be truncated
	// 2020.0404 -> 2020， ref: https://docs.pingcap.com/tidb/dev/data-type-numeric#fixed-point-types
	require.Equal(t, "2020", row.Row.GetMyDecimal(13).String())
	require.Equal(t, "2021.1208", row.Row.GetMyDecimal(14).String())
	// unsigned float,double,decimal,decimal(10,4)
	require.Equal(t, float32(3.1415), row.Row.GetFloat32(15))
	require.Equal(t, float64(2.7182), row.Row.GetFloat64(16))
	require.Equal(t, "8000", row.Row.GetMyDecimal(17).String())
	require.Equal(t, "179394.2330", row.Row.GetMyDecimal(18).String())
	// date, datetime, timestamp, time, year
	require.Equal(t, "2020-02-20", row.Row.GetTime(19).String())
	require.Equal(t, "2020-02-20 02:20:20", row.Row.GetTime(20).String())
	require.Equal(t, "2020-02-20 02:20:20", row.Row.GetTime(21).String())
	require.Equal(t, "02:20:20", row.Row.GetDuration(22, 0).String())
	require.Equal(t, int64(2020), row.Row.GetInt64(23))
	// tinytext, text, mediumtext, longtext
	require.Equal(t, "89504E470D0A1A0A", row.Row.GetString(24))
	require.Equal(t, "89504E470D0A1A0A", row.Row.GetString(25))
	require.Equal(t, "89504E470D0A1A0A", row.Row.GetString(26))
	require.Equal(t, "89504E470D0A1A0A", row.Row.GetString(27))
	// tinyblob, blob, mediumblob, longblob
	// Convert "89504E470D0A1A0A" to binary data then compare
	binaryFormat := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
	require.Equal(t, binaryFormat, row.Row.GetBytes(28))
	require.Equal(t, binaryFormat, row.Row.GetBytes(29))
	require.Equal(t, binaryFormat, row.Row.GetBytes(30))
	require.Equal(t, binaryFormat, row.Row.GetBytes(31))
	// char, varchar, binary(16), varbinary(16)
	require.Equal(t, "89504E470D0A1A0A", row.Row.GetString(32))
	require.Equal(t, "89504E470D0A1A0A", row.Row.GetString(33))
	// The length of binary(16) is 16, so the length of the binary data is 16, and the bytes number in
	// the binary data is 8, so the remaining 8 bytes need to be padded with 0
	// Ref: https://dev.mysql.com/doc/refman/8.4/en/binary-varbinary.html
	binaryFormat = append(binaryFormat, make([]byte, 8)...)
	require.Equal(t, binaryFormat, row.Row.GetBytes(34))
	// varbinary(16) is not fixed length, so the length of the binary data is 8, so we need to truncate padding 0
	binaryFormat = binaryFormat[:8]
	require.Equal(t, binaryFormat, row.Row.GetBytes(35))
	// enum, set, bit, json
	require.Equal(t, "b", row.Row.GetEnum(36).String())
	require.Equal(t, "b,c", row.Row.GetSet(37).String())
	d := row.Row.GetDatum(38, dmlEvent.TableInfo.GetFieldSlice()[38])
	v, err := d.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, 0b1000001, int(v))
	require.Equal(t, `{"key1": "value1", "key2": "value2", "key3": "123"}`, row.Row.GetJSON(39).String())
	// gbk format: varchar, char, text
	require.Equal(t, "测试", row.Row.GetString(40))
	require.Equal(t, "中国", row.Row.GetString(41))
	require.Equal(t, "上海", row.Row.GetString(42))
	require.Equal(t, "你好,世界", row.Row.GetString(43))
	// tinyblob
	// Convert "C4E3BAC3CAC0BDE7" to binary data then compare
	binaryFormat = []byte{0xC4, 0xE3, 0xBA, 0xC3, 0xCA, 0xC0, 0xBD, 0xE7}
	require.Equal(t, binaryFormat, row.Row.GetBytes(44))
}

func TestNullColumn(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	job := helper.DDL2Job(`create table test.t(
	a tinyint primary key, b tinyint, c bool, d bool, e smallint,
	f smallint, g int, h int, i float, j float,
	k double, l double, m timestamp, n timestamp, o bigint,
	p bigint, q mediumint, r mediumint, s date, t date,
	u time, v time, w datetime, x datetime, y year,
	z year, aa varchar(10), ab varchar(10), ac varbinary(10), ad varbinary(10),
	ae bit(10), af bit(10), ag json, ah json, ai decimal(10,2),
	aj decimal(10,2))`)

	require.NotNil(t, job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai) values (1, true, -1, 123, 153.123,153.123,"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59","2015-12-20 23:58:58",1970,"测试","测试",81,'{"key1": "value1"}', 129012.12)`)
	require.NotNil(t, dmlEvent)

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	require.Equal(t, RowTypeInsert, row.RowType)

	tableInfo := helper.GetTableInfo(job)
	require.NotNil(t, tableInfo)

	// column b is the 2th column
	colValue, err := common.FormatColVal(&row.Row, tableInfo.Columns[1], 1)
	require.NoError(t, err)
	require.Equal(t, nil, colValue)
	// column d is the 4th column
	colValue, err = common.FormatColVal(&row.Row, tableInfo.Columns[3], 3)
	require.NoError(t, err)
	require.Equal(t, nil, colValue)
	// column f is the 6th column
	colValue, err = common.FormatColVal(&row.Row, tableInfo.Columns[5], 5)
	require.NoError(t, err)
	require.Equal(t, nil, colValue)
	// column h is the 8th column
	colValue, err = common.FormatColVal(&row.Row, tableInfo.Columns[7], 7)
	require.NoError(t, err)
	require.Equal(t, nil, colValue)
	// column j is the 10th column
	colValue, err = common.FormatColVal(&row.Row, tableInfo.Columns[9], 9)
	require.NoError(t, err)
	require.Equal(t, nil, colValue)
	// column l is the 12th column
	colValue, err = common.FormatColVal(&row.Row, tableInfo.Columns[11], 11)
	require.NoError(t, err)
	require.Equal(t, nil, colValue)
	// column ah is the 34th column
	colValue, err = common.FormatColVal(&row.Row, tableInfo.Columns[33], 33)
	require.NoError(t, err)
	require.Equal(t, nil, colValue)
}

func TestBinary(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	job := helper.DDL2Job(`create table test.t( a binary(10), b binary(10))`)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t (a) values (0x0102030405060708090A)`)
	require.NotNil(t, dmlEvent)

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	require.Equal(t, RowTypeInsert, row.RowType)
	tableInfo := helper.GetTableInfo(job)
	v, err := common.FormatColVal(&row.Row, tableInfo.Columns[0], 0)
	require.NoError(t, err)
	binaryFormat := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A}
	require.Equal(t, binaryFormat, v)
}
