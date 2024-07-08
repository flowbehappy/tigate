package writer

import (
	"testing"

	"github.com/flowbehappy/tigate/common"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestPrepareUpdate(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		quoteTable   string
		preCols      []*common.Column
		cols         []*common.Column
		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			quoteTable:   "`test`.`t1`",
			preCols:      []*common.Column{},
			cols:         []*common.Column{},
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.HandleKeyFlag | common.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.HandleKeyFlag | common.PrimaryKeyFlag,
					Value: 1,
				},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test2"},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? LIMIT 1",
			expectedArgs: []interface{}{1, "test2", 1},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: "test2",
				},
				{
					Name: "c",
					Type: mysql.TypeLong, Flag: common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{2, "test2", 1, "test"},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name: "b", Type: mysql.TypeVarchar,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: []byte("世界"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{2, []byte("世界"), 1, []byte("你好")},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    common.MultipleKeyFlag | common.HandleKeyFlag,
					Charset: charset.CharsetBin,
					Value:   []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 2,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    common.MultipleKeyFlag | common.HandleKeyFlag,
					Charset: charset.CharsetBin,
					Value:   []byte("世界"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{2, []byte("世界"), 1, []byte("你好")},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    common.MultipleKeyFlag | common.HandleKeyFlag,
					Charset: charset.CharsetGBK,
					Value:   "你好",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 2,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    common.MultipleKeyFlag | common.HandleKeyFlag,
					Charset: charset.CharsetGBK,
					Value:   "世界",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{2, "世界", 1, "你好"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareUpdate(tc.quoteTable, tc.preCols, tc.cols)
		require.Equal(t, tc.expectedSQL, query)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestPrepareDelete(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		quoteTable   string
		preCols      []*common.Column
		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			quoteTable:   "`test`.`t1`",
			preCols:      []*common.Column{},
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.HandleKeyFlag | common.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1",
			expectedArgs: []interface{}{1},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{1, "test"},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name: "b", Type: mysql.TypeVarchar,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{1, []byte("你好")},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    common.MultipleKeyFlag | common.HandleKeyFlag,
					Charset: charset.CharsetBin,
					Value:   []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{1, []byte("你好")},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    common.MultipleKeyFlag | common.HandleKeyFlag,
					Charset: charset.CharsetGBK,
					Value:   "你好",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{1, "你好"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareDelete(tc.quoteTable, tc.preCols)
		require.Equal(t, tc.expectedSQL, query)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestWhereSlice(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		cols             []*common.Column
		forceReplicate   bool
		expectedColNames []string
		expectedArgs     []interface{}
	}{
		{
			cols:             []*common.Column{},
			forceReplicate:   false,
			expectedColNames: nil,
			expectedArgs:     nil,
		},
		{
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.HandleKeyFlag | common.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			expectedColNames: []string{"a"},
			expectedArgs:     []interface{}{1},
		},
		{
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name: "b", Type: mysql.TypeVarString,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.HandleKeyFlag | common.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			expectedColNames: []string{"a"},
			expectedArgs:     []interface{}{1},
		},
		{
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  common.MultipleKeyFlag | common.HandleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  common.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
	}
	for _, tc := range testCases {
		colNames, args := whereSlice(tc.cols)
		require.Equal(t, tc.expectedColNames, colNames)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestPrepareReplace(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		quoteTable    string
		cols          []*common.Column
		expectedQuery string
		expectedArgs  []interface{}
	}{
		{
			quoteTable: "`test`.`t1`",
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Value: "varchar",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Value: 1,
					Flag:  common.GeneratedColumnFlag,
				},
				{
					Name:  "d",
					Type:  mysql.TypeTiny,
					Value: uint8(255),
				},
			},
			expectedQuery: "REPLACE INTO `test`.`t1` (`a`,`b`,`d`) VALUES ",
			expectedArgs:  []interface{}{1, "varchar", uint8(255)},
		},
		{
			quoteTable: "`test`.`t1`",
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Value: "varchar",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Value: 1,
				},
				{
					Name:  "d",
					Type:  mysql.TypeTiny,
					Value: uint8(255),
				},
			},
			expectedQuery: "REPLACE INTO `test`.`t1` (`a`,`b`,`c`,`d`) VALUES ",
			expectedArgs:  []interface{}{1, "varchar", 1, uint8(255)},
		},
		{
			quoteTable: "`test`.`t1`",
			cols: []*common.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeVarchar,
					Charset: charset.CharsetGBK,
					Value:   []byte("你好"),
				},
				{
					Name:    "c",
					Type:    mysql.TypeTinyBlob,
					Charset: charset.CharsetUTF8MB4,
					Value:   []byte("世界"),
				},
				{
					Name:    "d",
					Type:    mysql.TypeMediumBlob,
					Charset: charset.CharsetBin,
					Value:   []byte("你好,世界"),
				},
				{
					Name:  "e",
					Type:  mysql.TypeBlob,
					Value: []byte("你好,世界"),
				},
			},
			expectedQuery: "REPLACE INTO `test`.`t1` (`a`,`b`,`c`,`d`,`e`) VALUES ",
			expectedArgs: []interface{}{
				1, "你好", "世界", []byte("你好,世界"),
				[]byte("你好,世界"),
			},
		},
	}
	for _, tc := range testCases {
		// multiple times to verify the stability of column sequence in query string
		for i := 0; i < 10; i++ {
			query, args := prepareReplace(tc.quoteTable, tc.cols, false, false)
			require.Equal(t, tc.expectedQuery, query)
			require.Equal(t, tc.expectedArgs, args)
		}
	}
}
