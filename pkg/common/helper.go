package common

import (
	"fmt"
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var EmptyBytes = make([]byte, 0)

// getColumnValue returns the column value in the row
func FormatColVal(row *chunk.Row, col *model.ColumnInfo, idx int) (
	value interface{}, err error,
) {
	var v interface{}
	switch col.GetType() {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		v = row.GetTime(idx).String()
	case mysql.TypeDuration:
		v = row.GetDuration(idx, 0).String()
	case mysql.TypeJSON:
		v = row.GetJSON(idx).String()
	case mysql.TypeNewDecimal:
		d := row.GetMyDecimal(idx)
		if d == nil {
			// nil takes 0 byte.
			return nil, nil
		}
		v = d.String()
	case mysql.TypeEnum, mysql.TypeSet:
		v = row.GetEnum(idx).Value
	case mysql.TypeBit:
		d := row.GetDatum(idx, &col.FieldType)
		dp := &d
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		v, err = dp.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return nil, err
		}
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		b := row.GetBytes(idx)
		if b == nil {
			b = EmptyBytes
		}

		v = b
	case mysql.TypeFloat:
		b := row.GetFloat32(idx)
		if math.IsNaN(float64(b)) || math.IsInf(float64(b), 1) || math.IsInf(float64(b), -1) {
			warn := fmt.Sprintf("the value is invalid in column: %f", v)
			log.Warn(warn)
			b = 0
		}
		v = b
	case mysql.TypeDouble:
		b := row.GetFloat64(idx)
		if math.IsNaN(b) || math.IsInf(b, 1) || math.IsInf(b, -1) {
			warn := fmt.Sprintf("the value is invalid in column: %f", v)
			log.Warn(warn)
			b = 0
		}
		v = b
	default:
		d := row.GetDatum(idx, &col.FieldType)
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		v = d.GetValue()
	}

	// If the column value type is []byte and charset is not binary, we get its string
	// representation. Because if we use the byte array respresentation, the go-sql-driver
	// will automatically set `_binary` charset for that column, which is not expected.
	// See https://github.com/go-sql-driver/mysql/blob/ce134bfc/connection.go#L267
	if col.GetCharset() != "" && col.GetCharset() != charset.CharsetBin {
		if b, ok := v.([]byte); ok {
			v = string(b)
		}
	}
	return v, nil
}
