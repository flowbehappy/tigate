package common

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var EmptyBytes = make([]byte, 0)

// getColumnValue returns the column value in the row
func FormatColVal(row *chunk.Row, col *model.ColumnInfo, idx int) (
	value interface{}, err error,
) {
	if row.IsNull(idx) {
		return nil, nil
	}
	switch col.GetType() {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		return row.GetTime(idx).String(), nil
	case mysql.TypeDuration:
		return row.GetDuration(idx, 0).String(), nil
	case mysql.TypeJSON:
		return row.GetJSON(idx).String(), nil
	case mysql.TypeNewDecimal:
		d := row.GetMyDecimal(idx)
		if d == nil {
			// nil takes 0 byte.
			return nil, nil
		}
		return d.String(), nil
	case mysql.TypeEnum, mysql.TypeSet:
		return row.GetEnum(idx).Value, nil
	case mysql.TypeBit:
		d := row.GetDatum(idx, &col.FieldType)
		dp := &d
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		return dp.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		b := row.GetBytes(idx)
		if b == nil {
			b = EmptyBytes
		}
		// If the column value type is []byte and charset is not binary, we get its string
		// representation. Because if we use the byte array respresentation, the go-sql-driver
		// will automatically set `_binary` charset for that column, which is not expected.
		// See https://github.com/go-sql-driver/mysql/blob/ce134bfc/connection.go#L267
		if col.GetCharset() != "" && col.GetCharset() != charset.CharsetBin {
			if len(b) == 0 {
				return "", nil
			}
			return unsafe.String(&b[0], len(b)), nil
		}
		return b, nil
	case mysql.TypeFloat:
		b := row.GetFloat32(idx)
		if math.IsNaN(float64(b)) || math.IsInf(float64(b), 1) || math.IsInf(float64(b), -1) {
			warn := fmt.Sprintf("the value is invalid in column: %f", b)
			log.Warn(warn)
			b = 0
		}
		return b, nil
	case mysql.TypeDouble:
		b := row.GetFloat64(idx)
		if math.IsNaN(b) || math.IsInf(b, 1) || math.IsInf(b, -1) {
			warn := fmt.Sprintf("the value is invalid in column: %f", b)
			log.Warn(warn)
			b = 0
		}
		return b, nil
	default:
		d := row.GetDatum(idx, &col.FieldType)
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		return d.GetValue(), nil
	}
}
