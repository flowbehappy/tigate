package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDMLEvent test the Marshal and Unmarshal of DMLEvent.
func TestDMLEvent(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	data, err := dmlEvent.Marshal()
	require.NoError(t, err)

	reverseEvent := &DMLEvent{}
	// Set the TableInfo before unmarshal, it is used in Unmarshal.
	reverseEvent.TableInfo = dmlEvent.TableInfo
	err = reverseEvent.Unmarshal(data)
	require.NoError(t, err)

	// Compare the content of the two event's rows.
	require.Equal(t, dmlEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()), reverseEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()))
	for i := 0; i < dmlEvent.Rows.NumRows(); i++ {
		for j := 0; j < dmlEvent.Rows.NumCols(); j++ {
			require.Equal(t, dmlEvent.Rows.GetRow(i).GetRaw(j), reverseEvent.Rows.GetRow(i).GetRaw(j))
		}
	}
	// Compare the remaining content of the two events.
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	require.Equal(t, dmlEvent, reverseEvent)
}

func TestEncodeAndDecodeV0(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	data, err := dmlEvent.encodeV0()
	require.NoError(t, err)

	reverseEvent := &DMLEvent{}
	// Set the TableInfo before decode, it is used in decode.
	reverseEvent.TableInfo = dmlEvent.TableInfo
	err = reverseEvent.decodeV0(data)
	require.NoError(t, err)

	require.Equal(t, dmlEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()), reverseEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()))
	for i := 0; i < dmlEvent.Rows.NumRows(); i++ {
		for j := 0; j < dmlEvent.Rows.NumCols(); j++ {
			require.Equal(t, dmlEvent.Rows.GetRow(i).GetRaw(j), reverseEvent.Rows.GetRow(i).GetRaw(j))
		}
	}

	// Compare the remaining content of the two events.
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	require.Equal(t, dmlEvent, reverseEvent)
}
