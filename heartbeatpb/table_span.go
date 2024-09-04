package heartbeatpb

import (
	"bytes"
)

// DDLSpanSchemaID is the special schema id for DDL
var DDLSpanSchemaID int64 = 0

// DDLSpan is the special span for Table Trigger Event Dispatcher
var DDLSpan = &TableSpan{TableID: 0, StartKey: nil, EndKey: nil}

// Less compares two Spans, defines the order between spans.
func (s *TableSpan) Less(other any) bool {
	tbl := other.(*TableSpan)
	if s.TableID < tbl.TableID {
		return true
	}
	if bytes.Compare(s.StartKey, tbl.StartKey) < 0 {
		return true
	}
	return false
}

func (s *TableSpan) Equal(inferior any) bool {
	tbl := inferior.(*TableSpan)
	return s.TableID == tbl.TableID &&
		bytes.Equal(s.StartKey, tbl.StartKey) &&
		bytes.Equal(s.EndKey, tbl.EndKey)
}

func (s *TableSpan) Copy() *TableSpan {
	return &TableSpan{
		TableID:  s.TableID,
		StartKey: s.StartKey,
		EndKey:   s.EndKey,
	}
}
