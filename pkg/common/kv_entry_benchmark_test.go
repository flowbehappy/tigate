package common

import (
	"encoding/json"
	"testing"
)

func getRawKVEntry() *RawKVEntry {
	res := &RawKVEntry{
		OpType:       OpTypePut,
		CRTs:         1234567890,
		StartTs:      9876543210,
		RegionID:     42,
		CompressType: CompressTypeNone,
		Key:          []byte("test-key"),
	}
	var value string
	// 1600 bytes
	for i := 0; i < 100; i++ {
		value += "0123456789ABCDEF" // 16 bytes
	}
	res.Value = []byte(value)
	res.OldValue = []byte(value)
	return res
}

// BenchmarkRawKVEntry_EncodeDecode-10    	 2949572	       389.0 ns/op	    3456 B/op	       1 allocs/op
func BenchmarkRawKVEntry_EncodeDecode(b *testing.B) {
	entry := getRawKVEntry()

	b.ResetTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded := entry.Encode()
		decodedEntry := &RawKVEntry{}
		_ = decodedEntry.Decode(encoded)
	}
}

// BenchmarkRawKVEntry_MarshalUnmarshal-10    	   51458	     22896 ns/op	    8828 B/op	       9 allocs/op
func BenchmarkRawKVEntry_MarshalUnmarshal(b *testing.B) {
	entry := getRawKVEntry()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := json.Marshal(entry)
		if err != nil {
			b.Fatalf("Failed to marshal: %v", err)
		}
		decodedEntry := &RawKVEntry{}
		err = json.Unmarshal(encoded, decodedEntry)
		if err != nil {
			b.Fatalf("Failed to unmarshal: %v", err)
		}
	}
}
