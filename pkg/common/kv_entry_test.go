package common

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRawKVEntryEncodeDecode_PutOperation(t *testing.T) {
	original := RawKVEntry{
		OpType:   OpTypePut,            // 4 bytes
		CRTs:     1234567890,           // 8 bytes
		StartTs:  9876543210,           // 8 bytes
		RegionID: 42,                   // 8 bytes
		Key:      []byte("12345678"),   // 8 bytes
		Value:    []byte("123456789A"), // 10 bytes
		OldValue: make([]byte, 0),      // 10 bytes
	}

	original.KeyLen = uint32(len(original.Key))           // 4 bytes
	original.ValueLen = uint32(len(original.Value))       // 4 bytes
	original.OldValueLen = uint32(len(original.OldValue)) // 4 bytes

	encoded := original.Encode()

	log.Info("encoded", zap.Any("encoded", encoded), zap.Int("len", len(encoded)))

	var decoded RawKVEntry
	err := decoded.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestRawKVEntryEncodeDecode_DeleteOperation(t *testing.T) {
	original := RawKVEntry{
		OpType:   OpTypeDelete,
		CRTs:     1111111111,
		StartTs:  2222222222,
		RegionID: 24,
		Key:      []byte("delete_key"),
		Value:    make([]byte, 0),
		OldValue: []byte("old_value"),
	}

	encoded := original.Encode()

	var decoded RawKVEntry
	err := decoded.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestRawKVEntryEncodeDecode_ResolvedOperation(t *testing.T) {
	original := RawKVEntry{
		OpType:   OpTypeResolved,
		CRTs:     3333333333,
		StartTs:  4444444444,
		RegionID: 100,
		Key:      make([]byte, 0),
		Value:    make([]byte, 0),
		OldValue: make([]byte, 0),
	}

	encoded := original.Encode()

	var decoded RawKVEntry
	err := decoded.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestCompareEncodedSize(t *testing.T) {
	entry := getRawKVEntry()
	encoded := entry.Encode()
	jsonEncoded, err := json.Marshal(entry)
	require.NoError(t, err)

	require.Less(t, len(encoded), len(jsonEncoded))
}
