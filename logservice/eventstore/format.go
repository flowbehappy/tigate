package eventstore

import (
	"encoding/binary"

	"github.com/flowbehappy/tigate/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	typeDelete = iota + 1
	typeUpdate
	typeInsert
)

// DecodeKey decodes a key to tableID, startTs, CRTs.
func DecodeKey(key []byte) (tableID uint64, startTs, CRTs uint64) {
	// tableID, CRTs, startTs, Key, Put/Delete
	// table ID
	tableID = binary.BigEndian.Uint64(key[4:])
	// CRTs
	CRTs = binary.BigEndian.Uint64(key[12:])
	if len(key) >= 28 {
		// startTs
		startTs = binary.BigEndian.Uint64(key[20:])
	}
	return
}

// DecodeCRTs decodes CRTs from the given key.
func DecodeCRTs(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[12:])
}

// EncodeTsKey encodes tableID, CRTs and StartTs.
// StartTs is optional.
func EncodeTsKey(tableID uint64, CRTs uint64, startTs ...uint64) []byte {
	var buf []byte
	if len(startTs) == 0 {
		// tableID, CRTs.
		buf = make([]byte, 0, 8+8)
	} else if len(startTs) == 1 {
		// tableID, CRTs and startTs.
		buf = make([]byte, 0, 8+8+8)
	} else {
		log.Panic("EncodeTsKey retrieve one startTs at most")
	}

	uint64Buf := [8]byte{}
	// tableID
	binary.BigEndian.PutUint64(uint64Buf[:], tableID)
	buf = append(buf, uint64Buf[:]...)
	// CRTs
	binary.BigEndian.PutUint64(uint64Buf[:], CRTs)
	buf = append(buf, uint64Buf[:]...)
	if len(startTs) > 0 {
		// startTs
		binary.BigEndian.PutUint64(uint64Buf[:], startTs[0])
		buf = append(buf, uint64Buf[:]...)
	}
	return buf
}

// EncodeKey encodes a key according to event.
// Format: tableID, CRTs, startTs, delete/update/insert, Key.
func EncodeKey(tableID uint64, event *common.RawKVEntry) []byte {
	if event == nil {
		log.Panic("rawkv must not be nil", zap.Any("event", event))
	}
	// tableID, CRTs, startTs, Put/Delete, Key
	length := 8 + 8 + 8 + 2 + len(event.Key)
	buf := make([]byte, 0, length)
	uint64Buf := [8]byte{}
	// table ID
	binary.BigEndian.PutUint64(uint64Buf[:], tableID)
	buf = append(buf, uint64Buf[:]...)
	// CRTs
	binary.BigEndian.PutUint64(uint64Buf[:], event.CRTs)
	buf = append(buf, uint64Buf[:]...)
	// startTs
	binary.BigEndian.PutUint64(uint64Buf[:], event.StartTs)
	buf = append(buf, uint64Buf[:]...)
	// Let Delete < Update < Insert
	binary.BigEndian.PutUint16(uint64Buf[:], getDMLOrder(event))
	buf = append(buf, uint64Buf[:2]...)
	// key
	return append(buf, event.Key...)
}

// getDMLOrder returns the order of the dml types: delete<update<insert
func getDMLOrder(rowKV *common.RawKVEntry) uint16 {
	if rowKV.OpType == common.OpTypeDelete {
		return typeDelete
	} else if rowKV.OldValue != nil {
		return typeUpdate
	}
	return typeInsert
}
