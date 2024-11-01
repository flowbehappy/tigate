package eventstore

import (
	"encoding/binary"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

const (
	typeDelete = iota + 1
	typeUpdate
	typeInsert
)

// EncodeKeyPrefix encodes uniqueID, tableID, CRTs and StartTs.
// StartTs is optional.
// The result should be a prefix of normal key. (TODO: add a unit test)
func EncodeKeyPrefix(uniqueID uint64, tableID int64, CRTs uint64, startTs ...uint64) []byte {
	if len(startTs) > 1 {
		log.Panic("startTs should be at most one")
	}
	// uniqueID, tableID, CRTs.
	keySize := 8 + 8 + 8
	if len(startTs) > 0 {
		keySize += 8
	}
	buf := make([]byte, 0, keySize)
	uint64Buf := [8]byte{}
	// uniqueID
	binary.BigEndian.PutUint64(uint64Buf[:], uniqueID)
	buf = append(buf, uint64Buf[:]...)
	// tableID
	binary.BigEndian.PutUint64(uint64Buf[:], uint64(tableID))
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
// Format: uniqueID, tableID, CRTs, startTs, delete/update/insert, Key.
func EncodeKey(uniqueID uint64, tableID int64, event *common.RawKVEntry) []byte {
	if event == nil {
		log.Panic("rawkv must not be nil", zap.Any("event", event))
	}
	// uniqueID, tableID, CRTs, startTs, Put/Delete, Key
	length := 8 + 8 + 8 + 8 + 2 + len(event.Key)
	buf := make([]byte, 0, length)
	uint64Buf := [8]byte{}
	// unique ID
	binary.BigEndian.PutUint64(uint64Buf[:], uniqueID)
	buf = append(buf, uint64Buf[:]...)
	// table ID
	binary.BigEndian.PutUint64(uint64Buf[:], uint64(tableID))
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
