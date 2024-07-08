package common

import (
	"encoding/binary"
)

type Timestamp uint64
type TableID int64
type DispatcherID string
type DatabaseID int64

var DefaultEndian = binary.LittleEndian
