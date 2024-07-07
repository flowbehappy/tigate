package common

import (
	"encoding/binary"
)

type Timestamp uint64
type TableId int64

var DefaultEndian = binary.LittleEndian
