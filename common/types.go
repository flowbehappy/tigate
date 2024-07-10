package common

import (
	"encoding/binary"

	"github.com/google/uuid"
)

type Timestamp uint64
type TableID int64
type DispatcherID uuid.UUID
type DatabaseID int64
type SchemaID int64

var DefaultEndian = binary.LittleEndian
