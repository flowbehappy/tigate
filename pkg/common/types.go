package common

import (
	"encoding/binary"

	"github.com/google/uuid"
)

type Ts uint64
type TableID int64
type DispatcherID uuid.UUID
type DatabaseID int64
type SchemaID int64

type TopicType string

type EpochType uint64

type AddressType string

var DefaultEndian = binary.LittleEndian
