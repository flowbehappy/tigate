package common

import (
	"encoding/binary"

	"github.com/google/uuid"
	"github.com/tinylib/msgp/msgp"
)

var DefaultEndian = binary.LittleEndian

type Ts uint64
type TableID int64

type DispatcherID uuid.UUID

func (d *DispatcherID) Msgsize() int {
	return 16
}

func (d DispatcherID) MarshalMsg(b []byte) ([]byte, error) {
	return msgp.AppendBytes(b, d[:]), nil
}

func (d *DispatcherID) UnmarshalMsg(b []byte) ([]byte, error) {
	var tmp []byte
	var err error
	tmp, b, err = msgp.ReadBytesBytes(b, tmp)
	if err != nil {
		return nil, err
	}
	copy(d[:], tmp)
	return b, nil
}

func (d DispatcherID) EncodeMsg(en *msgp.Writer) error {
	return en.WriteBytes(d[:])
}

func (d *DispatcherID) DecodeMsg(dc *msgp.Reader) error {
	var tmp []byte
	tmp, err := dc.ReadBytes(tmp)
	if err != nil {
		return err
	}
	copy(d[:], tmp)
	return nil
}

type SchemaID int64

type TopicType string

type EpochType uint64

type AddressType string
