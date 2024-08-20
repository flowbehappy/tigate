package common

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/flowbehappy/tigate/eventpb"
	"github.com/google/uuid"
	"github.com/tinylib/msgp/msgp"
)

var DefaultEndian = binary.LittleEndian

type Ts = uint64
type TableID = int64

type DispatcherID GID

func NewDispatcherID() DispatcherID {
	return DispatcherID(NewGID())
}

func NewDispatcherIDFromPB(pb *eventpb.DispatcherID) DispatcherID {
	d := DispatcherID{low: pb.Low, high: pb.High}
	return d
}

func (d DispatcherID) ToPB() *eventpb.DispatcherID {
	return &eventpb.DispatcherID{
		Low:  d.low,
		High: d.high,
	}
}

func (d DispatcherID) String() string {
	return GID(d).String()
}

func (d *DispatcherID) Msgsize() int {
	return 16
}

func (d DispatcherID) MarshalMsg(b []byte) ([]byte, error) {
	return msgp.AppendBytes(b, GID(d).Marshal()), nil
}

func (d *DispatcherID) UnmarshalMsg(b []byte) ([]byte, error) {
	var tmp []byte
	var err error
	tmp, b, err = msgp.ReadBytesBytes(b, tmp)
	if err != nil {
		return nil, err
	}

	err = d.Unmarshal(tmp)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (d *DispatcherID) Unmarshal(b []byte) error {
	gid := GID{}
	gid.Unmarshal(b)
	*d = DispatcherID(gid)
	return nil
}

func (d DispatcherID) Marshal() []byte {
	return GID(d).Marshal()
}

func (d DispatcherID) EncodeMsg(en *msgp.Writer) error {
	return en.WriteBytes(GID(d).Marshal())
}

func (d *DispatcherID) DecodeMsg(dc *msgp.Reader) error {
	var tmp []byte
	tmp, err := dc.ReadBytes(tmp)
	if err != nil {
		return err
	}
	err = d.Unmarshal(tmp)
	return err
}

type SchemaID int64

type GID struct {
	low  uint64
	high uint64
}

func (g GID) IsZero() bool {
	return g.low == 0 && g.high == 0
}

func (g GID) Marshal() []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], g.low)
	binary.LittleEndian.PutUint64(b[8:16], g.high)
	return b
}

func (g *GID) Unmarshal(b []byte) {
	g.low = binary.LittleEndian.Uint64(b[0:8])
	g.high = binary.LittleEndian.Uint64(b[8:16])
}

func NewGID() GID {
	uuid := uuid.New()
	return GID{
		low:  binary.LittleEndian.Uint64(uuid[0:8]),
		high: binary.LittleEndian.Uint64(uuid[8:16]),
	}
}

func (g GID) String() string {
	var buf bytes.Buffer
	buf.WriteString(strconv.FormatUint(g.low, 10))
	buf.WriteString(strconv.FormatUint(g.high, 10))
	return buf.String()
}
