package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/heartbeatpb"
)

var DefaultEndian = binary.LittleEndian

type Ts = uint64
type TableID = int64

type CoordinatorID string

func (id CoordinatorID) String() string { return string(id) }

type MaintainerID string

func (m MaintainerID) String() string { return string(m) }

type DispatcherID GID

func NewDispatcherID() DispatcherID {
	return DispatcherID(NewGID())
}

func NewDispatcherIDFromPB(pb *heartbeatpb.DispatcherID) DispatcherID {
	d := DispatcherID{low: pb.Low, high: pb.High}
	return d
}

func (d DispatcherID) ToPB() *heartbeatpb.DispatcherID {
	return &heartbeatpb.DispatcherID{
		Low:  d.low,
		High: d.high,
	}
}

func (d DispatcherID) String() string {
	return GID(d).String()
}

func (d *DispatcherID) GetSize() int {
	return 16
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

func (d DispatcherID) Equal(inferior any) bool {
	tbl := inferior.(DispatcherID)
	return d.low == tbl.low && d.high == tbl.high
}

func (d DispatcherID) Less(t any) bool {
	cf := t.(DispatcherID)
	return d.low < cf.low || d.low == cf.low && d.high < cf.high
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

func NewGIDWithValue(low uint64, high uint64) GID {
	return GID{
		low:  low,
		high: high,
	}
}

type Action uint8

func (a Action) String() string {
	switch a {
	case ActionPause:
		return "pause"
	case ActionResume:
		return "resume"
	case ActionReset:
		return "reset"
	default:
		return "unknown"
	}
}

const (
	ActionPause  Action = 0
	ActionResume Action = 1
	ActionReset  Action = 2
)

type DispatcherAction struct {
	DispatcherID DispatcherID
	Action       Action
}

func (a DispatcherAction) String() string {
	return fmt.Sprintf("dispatcherID: %s, action: %s", a.DispatcherID, a.Action.String())
}
