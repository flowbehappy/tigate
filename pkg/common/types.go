package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/heartbeatpb"
)

const (
	// DefaultNamespace is the default namespace value,
	// all the old changefeed will be put into default namespace
	DefaultNamespace = "default"
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
	d := DispatcherID{Low: pb.Low, High: pb.High}
	return d
}

func (d DispatcherID) ToPB() *heartbeatpb.DispatcherID {
	return &heartbeatpb.DispatcherID{
		Low:  d.Low,
		High: d.High,
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
	return d.Low == tbl.Low && d.High == tbl.High
}

func (d DispatcherID) Less(t any) bool {
	cf := t.(DispatcherID)
	return d.Low < cf.Low || d.Low == cf.Low && d.High < cf.High
}

type SchemaID int64

type GID struct {
	Low  uint64 `json:"Low"`
	High uint64 `json:"High"`
}

func (g GID) IsZero() bool {
	return g.Low == 0 && g.High == 0
}

func (g GID) Marshal() []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], g.Low)
	binary.LittleEndian.PutUint64(b[8:16], g.High)
	return b
}

func (g *GID) Unmarshal(b []byte) {
	g.Low = binary.LittleEndian.Uint64(b[0:8])
	g.High = binary.LittleEndian.Uint64(b[8:16])
}

func NewGID() GID {
	uuid := uuid.New()
	return GID{
		Low:  binary.LittleEndian.Uint64(uuid[0:8]),
		High: binary.LittleEndian.Uint64(uuid[8:16]),
	}
}

func (g GID) String() string {
	var buf bytes.Buffer
	buf.WriteString(strconv.FormatUint(g.Low, 10))
	buf.WriteString(strconv.FormatUint(g.High, 10))
	return buf.String()
}

func NewGIDWithValue(Low uint64, High uint64) GID {
	return GID{
		Low:  Low,
		High: High,
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

// ChangeFeedDisplayName represents the user-friendly name and namespace of a changefeed.
// This structure is used for external queries and display purposes.
type ChangeFeedDisplayName struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

func NewChangeFeedDisplayName(name string, namespace string) ChangeFeedDisplayName {
	return ChangeFeedDisplayName{
		Name:      name,
		Namespace: namespace,
	}
}

func (r ChangeFeedDisplayName) String() string {
	return r.Namespace + "/" + r.Name
}

// ChangefeedID is the unique identifier of a changefeed.
// GID is the inner unique identifier of a changefeed.
// we can use Id to represent the changefeedID in performance-critical scenarios.
// DisplayName is the user-friendly expression of a changefeed.
// ChangefeedID can be specified the name of changefeedID.
// If the name is not specified, it will be the id in string format.
// We ensure whether the id or the representation is both unique in the cluster.
type ChangeFeedID struct {
	Id          GID                   `json:"id"`
	DisplayName ChangeFeedDisplayName `json:"display"`
}

func NewChangefeedID() ChangeFeedID {
	cfID := ChangeFeedID{
		Id: NewGID(),
	}
	cfID.DisplayName = ChangeFeedDisplayName{
		Name:      cfID.Id.String(),
		Namespace: DefaultNamespace,
	}
	return cfID
}

func NewChangeFeedIDWithName(name string) ChangeFeedID {
	return ChangeFeedID{
		Id: NewGID(),
		DisplayName: ChangeFeedDisplayName{
			Name:      name,
			Namespace: DefaultNamespace,
		},
	}
}

func (cfID ChangeFeedID) String() string {
	return fmt.Sprintf("changefeedID representation: %s, id: %s", cfID.DisplayName.String(), cfID.Id.String())
}

func (cfID ChangeFeedID) Name() string {
	return cfID.DisplayName.Name
}

func (cfID ChangeFeedID) Namespace() string {
	return cfID.DisplayName.Namespace
}

func (cfID ChangeFeedID) ID() GID {
	return cfID.Id
}

func NewChangefeedIDFromPB(pb *heartbeatpb.ChangefeedID) ChangeFeedID {
	d := ChangeFeedID{
		Id: GID{
			Low:  pb.Low,
			High: pb.High,
		},
		DisplayName: ChangeFeedDisplayName{
			Name:      pb.Name,
			Namespace: pb.Namespace,
		},
	}
	return d
}

func (c ChangeFeedID) ToPB() *heartbeatpb.ChangefeedID {
	return &heartbeatpb.ChangefeedID{
		Low:       c.Id.Low,
		High:      c.Id.High,
		Name:      c.Name(),
		Namespace: c.Namespace(),
	}
}
