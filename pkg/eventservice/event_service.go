package eventservice

import (
	. "github.com/flowbehappy/tigate/pkg/common"
	"github.com/google/uuid"
)

type GlobalId uuid.UUID
type TableId uint64

type EventAcceptor interface {
	GetId() GlobalId
	GetServerId()
	GetTableId() TableId
	// GetTableRange() xxx
	GetInitialTsRange() (Timestamp, Timestamp)
}

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type EventService interface {
	RegisterAcceptor(acceptor EventAcceptor) error
	UnregisterAcceptor(id GlobalId) error
}

// Store the progress of the acceptor, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this acceptor.
type acceptorStatus struct {
	acceptor   EventAcceptor
	tsProgress Timestamp
}

type tableStat struct {
	tableId TableId
}

type eventService struct {
}
