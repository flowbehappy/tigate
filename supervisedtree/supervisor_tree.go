package supervisedtree

import (
	"github.com/flowbehappy/tigate/apperror"
	"github.com/pingcap/log"

	"github.com/google/uuid"
)

// A supervised tree is a tree structure where each node has a (or none) superior and some (or none) inferiors.
// A SupervisedNode could be a Supervisor or a Worker.
// A Supervisor is a node other than the bottom nodes in a supervised tree. The root Supervisor has no superior.
// A Worker is a node at the bottom of a supervised tree, which has no inferiors.
//
// Here is an example of a supervised tree similar to a B+ tree:
//
//                     +------------------+
//                     | Supervisor (root)|
//                     +------------------+
//                    /         |         \
//   +----------------+  +----------------+  +----------------+
//   |   Supervisor   |  |   Supervisor   |  |   Supervisor   |
//   +----------------+  +----------------+  +----------------+
//   /        |       \          |        \          |
// +---+   +---+     +---+     +---+     +---+     +---+
// | W |   | W |     | W |     | W |     | W |     | W |
// +---+   +---+     +---+     +---+     +---+     +---+
//
// In this example, there is a root Supervisor node named "Supervisor (root)" at the top, which has three Supervisor nodes as its inferiors.
// Each Supervisor node can have multiple Worker nodes as its inferiors.
// The arrows represent the superior-inferior relationship between nodes.

type ExtraNodeInfo interface {
	Info() string
}

// The SupervisedNodeID is used to identify an in-memory supervised node object.
// The Epoch comes from the root supervisor, the TaskID comes from the actual job of the supervised node,
// and the ObjectID is created when the supervised node object is created via "uuid.New()".
//
// Here is the example from the scheduler system of this repository:
// When a new Coordinator is elected, the epoch will be increased by 1, starting from a small fixed number.
// And the Coordinator assigns the Maintainers, and each Maintainer has the same epoch as the Coordinator's.
// The TaskID comes from the actual job of the supervised node. E.g.
//   - The TaskID of the Coordinator is "coordinator".
//   - The TaskID of a Maintainer is "maintainer.<changefeed_id>"
//   - The TaskID of an Event Dispatcher is "dispatcher.event.<changefeed_id>.<table_id>
type SupervisedNodeID struct {
	// ObjectID is the unique identifier of the supervised node object. It is used as the key in a map.
	ObjectID uuid.UUID
	// Epoch is used in cases including the election, correctness check, and the TTL update.
	Epoch int64
	// TaskID is mainly used in user-facing logs and metrics.
	TaskID string
	// The host message of the node. E.g. "127.0.0.1"
	HostInfo string
	// Other information of the node.
	ExtraInfo ExtraNodeInfo
}

type SupervisedNode interface {
	GetID() SupervisedNodeID
	GetToken() *AliveToken
	UpdateToken(sid *SupervisedNodeID, token *AliveToken) error
}

type Supervisor interface {
	GetSuperior() *Supervisor
	GetInferiors() map[SupervisedNodeID]SupervisedNode
}

type Worker interface {
	GetSuperior() *Supervisor
}

type SupervisedNodeWithTTLImpl struct {
	id    SupervisedNodeID
	token *AliveToken
}

func (s *SupervisedNodeWithTTLImpl) GetID() SupervisedNodeID {
	return s.id
}

func (s *SupervisedNodeWithTTLImpl) GetToken() *AliveToken {
	return s.token
}

func (s *SupervisedNodeWithTTLImpl) UpdateToken(sid *SupervisedNodeID, token *AliveToken) error {
	if s.id.Epoch > sid.Epoch {
		return apperror.APPError{Type: apperror.ErrorTypeEpochSmaller, Reason: "The epoch of the new supervisor is smaller than the current supervisor ID."}
	}
	if s.token.TTL.After(token.TTL) {
		log.Warn("The new TTL is smaller than the current TTL.")
		return nil
	}
	s.token = token
	return nil
}
