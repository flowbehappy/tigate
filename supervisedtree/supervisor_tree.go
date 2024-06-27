package supervisedtree

import (
	"encoding/json"
	"fmt"

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

type StringInfo string

func (s StringInfo) Info() string {
	return string(s)
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

func (sid *SupervisedNodeID) String() string {
	data, err := json.Marshal(sid)
	if err != nil {
		return ""
	}
	return string(data)
}

type SupervisedType int

const (
	// A Supervisor is a node other than the bottom nodes in a supervised tree
	Supervisor SupervisedType = 1
	// A Worker is a node at the bottom of a supervised tree, which has no inferiors
	Worker SupervisedType = 2
)

func (t SupervisedType) isSupervisor() bool { return t == Supervisor }
func (t SupervisedType) isWorker() bool     { return t == Worker }

// A node in a supervised tree.
type SupervisedNode interface {
	GetType() SupervisedType
	GetID() SupervisedNodeID
	GetToken() *AliveToken
	GetInferiors() []SupervisedNode
	GetSuperior() SupervisedNode
	UpdateToken(sid *SupervisedNodeID, token *AliveToken) error
	String() string
}

// A supervied node with common implementation.
type SupervisedNodeImpl struct {
	id        SupervisedNodeID
	token     *AliveToken
	inferiors []SupervisedNode
	superior  SupervisedNode
}

// This is a place holder. It should be overriden by the child class. If not, it will cause a panic when calling this method.
func (s *SupervisedNodeImpl) GetType() SupervisedType        { return 0 }
func (s *SupervisedNodeImpl) GetID() SupervisedNodeID        { return s.id }
func (s *SupervisedNodeImpl) GetToken() *AliveToken          { return s.token }
func (s *SupervisedNodeImpl) GetInferiors() []SupervisedNode { return s.inferiors }
func (s *SupervisedNodeImpl) GetSuperior() SupervisedNode    { return s.superior }

func (s *SupervisedNodeImpl) UpdateToken(sid *SupervisedNodeID, token *AliveToken) error {
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

func (s *SupervisedNodeImpl) String() string {
	if s.GetType().isSupervisor() {
		var children = ""
		inferiors := s.GetInferiors()
		for i := 0; i < len(inferiors); i++ {
			if i == len(inferiors)-1 {
				children += inferiors[i].String()
			} else {
				children += inferiors[i].String() + ", "
			}
		}
		return fmt.Sprintf("{\"id\": %s, \"children\": %s}", s.id.String(), children)
	} else {
		return fmt.Sprintf("{\"id\": %s}", s.id.String())
	}
}
