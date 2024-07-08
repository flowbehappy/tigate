package supervisedtree

import (
	"fmt"

	"github.com/flowbehappy/tigate/scheduler"

	"github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/pingcap/log"
	"go.uber.org/zap"

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
	return fmt.Sprintf("{\"object_id\": \"%s\", \"epoch\": %d, \"task_id\": \"%s\", \"host_info\": \"%s\", \"extra_info\": \"%s\"}",
		sid.ObjectID.String(), sid.Epoch, sid.TaskID, sid.HostInfo, sid.ExtraInfo.Info())
}

func (sid *SupervisedNodeID) Equal(id scheduler.InferiorID) bool {
	return sid.ObjectID == id.(*SupervisedNodeID).ObjectID
}

func (sid *SupervisedNodeID) Less(id scheduler.InferiorID) bool {
	return sid.TaskID < id.(*SupervisedNodeID).TaskID
}

type SupervisedType int

const (
	// A Supervisor is a supervised node other than the bottom nodes in a supervised tree
	TypeSupervisor SupervisedType = 1
	// A Worker is a supervised node at the bottom of a supervised tree, which has no inferiors
	TypeWorker SupervisedType = 2
)

func (t SupervisedType) String() string {
	if t == TypeSupervisor {
		return "Supervisor"
	} else {
		return "Worker"
	}
}

func (t SupervisedType) isSupervisor() bool { return t == TypeSupervisor }
func (t SupervisedType) isWorker() bool     { return t == TypeWorker }

// A node in a supervised tree.
type SupervisedNode interface {
	GetType() SupervisedType
	GetID() *SupervisedNodeID
	GetToken() *AliveToken
	UpdateToken(sid *SupervisedNodeID, token *AliveToken) error

	// Those two methods below are usually only use by logging the tree's information.
	// We use a channel to return the inferiors in case the inferiors are too many to store in memory.
	GetInferiors() <-chan SupervisedNode
	GetSuperior() SupervisedNode
}

// A supervied node with common implementation.
type SupervisedNodeWithTTLImpl struct {
	id    *SupervisedNodeID
	token *AliveToken
}

// This is a place holder. It should be overriden by the child struct. If not, it will cause a panic when calling this method.
func (s *SupervisedNodeWithTTLImpl) GetType() SupervisedType {
	panic("Please implement this method GetType!")
}
func (s *SupervisedNodeWithTTLImpl) GetID() *SupervisedNodeID { return s.id }
func (s *SupervisedNodeWithTTLImpl) GetToken() *AliveToken    { return s.token }

func (s *SupervisedNodeWithTTLImpl) UpdateToken(sid *SupervisedNodeID, token *AliveToken) error {
	if s.id.Epoch > sid.Epoch {
		return apperror.AppError{Type: apperror.ErrorTypeEpochSmaller, Reason: "The epoch of the new supervisor is smaller than the current supervisor ID."}
	}
	if s.token.TTL.After(token.TTL) {
		log.Warn("The new TTL is smaller than the current TTL.",
			zap.String("new TTL", token.TTL.String()),
			zap.String("current TTL", s.token.TTL.String()))
		return nil
	}
	s.token = token
	return nil
}

func PrintString(s SupervisedNode, print_token bool) string {
	var token = ""
	if print_token && s.GetToken() != nil {
		token = fmt.Sprintf(", \"token\": \"%s\"", s.GetToken().String())
	}
	if s.GetType().isSupervisor() {
		var children = ""
		for inferior := range s.GetInferiors() {
			children += PrintString(inferior, print_token) + ", "
		}
		if len(children) != 0 {
			children = children[:len(children)-2]
		}

		if len(children) == 0 {
			return fmt.Sprintf("{\"id\": %s, \"type\": \"%s\"%s}", s.GetID().String(), s.GetType().String(), token)
		} else {
			return fmt.Sprintf("{\"id\": %s, \"type\": \"%s\",%s \"children\": [%s]}", s.GetID().String(), s.GetType().String(), token, children)
		}
	} else if s.GetType().isWorker() {
		return fmt.Sprintf("{\"id\": %s, \"type\": \"%s\"%s}", s.GetID().String(), s.GetType().String(), token)
	} else {
		panic(fmt.Sprintf("The type(%d) of the supervised node is unknown.", s.GetType()))
	}
}
