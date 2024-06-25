package supervisedtree

import (
	"github.com/flowbehappy/tigate/apperror"
	"github.com/pingcap/log"

	"github.com/google/uuid"
)

// The SupervisorID is used to identify an in-memory supervisor object.
// The Epoch comes from the root supervisor, the TaskID comes from the acutal job of the supervisor,
// and the ObjectID is created when the supervisor object is created via "uuid.New()".
//
// Here is the example from the scheduler system of this repository:
// When a new Coordinator is elected, the epoch will be increased by 1, starting from a small fixed number.
// And the the Coordinator assigns the Maintainers, and each Maintainer has the same epoch as the Coordiantor's.
// The TaskID comes from the actual job of the supervisor. E.g.
//   - The TaskID of the Coordinator is "coordinator".
//   - The TaskID of a Maintainer is "maintainer.<changefeed_id>"
//   - The TaskID of a Event Dispatcher is "dispatcher.event.<changefeed_id>.<table_id>
type SupervisorID struct {
	Epoch    int64
	TaskID   string
	ObjectID uuid.UUID
}

// A supervisor is a node in the supervised tree.
// Each supervisor has a superior (the manager of this node) and some inferiors (the wokers of this node).
type Supervisor interface {
	GetID() SupervisorID
	GetToken() *AliveToken
	UpdateToken(sid *SupervisorID, token *AliveToken) error
}

type SupervisorWithTTL struct {
	id    SupervisorID
	token *AliveToken
}

func (s *SupervisorWithTTL) GetID() SupervisorID {
	return s.id
}

func (s *SupervisorWithTTL) GetToken() *AliveToken {
	return s.token
}

func (s *SupervisorWithTTL) UpdateToken(sid *SupervisorID, token *AliveToken) error {
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
