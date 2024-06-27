package supervisedtree

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A persudo implementation of supervisor
type Supervisor struct {
	SupervisedNodeWithTTLImpl
	superior  SupervisedNode
	inferiors []SupervisedNode
}

func (s *Supervisor) GetType() SupervisedType     { return TypeSupervisor }
func (s *Supervisor) GetSuperior() SupervisedNode { return s.superior }
func (s *Supervisor) GetInferiors() <-chan SupervisedNode {
	ch := make(chan SupervisedNode)
	go func() {
		for _, inferior := range s.inferiors {
			ch <- inferior
		}
		close(ch)
	}()
	return ch
}

// A persudo implementation of worker
type Worker struct {
	SupervisedNodeWithTTLImpl
	superior SupervisedNode
}

func (s *Worker) GetType() SupervisedType             { return TypeWorker }
func (s *Worker) GetSuperior() SupervisedNode         { return s.superior }
func (s *Worker) GetInferiors() <-chan SupervisedNode { return nil }

func TestSupervisedNodeWithTTLImpl_String(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()
	token := &AliveToken{time.Now()}
	// Create a supervisor node
	supervisorID := &SupervisedNodeID{
		ObjectID:  id1,
		Epoch:     1,
		TaskID:    "supervisor",
		HostInfo:  "127.0.0.1",
		ExtraInfo: StringInfo("Extra infos"),
	}
	supervisor := &Supervisor{}
	supervisor.id = supervisorID
	supervisor.token = token

	worker1 := &Worker{}
	worker1.id = &SupervisedNodeID{
		ObjectID:  id2,
		Epoch:     1,
		TaskID:    "worker1",
		HostInfo:  "127.0.0.1",
		ExtraInfo: StringInfo("Extra info1"),
	}
	worker1.token = token

	worker2 := &Worker{}
	worker2.id = &SupervisedNodeID{
		ObjectID:  id3,
		Epoch:     1,
		TaskID:    "worker2",
		HostInfo:  "127.0.0.2",
		ExtraInfo: StringInfo("Extra info2"),
	}
	worker2.token = token

	// Set inferiors for the supervisor
	supervisor.inferiors = []SupervisedNode{worker1, worker2}

	// Call the String() method and check the result
	expected := fmt.Sprintf("{\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"supervisor\", \"host_info\": \"127.0.0.1\", \"extra_info\": \"Extra infos\"}, \"type\": \"Supervisor\", \"children\": [{\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"worker1\", \"host_info\": \"127.0.0.1\", \"extra_info\": \"Extra info1\"}, \"type\": \"Worker\"}, {\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"worker2\", \"host_info\": \"127.0.0.2\", \"extra_info\": \"Extra info2\"}, \"type\": \"Worker\"}]}", id1.String(), id2.String(), id3.String())
	result := PrintString(supervisor, false)
	if result != expected {
		t.Errorf("Unexpected result. Expected: %s, Got: %s", expected, result)
	}

	expected = fmt.Sprintf("{\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"supervisor\", \"host_info\": \"127.0.0.1\", \"extra_info\": \"Extra infos\"}, \"type\": \"Supervisor\",, \"token\": \"%s\" \"children\": [{\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"worker1\", \"host_info\": \"127.0.0.1\", \"extra_info\": \"Extra info1\"}, \"type\": \"Worker\", \"token\": \"%s\"}, {\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"worker2\", \"host_info\": \"127.0.0.2\", \"extra_info\": \"Extra info2\"}, \"type\": \"Worker\", \"token\": \"%s\"}]}", id1.String(), token.String(), id2.String(), token.String(), id3.String(), token.String())
	result = PrintString(supervisor, true)
	if result != expected {
		t.Errorf("Unexpected result. Expected: %s, Got: %s", expected, result)
	}
}
