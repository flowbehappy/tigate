package supervisedtree

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A persudo implementation of SupervisorTest
type SupervisorTest struct {
	SupervisedNodeWithTTLImpl
	superior  SupervisedNode
	inferiors []SupervisedNode
}

func (s *SupervisorTest) GetType() SupervisedType     { return TypeSupervisor }
func (s *SupervisorTest) GetSuperior() SupervisedNode { return s.superior }
func (s *SupervisorTest) GetInferiors() <-chan SupervisedNode {
	ch := make(chan SupervisedNode)
	go func() {
		for _, inferior := range s.inferiors {
			ch <- inferior
		}
		close(ch)
	}()
	return ch
}

// A persudo implementation of WokerTest
type WokerTest struct {
	SupervisedNodeWithTTLImpl
	superior SupervisedNode
}

func (s *WokerTest) GetType() SupervisedType             { return TypeWorker }
func (s *WokerTest) GetSuperior() SupervisedNode         { return s.superior }
func (s *WokerTest) GetInferiors() <-chan SupervisedNode { return nil }

func TestSupervisedNodeWithTTLImpl_String(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()
	token := &AliveToken{time.Now()}
	// Create a SupervisorTest node
	SupervisorTestID := &SupervisedNodeID{
		ObjectID:  id1,
		Epoch:     1,
		TaskID:    "SupervisorTest",
		HostInfo:  "127.0.0.1",
		ExtraInfo: StringInfo("Extra infos"),
	}
	SupervisorTest := &SupervisorTest{}
	SupervisorTest.id = SupervisorTestID
	SupervisorTest.token = token

	WokerTest1 := &WokerTest{}
	WokerTest1.id = &SupervisedNodeID{
		ObjectID:  id2,
		Epoch:     1,
		TaskID:    "WokerTest1",
		HostInfo:  "127.0.0.1",
		ExtraInfo: StringInfo("Extra info1"),
	}
	WokerTest1.token = token

	WokerTest2 := &WokerTest{}
	WokerTest2.id = &SupervisedNodeID{
		ObjectID:  id3,
		Epoch:     1,
		TaskID:    "WokerTest2",
		HostInfo:  "127.0.0.2",
		ExtraInfo: StringInfo("Extra info2"),
	}
	WokerTest2.token = token

	// Set inferiors for the SupervisorTest
	SupervisorTest.inferiors = []SupervisedNode{WokerTest1, WokerTest2}

	// Call the String() method and check the result
	expected := fmt.Sprintf("{\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"SupervisorTest\", \"host_info\": \"127.0.0.1\", \"extra_info\": \"Extra infos\"}, \"type\": \"SupervisorTest\", \"children\": [{\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"WokerTest1\", \"host_info\": \"127.0.0.1\", \"extra_info\": \"Extra info1\"}, \"type\": \"WokerTest\"}, {\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"WokerTest2\", \"host_info\": \"127.0.0.2\", \"extra_info\": \"Extra info2\"}, \"type\": \"WokerTest\"}]}", id1.String(), id2.String(), id3.String())
	result := PrintString(SupervisorTest, false)
	if result != expected {
		t.Errorf("Unexpected result. Expected: %s, Got: %s", expected, result)
	}

	expected = fmt.Sprintf("{\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"SupervisorTest\", \"host_info\": \"127.0.0.1\", \"extra_info\": \"Extra infos\"}, \"type\": \"SupervisorTest\",, \"token\": \"%s\" \"children\": [{\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"WokerTest1\", \"host_info\": \"127.0.0.1\", \"extra_info\": \"Extra info1\"}, \"type\": \"WokerTest\", \"token\": \"%s\"}, {\"id\": {\"object_id\": \"%s\", \"epoch\": 1, \"task_id\": \"WokerTest2\", \"host_info\": \"127.0.0.2\", \"extra_info\": \"Extra info2\"}, \"type\": \"WokerTest\", \"token\": \"%s\"}]}", id1.String(), token.String(), id2.String(), token.String(), id3.String(), token.String())
	result = PrintString(SupervisorTest, true)
	if result != expected {
		t.Errorf("Unexpected result. Expected: %s, Got: %s", expected, result)
	}
}
