package producer

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

// MockDMLProducer is a mock producer for test.
type MockDMLProducer struct {
	mu     sync.Mutex
	events map[string][]*common.Message
}

// NewDMLMockProducer creates a mock producer.
func NewDMLMockProducer() dmlproducer.DMLProducer {
	return &MockDMLProducer{
		events: make(map[string][]*common.Message),
	}
}

// AsyncSendMessage appends a message to the mock producer.
func (m *MockDMLProducer) AsyncSendMessage(_ context.Context, topic string,
	partition int32, message *common.Message,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*common.Message, 0)
	}
	m.events[key] = append(m.events[key], message)

	message.Callback()

	return nil
}

// Close do nothing.
func (m *MockDMLProducer) Close() {
}

// GetAllEvents returns the events received by the mock producer.
func (m *MockDMLProducer) GetAllEvents() []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	var events []*common.Message
	for _, v := range m.events {
		events = append(events, v...)
	}
	return events
}

// GetEvents returns the event filtered by the key.
func (m *MockDMLProducer) GetEvents(topic string, partition int32) []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	return m.events[key]
}
