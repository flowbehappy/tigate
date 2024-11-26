package producer

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

func NewMockDMLProducer() DMLProducer {
	return &MockProducer{
		events: make(map[string][]*common.Message),
	}
}

func NewMockDDLProducer() ddlproducer.DDLProducer {
	return &MockProducer{
		events: make(map[string][]*common.Message),
	}
}

// MockProducer is a mock producer for test.
type MockProducer struct {
	mu     sync.Mutex
	events map[string][]*common.Message
}

// AsyncSendMessage appends a message to the mock producer.
func (m *MockProducer) AsyncSendMessage(_ context.Context, topic string,
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

func (m *MockProducer) Run() error {
	// do nothing
	return nil
}

// Close do nothing.
func (m *MockProducer) Close() {
}

// GetAllEvents returns the events received by the mock producer.
func (m *MockProducer) GetAllEvents() []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	var events []*common.Message
	for _, v := range m.events {
		events = append(events, v...)
	}
	return events
}

// GetEvents returns the event filtered by the key.
func (m *MockProducer) GetEvents(topic string, partition int32) []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	return m.events[key]
}

// SyncBroadcastMessage stores a message to all partitions of the topic.
func (m *MockProducer) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < int(totalPartitionsNum); i++ {
		key := fmt.Sprintf("%s-%d", topic, i)
		if _, ok := m.events[key]; !ok {
			m.events[key] = make([]*common.Message, 0)
		}
		m.events[key] = append(m.events[key], message)
	}

	return nil
}

// SyncSendMessage stores a message to a partition of the topic.
func (m *MockProducer) SyncSendMessage(_ context.Context, topic string,
	partitionNum int32, message *common.Message,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partitionNum)
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*common.Message, 0)
	}
	m.events[key] = append(m.events[key], message)

	return nil
}
