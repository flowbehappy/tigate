package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRegisterAndDeRegisterHandler(t *testing.T) {
	r := newRouter()
	testTopic := "test-topic"

	handler := func(ctx context.Context, msg *TargetMessage) error { return nil }
	r.registerHandler(testTopic, handler)

	assert.Len(t, r.handlers, 1)
	assert.Contains(t, r.handlers, testTopic)

	r.deRegisterHandler(testTopic)
	assert.Len(t, r.handlers, 0)
	assert.NotContains(t, r.handlers, testTopic)
}

type mockIOTypeT struct {
	payload []byte
}

func (m *mockIOTypeT) Unmarshal(data []byte) error {
	m.payload = data
	return nil
}

func (m *mockIOTypeT) Marshal() ([]byte, error) {
	return m.payload, nil
}

func newMockIOTypeT(payload []byte) []IOTypeT {
	return []IOTypeT{&mockIOTypeT{payload: payload}}
}

func TestRunDispatchSuccessful(t *testing.T) {
	t.Parallel()
	handledMsg := make([]*TargetMessage, 0)
	r := newRouter()
	r.registerHandler("topic1", func(ctx context.Context, msg *TargetMessage) error {
		handledMsg = append(handledMsg, msg)
		return nil
	})

	msgChan := make(chan *TargetMessage, 1)
	msgChan <- &TargetMessage{Topic: "topic1", Message: newMockIOTypeT([]byte("test"))}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	r.runDispatch(ctx, &wg, msgChan)
	wg.Wait()

	assert.Len(t, handledMsg, 1)
	assert.Equal(t, "topic1", handledMsg[0].Topic)
}

func TestRunDispatchWithError(t *testing.T) {
	t.Parallel()
	r := newRouter()
	r.registerHandler("topic1", func(ctx context.Context, msg *TargetMessage) error {
		return errors.New("handler error")
	})

	msgChan := make(chan *TargetMessage, 1)
	msgChan <- &TargetMessage{Topic: "topic1", Message: newMockIOTypeT([]byte("test"))}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	r.runDispatch(ctx, &wg, msgChan)
	wg.Wait()
}

func TestRunDispatchNoHandler(t *testing.T) {
	t.Parallel()
	r := newRouter()
	msgChan := make(chan *TargetMessage, 1)
	msgChan <- &TargetMessage{Topic: "unknown-topic", Message: newMockIOTypeT([]byte("test"))}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	r.runDispatch(ctx, &wg, msgChan)
	wg.Wait()
}

func TestConcurrentAccess(t *testing.T) {
	r := newRouter()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(2)
		topic := fmt.Sprintf("topic-%d", i)

		go func() {
			defer wg.Done()
			handler := func(ctx context.Context, msg *TargetMessage) error { return nil }
			r.registerHandler(topic, handler)
		}()

		go func() {
			defer wg.Done()
			r.deRegisterHandler(topic)
		}()
	}

	wg.Wait()
}
