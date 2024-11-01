package dispatchermanager

import (
	"sync"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/assert"
)

func TestNewMemoryController(t *testing.T) {
	mc := NewMemoryController(1000)
	assert.Equal(t, int64(1000), mc.totalMemory)
	assert.Equal(t, int64(1000), mc.AvailableMemory())
}

func TestRegisterEvent(t *testing.T) {
	mc := NewMemoryController(1000)
	dispatcherID := common.NewDispatcherID()

	// Mock DMLEvent
	dmlEvent := &event.DMLEvent{}
	dmlEvent.ApproximateSize = 500

	// Test successful registration
	success := mc.RegisterEvent(dispatcherID, dmlEvent)
	assert.True(t, success)
	assert.Equal(t, int64(500), mc.AvailableMemory())

	// Test registration when memory is not enough
	bigEvent := &event.DMLEvent{}
	bigEvent.ApproximateSize = 600
	success = mc.RegisterEvent(dispatcherID, bigEvent)
	assert.False(t, success)
	assert.Equal(t, int64(500), mc.AvailableMemory())

	// Test post flush function
	dmlEvent.PostFlush()
	assert.Equal(t, int64(1000), mc.AvailableMemory())
}

func TestReleaseDispatcher(t *testing.T) {
	mc := NewMemoryController(1000)
	dispatcherID := common.NewDispatcherID()

	// Register some events
	dmlEvent1 := &event.DMLEvent{}
	dmlEvent1.ApproximateSize = 300
	mc.RegisterEvent(dispatcherID, dmlEvent1)

	dmlEvent2 := &event.DMLEvent{}
	dmlEvent2.ApproximateSize = 200
	mc.RegisterEvent(dispatcherID, dmlEvent2)

	assert.Equal(t, int64(500), mc.AvailableMemory())

	// Release the dispatcher
	mc.ReleaseDispatcher(dispatcherID)
	assert.Equal(t, int64(1000), mc.AvailableMemory())

	// Releasing a non-existent dispatcher should not affect the available memory
	mc.ReleaseDispatcher(common.NewDispatcherID())
	assert.Equal(t, int64(1000), mc.AvailableMemory())
}

func TestConcurrentOperations(t *testing.T) {
	mc := NewMemoryController(10000)
	numGoroutines := 100
	eventsPerGoroutine := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			dispatcherID := common.NewDispatcherID()

			for j := 0; j < eventsPerGoroutine; j++ {
				dmlEvent := &event.DMLEvent{}
				dmlEvent.ApproximateSize = 10
				mc.RegisterEvent(dispatcherID, dmlEvent)
			}

			mc.ReleaseDispatcher(dispatcherID)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int64(10000), mc.AvailableMemory())
}

func TestAvailable(t *testing.T) {
	mc := NewMemoryController(1000)
	assert.True(t, mc.Available())

	mc.availableMemory.Store(mc.totalMemory / 5)
	assert.False(t, mc.Available())
}
