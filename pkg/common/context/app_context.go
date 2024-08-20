package context

import (
	"sync"
)

var (
	instance *AppContext
	once     sync.Once
)

const (
<<<<<<< HEAD
	MessageCenter           = "MessageCenter"
	EventCollector          = "EventCollector"
	HeartbeatCollector      = "HeartbeatCollector"
	SchemaStore             = "SchemaStore"
	EventStore              = "EventStore"
	EventService            = "EventService"
	DispatcherDynamicStream = "DispatcherDynamicStream"
=======
	MessageCenter      = "MessageCenter"
	EventCollector     = "EventCollector"
	HeartbeatCollector = "HeartbeatCollector"
	SchemaStore        = "SchemaStore"
	EventStore         = "EventStore"
	EventService       = "EventService"
>>>>>>> 6a4a533 (schemaStore, maintainer:query tables from schemastore (#210))
)

// Put all the global instances here.
type AppContext struct {
	// TODO
	serviceMap sync.Map
}

func GetGlobalContext() *AppContext {
	once.Do(func() {
		instance = &AppContext{
			// Initialize fields here
		}
	})
	return instance
}

func SetService[T any](name string, t T) { GetGlobalContext().serviceMap.Store(name, t) }
func GetService[T any](name string) T {
	v, _ := GetGlobalContext().serviceMap.Load(name)
	return v.(T)
}
