package context

import (
	"sync"
)

var (
	instance *AppContext
	once     sync.Once
)

const (
	MessageCenter           = "MessageCenter"
	EventCollector          = "EventCollector"
	HeartbeatCollector      = "HeartbeatCollector"
	SubscriptionClient      = "SubscriptionClient"
	SchemaStore             = "SchemaStore"
	EventStore              = "EventStore"
	EventService            = "EventService"
	DispatcherDynamicStream = "DispatcherDynamicStream"
)

// Put all the global instances here.
type AppContext struct {
	id         string
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

func SetID(id string) { GetGlobalContext().id = id }
func GetID() string   { return GetGlobalContext().id }

func SetService[T any](name string, t T) { GetGlobalContext().serviceMap.Store(name, t) }
func GetService[T any](name string) T {
	v, _ := GetGlobalContext().serviceMap.Load(name)
	return v.(T)
}
