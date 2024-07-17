package context

import (
	"sync"
)

var (
	instance *AppContext
	once     sync.Once
)

const (
	MessageCenter      = "MessageCenter"
	EventCollector     = "EventCollector"
	HeartbeatCollector = "HeartbeatCollector"
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
