package context

import (
	"sync"

	"github.com/flowbehappy/tigate/eventservice"
)

var (
	instance *AppContext
	once     sync.Once
)

// Put all the global instances here.
type AppContext struct {
	eventService *eventservice.EventService
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

func SetEventService(s *eventservice.EventService) { GetGlobalContext().eventService = s }
func GetEventService() *eventservice.EventService  { return GetGlobalContext().eventService }

func SetService[T any](name string, t T) { GetGlobalContext().serviceMap.Store(name, t) }
func GetService[T any](name string) T {
	v, _ := GetGlobalContext().serviceMap.Load(name)
	return v.(T)
}
