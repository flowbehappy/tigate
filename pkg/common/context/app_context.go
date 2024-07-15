package context

import (
	"sync"

	"github.com/flowbehappy/tigate/downstreamadapter"
	"github.com/flowbehappy/tigate/pkg/eventservice"
	"github.com/flowbehappy/tigate/pkg/messaging"
)

var (
	instance *AppContext
	once     sync.Once
)

const globalMemoryQuota = 100 * 1024 * 1024 * 1024 // 100GB for demo

// Put all the global instances here.
type AppContext struct {
	messageCenter      messaging.MessageCenter
	eventService       eventservice.EventService
	eventCollector     *downstreamadapter.EventCollector
	heartbeatCollector *downstreamadapter.HeartBeatCollector
	// TODO
	serviceMap sync.Map
	clusterID  messaging.ServerId
}

func GetGlobalContext() *AppContext {
	once.Do(func() {
		instance = &AppContext{
			// Initialize fields here
			messageCenter:      messageCenter,
			eventCollector:     downstreamadapter.NewEventCollector(globalMemoryQuota, clusterID, clusterID),
			heartbeatCollector: downstreamadapter.NewHeartBeatCollector(clusterID),
		}
	})
	return instance
}

func SetMessageCenter(c messaging.MessageCenter) { GetGlobalContext().messageCenter = c }
func GetMessageCenter() messaging.MessageCenter  { return GetGlobalContext().messageCenter }

func SetEventCollector(c *downstreamadapter.EventCollector) { GetGlobalContext().eventCollector = c }
func GetEventCollector() *downstreamadapter.EventCollector  { return GetGlobalContext().eventCollector }

func SetHeartBeatCollector(c *downstreamadapter.HeartBeatCollector) {
	GetGlobalContext().heartbeatCollector = c
}
func GetHeartBeatCollector() *downstreamadapter.HeartBeatCollector {
	return GetGlobalContext().heartbeatCollector
}

func SetEventService(s eventservice.EventService) { GetGlobalContext().eventService = s }
func GetEventService() eventservice.EventService  { return GetGlobalContext().eventService }

func SetService[T any](name string, t T) { GetGlobalContext().serviceMap.Store(name, t) }
func GetService[T any](name string) T {
	v, _ := GetGlobalContext().serviceMap.Load(name)
	return v.(T)
}
