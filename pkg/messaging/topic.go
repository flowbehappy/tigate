package messaging

const (
	// EventServiceTopic is the topic of the event service.
	EventServiceTopic = "EventServiceTopic"
	EventFeedTopic    = "EventFeed"

	// CoordinatorTopic is the topic name for coordinator to receive all messages from maintainer
	CoordinatorTopic = "coordinator"

	// MaintainerManagerTopic is the topic name for manager receive all messages from
	MaintainerManagerTopic           = "maintainer-manager"
	DispatcherHeartBeatRequestTopic  = "HeartBeatRequest"
	MaintainerBootstrapResponseTopic = "MaintainerBootstrapResponse"

	// SchedulerDispatcherTopic is the topic name for dispatcher manager receive scheduler message
	SchedulerDispatcherTopic       = "SchedulerDispatcherRequest"
	MaintainerBoostrapRequestTopic = "MaintainerBoostrapRequest"
)
