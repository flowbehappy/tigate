package messaging

// A topic identifies a message target.
const (
	// EventServiceTopic is the topic of the event service.
	EventServiceTopic = "EventServiceTopic"
	// EventStoreTopic is the topic of the event store
	EventStoreTopic = "event-store"
	// LogCoordinatorTopic is the topic of the log coordinator
	LogCoordinatorTopic = "log-coordinator"
	// EventCollectorTopic is the topic of the event collector.
	EventCollectorTopic = "event-collector"
	// CoordinatorTopic is the topic of the coordinator.
	CoordinatorTopic = "coordinator"
	// MaintainerManagerTopic is the topic of the maintainer manager.
	MaintainerManagerTopic = "maintainer-manager"
	// MaintainerTopic is the topic of the maintainer.
	MaintainerTopic = "maintainer"
	// HeartbeatCollectorTopic is the topic of the heartbeat collector.
	HeartbeatCollectorTopic = "heartbeat-collector"
	// DispatcherManagerTopic is the topic of the dispatcher manager.
	DispatcherManagerManagerTopic = "dispatcher-manager-manager"
)
