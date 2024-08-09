package messaging

// A topic identifies a message target.
const (
	// EventServiceTopic is the topic of the event service.
	EventServiceTopic = "EventServiceTopic"

	// EventCollectorTopic is the topic name for event collector to receive all messages from event service
	EventCollectorTopic = "event-collector"

	// CoordinatorTopic is the topic name for coordinator to receive all messages from maintainer
	CoordinatorTopic = "coordinator"

	// MaintainerManagerTopic is the topic name for manager receive all messages from
	MaintainerManagerTopic = "maintainer-manager"

	MaintainerTopic = "maintainer"

	// HeartbeatCollectorTopic is the topic name for dispatcher manager receive scheduler message
	HeartbeatCollectorTopic = "heartbeat-collector"

	// DispatcherManagerTopic is the topic name for dispatcher manager manager to receive all messages from maintainers
	DispatcherManagerManagerTopic = "dispatcher-manager-manager"
)
