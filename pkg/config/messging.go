package config

const (
	// size of channel to cache the messages to be sent and received
	defaultCacheSize = 102400
)

type MessageCenterConfig struct {
	// The size of the channel for pending messages to be sent and received.
	CacheChannelSize int
}

func NewDefaultMessageCenterConfig() *MessageCenterConfig {
	return &MessageCenterConfig{
		CacheChannelSize: defaultCacheSize,
	}
}
