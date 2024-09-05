package helper

import (
	"net/url"
	"strings"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// GetTopic returns the topic name from the sink URI.
func GetTopic(sinkURI *url.URL) (string, error) {
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return "", cerror.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}
	return topic, nil
}
