package supervisedtree

import (
	"time"
)

// We need to consider the issues in the real world, such as
//   - network latency
//   - the clock skew between different nodes
//   - the time to process on the road between different nodes
//
// The basic idea:
//   - A inferior adds the local time (requestTime) to the token request
//   - The superior responds with a lifeSpan and the requestTime
//   - The TTL = requestTime + lifeSpan
//
// Note that we don't consider the lag between the requestTime and the responseTime,
// because we can't get the accurate responseTime due to the clock skew and network latency between inferior and superior.
// It makes the result TTL a little bit earier than the actual TTL, but it's acceptable and safer.
type AliveToken struct {
	// The time when the token will expire. All behaviors which rely on the TTL
	// that are not completed before this time will be considered as failed, and
	// should be suspended immediately.
	TTL time.Time
}

// TODO: some helper functions

func (t *AliveToken) IsExpired() bool {
	return time.Now().After(t.TTL)
}
