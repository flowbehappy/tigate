// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package changefeed

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// When errors occurred, and we need to do backoff, we start an exponential backoff
	// with an interval from 10s to 30min (10s, 20s, 40s, 80s, 160s, 320s,
	//	 600s, 600s, ...).
	// To avoid thunderherd, a random factor is also added.
	defaultBackoffInitInterval        = 10 * time.Second
	defaultBackoffMaxInterval         = 10 * time.Minute
	defaultBackoffRandomizationFactor = 0.1
	defaultBackoffMultiplier          = 2.0
)

// Backoff manages the backoff of a changefeed
// when an error occurs, the Backoff is responsible for controlling the retry of the changefeed
type Backoff struct {
	id common.ChangeFeedID

	isRetrying                 bool
	lastErrorRetryTime         time.Time                   // time of last error for a changefeed
	lastErrorRetryCheckpointTs model.Ts                    // checkpoint ts of last retry
	backoffInterval            time.Duration               // the interval for restarting a changefeed in 'error' state
	errBackoff                 *backoff.ExponentialBackOff // an exponential backoff for restarting a changefeed

	checkpointTs         model.Ts
	checkpointTsAdvanced time.Time

	changefeedErrorStuckDuration time.Duration
}

// NewBackoff creates Backoff and initialize the exponential backoff
func NewBackoff(id common.ChangeFeedID, changefeedErrorStuckDuration time.Duration, checkpointTs uint64) *Backoff {
	m := &Backoff{
		id:                           id,
		errBackoff:                   backoff.NewExponentialBackOff(),
		changefeedErrorStuckDuration: changefeedErrorStuckDuration,
		isRetrying:                   false,
		lastErrorRetryTime:           time.Time{},
	}
	m.errBackoff.InitialInterval = defaultBackoffInitInterval
	m.errBackoff.MaxInterval = defaultBackoffMaxInterval
	m.errBackoff.Multiplier = defaultBackoffMultiplier
	m.errBackoff.RandomizationFactor = defaultBackoffRandomizationFactor
	// backoff will stop once the defaultBackoffMaxElapsedTime has elapsed.
	m.errBackoff.MaxElapsedTime = changefeedErrorStuckDuration
	m.lastErrorRetryCheckpointTs = checkpointTs
	log.Info("init lastRetryCheckpointTs", zap.Uint64("lastRetryCheckpointTs", m.lastErrorRetryCheckpointTs))
	m.resetErrRetry()
	return m
}

func (m *Backoff) shouldRetry() bool {
	// changefeed should not retry within [m.lastErrorRetryTime, m.lastErrorRetryTime + m.backoffInterval).
	return time.Since(m.lastErrorRetryTime) >= m.backoffInterval
}

func (m *Backoff) shouldFailWhenRetry() bool {
	// NextBackOff() will return -1 once the MaxElapsedTime has elapsed,
	// set the changefeed to failed state.
	if m.backoffInterval == m.errBackoff.Stop {
		return true
	}
	return false
}

// resetErrRetry reset the error retry related fields
func (m *Backoff) resetErrRetry() {
	m.errBackoff.Reset()
}

func (m *Backoff) CheckStatus(status *heartbeatpb.MaintainerStatus) (bool, model.FeedState, *heartbeatpb.RunningError) {
	if status == nil {
		return false, model.StateNormal, nil
	}
	if m.checkpointTs < status.CheckpointTs {
		m.checkpointTs = status.CheckpointTs
		m.checkpointTsAdvanced = time.Now()

		// the checkpointTs is advanced, we should reset the error retry
		if m.isRetrying &&
			status.CheckpointTs > m.lastErrorRetryCheckpointTs {
			log.Info("changefeed is recovered from warning state,"+
				"its checkpointTs is greater than lastRetryCheckpointTs,"+
				"it will be changed to normal state",
				zap.String("namespace", m.id.Namespace()),
				zap.String("changefeed", m.id.Name()),
				zap.Uint64("checkpointTs", status.CheckpointTs),
				zap.Uint64("lastRetryCheckpointTs", m.lastErrorRetryCheckpointTs))
			m.isRetrying = false
			// reset the retry backoff
			m.resetErrRetry()
			return true, model.StateNormal, nil
		}
	}
	// if the checkpointTs is not advanced for a long time, we should stop the changefeed
	return m.HandleError(status.Err)
}

func (m *Backoff) cleanUp() {
	m.checkpointTs = 0
	m.checkpointTsAdvanced = time.Time{}
}

// ShouldFailChangefeed return true if a running error contains a changefeed not retry error.
func ShouldFailChangefeed(e *heartbeatpb.RunningError) bool {
	return cerrors.ShouldFailChangefeed(errors.New(e.Message + e.Code))
}

func (m *Backoff) HandleError(errs []*heartbeatpb.RunningError) (bool, model.FeedState, *heartbeatpb.RunningError) {
	if len(errs) == 0 {
		return false, model.StateNormal, nil
	}
	// if there are a fastFail error in errs, we can just fastFail the changefeed
	for _, err := range errs {
		if cerrors.IsChangefeedGCFastFailErrorCode(errors.RFCErrorCode(err.Code)) ||
			ShouldFailChangefeed(err) {
			return true, model.StateFailed, err
		}
	}

	var lastError = errs[len(errs)-1]
	// if any error is occurred , we should set the changefeed state to warning and stop the changefeed
	log.Warn("changefeed meets an error, will be stopped",
		zap.Any("error", errs))

	// The errBackoff needs to be reset before the first retry.
	if !m.isRetrying {
		m.resetErrRetry()
		m.isRetrying = true
	}
	// set the next retry time
	m.backoffInterval = m.errBackoff.NextBackOff()
	m.lastErrorRetryTime = time.Now()

	// check if we exceed the maxElapsedTime
	if m.shouldFailWhenRetry() {
		log.Error("The changefeed won't be restarted as it has been experiencing failures for "+
			"an extended duration",
			zap.Duration("maxElapsedTime", m.errBackoff.MaxElapsedTime),
			zap.String("namespace", m.id.Namespace()),
			zap.String("changefeed", m.id.Name()),
			zap.Time("lastRetryTime", m.lastErrorRetryTime),
			zap.Uint64("lastRetryCheckpointTs", m.lastErrorRetryCheckpointTs),
		)
		return true, model.StateFailed, lastError
	}

	// patch the last error to changefeed info
	return true, model.StateWarning, lastError
}
