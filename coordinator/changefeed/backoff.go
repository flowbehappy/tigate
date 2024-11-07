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
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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

// feedStateManager manages the ReactorState of a changefeed
// when an error or an admin job occurs, the feedStateManager is responsible for controlling the ReactorState
type feedStateManager struct {
	shouldBeRunning bool
	// Based on shouldBeRunning = false
	// shouldBeRemoved = true means the changefeed is removed
	// shouldBeRemoved = false means the changefeed is paused
	shouldBeRemoved bool

	adminJobQueue                 []*model.AdminJob
	isRetrying                    bool
	lastErrorRetryTime            time.Time                   // time of last error for a changefeed
	lastErrorRetryCheckpointTs    model.Ts                    // checkpoint ts of last retry
	lastWarningReportCheckpointTs model.Ts                    // checkpoint ts of last warning report
	backoffInterval               time.Duration               // the interval for restarting a changefeed in 'error' state
	errBackoff                    *backoff.ExponentialBackOff // an exponential backoff for restarting a changefeed

	checkpointTs         model.Ts
	checkpointTsAdvanced time.Time

	changefeedErrorStuckDuration time.Duration
}

// NewFeedStateManager creates feedStateManager and initialize the exponential backoff
func NewFeedStateManager(changefeedErrorStuckDuration time.Duration) *feedStateManager {
	m := new(feedStateManager)

	m.errBackoff = backoff.NewExponentialBackOff()
	m.errBackoff.InitialInterval = defaultBackoffInitInterval
	m.errBackoff.MaxInterval = defaultBackoffMaxInterval
	m.errBackoff.Multiplier = defaultBackoffMultiplier
	m.errBackoff.RandomizationFactor = defaultBackoffRandomizationFactor
	// backoff will stop once the defaultBackoffMaxElapsedTime has elapsed.
	m.errBackoff.MaxElapsedTime = changefeedErrorStuckDuration
	m.changefeedErrorStuckDuration = changefeedErrorStuckDuration

	m.resetErrRetry()
	m.isRetrying = false
	return m
}

func (m *feedStateManager) shouldRetry() bool {
	// changefeed should not retry within [m.lastErrorRetryTime, m.lastErrorRetryTime + m.backoffInterval).
	return time.Since(m.lastErrorRetryTime) >= m.backoffInterval
}

func (m *feedStateManager) shouldFailWhenRetry() bool {
	// retry the changefeed
	m.backoffInterval = m.errBackoff.NextBackOff()
	// NextBackOff() will return -1 once the MaxElapsedTime has elapsed,
	// set the changefeed to failed state.
	if m.backoffInterval == m.errBackoff.Stop {
		return true
	}

	m.lastErrorRetryTime = time.Now()
	return false
}

// resetErrRetry reset the error retry related fields
func (m *feedStateManager) resetErrRetry() {
	m.errBackoff.Reset()
	m.backoffInterval = m.errBackoff.NextBackOff()
	m.lastErrorRetryTime = time.Unix(0, 0)
}

func (m *feedStateManager) Tick(resolvedTs model.Ts,
	status *model.ChangeFeedStatus, info *model.ChangeFeedInfo,
) (adminJobPending bool) {
	m.checkAndInitLastRetryCheckpointTs(status)

	if status != nil {
		if m.checkpointTs < status.CheckpointTs {
			m.checkpointTs = status.CheckpointTs
			m.checkpointTsAdvanced = time.Now()
		}
		if m.resolvedTs < resolvedTs {
			m.resolvedTs = resolvedTs
		}
		if m.checkpointTs >= m.resolvedTs {
			m.checkpointTsAdvanced = time.Now()
		}
	}

	m.shouldBeRunning = true
	defer func() {
		if !m.shouldBeRunning {
			m.cleanUp()
		}
	}()

	switch info.State {
	case model.StateUnInitialized:
		m.patchState(model.StateNormal)
		return
	case model.StateRemoved:
		m.shouldBeRunning = false
		m.shouldBeRemoved = true
		return
	case model.StateStopped, model.StateFailed, model.StateFinished:
		m.shouldBeRunning = false
		return
	case model.StatePending:
		if !m.shouldRetry() {
			m.shouldBeRunning = false
			return
		}

		if m.shouldFailWhenRetry() {
			log.Error("The changefeed won't be restarted as it has been experiencing failures for "+
				"an extended duration",
				zap.Duration("maxElapsedTime", m.errBackoff.MaxElapsedTime),
				zap.String("namespace", m.state.GetID().Namespace),
				zap.String("changefeed", m.state.GetID().ID),
				zap.Time("lastRetryTime", m.lastErrorRetryTime),
				zap.Uint64("lastRetryCheckpointTs", m.lastErrorRetryCheckpointTs),
			)
			m.shouldBeRunning = false
			m.patchState(model.StateFailed)
			return
		}

		// retry the changefeed
		m.shouldBeRunning = true
		if status != nil {
			m.lastErrorRetryCheckpointTs = m.state.GetChangefeedStatus().CheckpointTs
		}
		m.patchState(model.StateWarning)
		log.Info("changefeed retry backoff interval is elapsed,"+
			"chengefeed will be restarted",
			zap.String("namespace", m.state.GetID().Namespace),
			zap.String("changefeed", m.state.GetID().ID),
			zap.Time("lastErrorRetryTime", m.lastErrorRetryTime),
			zap.Duration("nextRetryInterval", m.backoffInterval))
	case model.StateNormal, model.StateWarning:
		m.checkAndChangeState()
		errs := m.state.TakeProcessorErrors()
		m.HandleError(errs...)
		// only handle warnings when there are no errors
		// otherwise, the warnings will cover the errors
		if len(errs) == 0 {
			// warning are come from processors' sink component
			// they ere not fatal errors, so we don't need to stop the changefeed
			warnings := m.state.TakeProcessorWarnings()
			m.HandleWarning(warnings...)
		}
	}
	return
}

func (m *feedStateManager) ShouldRunning() bool {
	return m.shouldBeRunning
}

func (m *feedStateManager) ShouldRemoved() bool {
	return m.shouldBeRemoved
}

func (m *feedStateManager) patchState(feedState model.FeedState) {
	var updateEpoch bool
	var adminJobType model.AdminJobType
	switch feedState {
	case model.StateNormal, model.StateWarning:
		adminJobType = model.AdminNone
		updateEpoch = false
	case model.StateFinished:
		adminJobType = model.AdminFinish
		updateEpoch = true
	case model.StatePending, model.StateStopped, model.StateFailed:
		adminJobType = model.AdminStop
		updateEpoch = true
	case model.StateRemoved:
		adminJobType = model.AdminRemove
		updateEpoch = true
	default:
		log.Panic("Unreachable")
	}
	epoch := uint64(0)
	if updateEpoch {
		if updateEpoch {
			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			defer cancel()
			epoch = GenerateChangefeedEpoch(ctx, m.upstream.PDClient)
		}
	}
	m.state.UpdateChangefeedState(feedState, adminJobType, epoch)
}

func (m *feedStateManager) cleanUp() {
	m.checkpointTs = 0
	m.checkpointTsAdvanced = time.Time{}
}

func (m *feedStateManager) HandleError(errs ...*model.RunningError) {
	if len(errs) == 0 {
		return
	}
	// if there are a fastFail error in errs, we can just fastFail the changefeed
	// and no need to patch other error to the changefeed info
	for _, err := range errs {
		if cerrors.IsChangefeedGCFastFailErrorCode(errors.RFCErrorCode(err.Code)) ||
			err.ShouldFailChangefeed() {
			m.state.SetError(err)
			m.shouldBeRunning = false
			m.patchState(model.StateFailed)
			return
		}
	}

	// Changing changefeed state from stopped to failed is allowed
	// but changing changefeed state from stopped to error or normal is not allowed.
	if m.state.GetChangefeedInfo() != nil && m.state.GetChangefeedInfo().State == model.StateStopped {
		log.Warn("changefeed is stopped, ignore errors",
			zap.String("namespace", m.state.GetID().Namespace),
			zap.String("changefeed", m.state.GetID().ID),
			zap.Any("errors", errs))
		return
	}

	var lastError *model.RunningError
	// find the last non nil error
	// BTW, there shouldn't be any nil error in errs
	// this is just a safe guard
	for i := len(errs) - 1; i >= 0; i-- {
		if errs[i] != nil {
			lastError = errs[i]
			break
		}
	}
	// if any error is occurred in this tick, we should set the changefeed state to warning
	// and stop the changefeed
	if lastError != nil {
		log.Warn("changefeed meets an error", zap.Any("error", lastError))
		m.shouldBeRunning = false
		m.patchState(model.StatePending)

		// patch the last error to changefeed info
		m.state.SetError(lastError)

		// The errBackoff needs to be reset before the first retry.
		if !m.isRetrying {
			m.resetErrRetry()
			m.isRetrying = true
		}
	}
}

// checkAndInitLastRetryCheckpointTs checks the lastRetryCheckpointTs and init it if needed.
// It the owner is changed, the lastRetryCheckpointTs will be reset to 0, and we should init
// it to the checkpointTs of the changefeed when the changefeed is ticked at the first time.
func (m *feedStateManager) checkAndInitLastRetryCheckpointTs(status *model.ChangeFeedStatus) {
	if status == nil || m.lastErrorRetryCheckpointTs != 0 {
		return
	}
	m.lastWarningReportCheckpointTs = status.CheckpointTs
	m.lastErrorRetryCheckpointTs = status.CheckpointTs
	log.Info("init lastRetryCheckpointTs", zap.Uint64("lastRetryCheckpointTs", m.lastErrorRetryCheckpointTs))
}
