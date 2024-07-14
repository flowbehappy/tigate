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

package maintainer

import (
	"context"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type FakeDispatcherManagerManager struct {
	messageCenter messaging.MessageCenter
	msgLock       sync.RWMutex
	msgBuf        []*messaging.TargetMessage

	dispatcherManagers map[model.ChangeFeedID]*DispatcherManager
}

// test only
func NewFakeMaintainerManager(messageCenter messaging.MessageCenter) *FakeDispatcherManagerManager {
	m := &FakeDispatcherManagerManager{
		messageCenter:      messageCenter,
		dispatcherManagers: make(map[model.ChangeFeedID]*DispatcherManager),
	}
	messageCenter.RegisterHandler(m.Name(), func(msg *messaging.TargetMessage) error {
		m.msgLock.Lock()
		m.msgBuf = append(m.msgBuf, msg)
		m.msgLock.Unlock()
		return nil
	})
	return m
}

func (m *FakeDispatcherManagerManager) Name() string {
	return "dispatcher-manager"
}

func (m *FakeDispatcherManagerManager) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 50)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			m.msgLock.Lock()
			buf := m.msgBuf
			m.msgBuf = nil
			m.msgLock.Unlock()

			for _, msg := range buf {
				switch msg.Type {
				case messaging.TypeMaintainerBootstrapRequest:
					req := msg.Message.(*messaging.MaintainerBootstrapRequest)
					cfID := model.DefaultChangeFeedID(req.Id)
					manager, ok := m.dispatcherManagers[cfID]
					if !ok {
						manager = NewDispatcherManager(cfID, msg.From)
						m.dispatcherManagers[cfID] = manager
					}
					manager.maintainerID = msg.From
					response := &messaging.MaintainerBootstrapResponse{
						MaintainerBootstrapResponse: &heartbeatpb.MaintainerBootstrapResponse{
							Statuses: make([]*heartbeatpb.TableSpanStatus, 0, manager.dispatchers.Len()),
						},
					}
					manager.dispatchers.Ascend(func(key *common.TableSpan, value *Dispatcher) bool {
						response.Statuses = append(response.Statuses, &heartbeatpb.TableSpanStatus{
							Span: &heartbeatpb.TableSpan{
								TableID:  value.ID.TableID,
								StartKey: value.ID.StartKey,
								EndKey:   value.ID.EndKey,
							},
							SchedulerStatus: int32(value.state),
							CheckpointTs:    0,
						})
						return true
					})
					err := m.messageCenter.SendCommand(messaging.NewTargetMessage(
						manager.maintainerID,
						"maintainer/"+manager.id.ID,
						messaging.TypeMaintainerBootstrapResponse,
						response,
					))
					if err != nil {
						log.Warn("send command failed", zap.Error(err))
					}
					log.Info("bootstrap coordinator",
						zap.String("id", msg.From.String()))
				case messaging.TypeDispatchMaintainerRequest:
					req := msg.Message.(*messaging.ScheduleDispatcherRequest)
					cfID := model.DefaultChangeFeedID(req.ChangefeedID)
					manager, ok := m.dispatcherManagers[cfID]
					if !ok {
						log.Warn("ignore invalid msg",
							zap.Any("request", req),
							zap.Any("coordinator", msg))
						continue
					}
					if manager.maintainerID != msg.From {
						log.Warn("ignore invalid maintainer id",
							zap.Any("request", req),
							zap.Any("maintainer", msg.From))
						continue
					}
					manager.handleDispatchTableSpanRequest(req)
				}
			}
			for _, manager := range m.dispatcherManagers {
				// send heartbeats
				if manager.maintainerID != "" {
					response := &messaging.HeartBeatResponse{
						HeartBeatResponse: &heartbeatpb.HeartBeatResponse{
							ChangefeedID: manager.id.ID,
							Info:         make([]*heartbeatpb.TableProgressInfo, 0, manager.dispatchers.Len()),
						},
					}
					manager.dispatchers.Ascend(func(key *common.TableSpan, value *Dispatcher) bool {
						if time.Since(value.lastReportTime) > time.Second {
							response.Info = append(response.Info, &heartbeatpb.TableProgressInfo{
								Span: &heartbeatpb.TableSpan{
									TableID:  value.ID.TableID,
									StartKey: value.ID.StartKey,
									EndKey:   value.ID.EndKey,
								},
								SchedulerStatus: int32(value.state),
							})
							value.lastReportTime = time.Now()
						}
						return true
					})
					if len(response.Info) != 0 {
						m.sendMessages(manager.maintainerID, manager.id.ID, response)
					}
				}
			}
		}
	}
}

func (m *FakeDispatcherManagerManager) sendMessages(target messaging.ServerId,
	id string,
	msg *messaging.HeartBeatResponse) {
	targetMsg := messaging.NewTargetMessage(
		target,
		"maintainer/"+id,
		messaging.TypeHeartBeatResponse,
		msg,
	)
	err := m.messageCenter.SendCommand(targetMsg)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}

// Close closes, it's a block call
func (m *FakeDispatcherManagerManager) Close(ctx context.Context) error {
	return nil
}

type DispatcherManager struct {
	id model.ChangeFeedID

	maintainerID messaging.ServerId
	dispatchers  scheduler.Map[*common.TableSpan, *Dispatcher]
}

func NewDispatcherManager(id model.ChangeFeedID,
	maintainerID messaging.ServerId) *DispatcherManager {
	return &DispatcherManager{
		id:           id,
		maintainerID: maintainerID,
		dispatchers:  scheduler.NewBtreeMap[*common.TableSpan, *Dispatcher](),
	}
}

func (m *DispatcherManager) handleDispatchTableSpanRequest(
	request *messaging.ScheduleDispatcherRequest,
) {
	tableSpan := &common.TableSpan{
		TableID:  request.GetConfig().Span.TableID,
		StartKey: request.GetConfig().Span.StartKey,
		EndKey:   request.GetConfig().Span.EndKey,
	}
	if request.GetScheduleAction() == heartbeatpb.ScheduleAction_Create {
		span, ok := m.dispatchers.Get(tableSpan)
		if !ok {
			span = NewDispatcher(request.GetIsSecondary())
			m.dispatchers.ReplaceOrInsert(tableSpan, span)
			threadpool.GetTaskSchedulerInstance().MaintainerTaskScheduler.Submit(span, threadpool.CPUTask, time.Now())
		}
		span.isSecondary.Store(request.IsSecondary)
	} else {
		span, ok := m.dispatchers.Get(tableSpan)
		if !ok {
			log.Warn("ignore remove span request, "+
				"since the span not found",
				zap.Any("span", span),
				zap.Any("request", request))
			return
		}
		span.removing.Store(true)
	}
}

type Dispatcher struct {
	taskID threadpool.TaskId
	ID     *common.TableSpan
	state  scheduler.ComponentStatus

	removing       *atomic.Bool
	isSecondary    *atomic.Bool
	lastReportTime time.Time
}

func NewDispatcher(isSecondary bool) *Dispatcher {
	d := &Dispatcher{
		taskID:      threadpool.NewGlobalTaskId(),
		removing:    atomic.NewBool(false),
		state:       scheduler.ComponentStatusPrepared,
		isSecondary: atomic.NewBool(isSecondary),
	}
	if !isSecondary {
		d.state = scheduler.ComponentStatusWorking
	}
	return d
}

func (d *Dispatcher) Execute(status threadpool.TaskStatus) (threadpool.TaskStatus, time.Time) {
	// removing, cancel the task
	if d.removing.Load() {
		d.state = scheduler.ComponentStatusStopped
		return threadpool.Done, time.Time{}
	}
	// not on the primary status, skip running
	if d.isSecondary.Load() {
		return status, time.Now().Add(500 * time.Millisecond)
	}
	d.state = scheduler.ComponentStatusWorking
	//todo: handle messages
	return status, time.Now().Add(500 * time.Millisecond)
}

func (d *Dispatcher) TaskId() threadpool.TaskId {
	return d.taskID
}
