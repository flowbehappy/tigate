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
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/utils"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type FakeDispatcherManagerManager struct {
	msgLock sync.RWMutex
	msgBuf  []*messaging.TargetMessage

	dispatcherManagers map[model.ChangeFeedID]*DispatcherManager
}

// test only
func NewFakeMaintainerManager() *FakeDispatcherManagerManager {
	m := &FakeDispatcherManagerManager{
		dispatcherManagers: make(map[model.ChangeFeedID]*DispatcherManager),
	}
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).RegisterHandler(messaging.DispatcherManagerManagerTopic,
		func(_ context.Context, msg *messaging.TargetMessage) error {
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
					req := msg.Message.(*heartbeatpb.MaintainerBootstrapRequest)
					cfID := model.DefaultChangeFeedID(req.ChangefeedID)
					manager, ok := m.dispatcherManagers[cfID]
					if !ok {
						manager = NewDispatcherManager(cfID, msg.From)
						m.dispatcherManagers[cfID] = manager
					}
					manager.maintainerID = msg.From
					response := &heartbeatpb.MaintainerBootstrapResponse{
						Statuses: make([]*heartbeatpb.TableSpanStatus, 0, manager.dispatchers.Len()),
					}
					manager.dispatchers.Ascend(func(key *common.TableSpan, value *Dispatcher) bool {
						response.Statuses = append(response.Statuses, &heartbeatpb.TableSpanStatus{
							Span: &heartbeatpb.TableSpan{
								TableID:  value.ID.TableID,
								StartKey: value.ID.StartKey,
								EndKey:   value.ID.EndKey,
							},
							ComponentStatus: value.state,
							CheckpointTs:    0,
						})
						return true
					})
					err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewTargetMessage(
						manager.maintainerID,
						string("maintainer/"+manager.id.ID),
						response,
					))
					if err != nil {
						log.Warn("send command failed", zap.Error(err))
					}
					log.Info("bootstrap maintainer",
						zap.String("changefeed", manager.id.ID),
						zap.String("maintainer node", msg.From.String()))
				case messaging.TypeScheduleDispatcherRequest:
					req := msg.Message.(*heartbeatpb.ScheduleDispatcherRequest)
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
					absentSpan := manager.handleDispatchTableSpanRequest(req)
					if absentSpan != nil {
						err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewTargetMessage(
							manager.maintainerID,
							string("maintainer/"+manager.id.ID),
							&heartbeatpb.HeartBeatResponse{
								ChangefeedID: manager.id.ID,
								Info: []*heartbeatpb.TableProgressInfo{
									{
										Span:            absentSpan.TableSpan,
										SchedulerStatus: heartbeatpb.ComponentState_Absent,
									},
								},
							},
						))
						if err != nil {
							log.Warn("send command failed", zap.Error(err))
						}
					}
				}
			}
			for _, manager := range m.dispatcherManagers {
				// send heartbeats
				if manager.maintainerID != "" {
					response := &heartbeatpb.HeartBeatRequest{
						ChangefeedID:    manager.id.ID,
						Watermark:       &heartbeatpb.Watermark{CheckpointTs: 0},
						Statuses:        make([]*heartbeatpb.TableSpanStatus, 0, manager.dispatchers.Len()),
						CompeleteStatus: false,
					}
					manager.dispatchers.Ascend(func(key *common.TableSpan, value *Dispatcher) bool {
						if time.Since(value.lastReportTime) > time.Second {
							response.Statuses = append(response.Statuses, &heartbeatpb.TableSpanStatus{
								Span: &heartbeatpb.TableSpan{
									TableID:  value.ID.TableID,
									StartKey: value.ID.StartKey,
									EndKey:   value.ID.EndKey,
								},
								ComponentStatus: value.state,
							})
							value.lastReportTime = time.Now()
						}
						return true
					})
					if len(response.Statuses) != 0 {
						err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(messaging.NewTargetMessage(
							manager.maintainerID,
							string("maintainer/"+manager.id.ID),
							response,
						))
						if err != nil {
							log.Warn("send command failed", zap.Error(err))
						}
					}
				}

				// must clean closed dispatchers
				manager.dispatchers.Ascend(func(key *common.TableSpan, value *Dispatcher) bool {
					if value.removing.Load() && value.state == heartbeatpb.ComponentState_Stopped {
						manager.dispatchers.Delete(key)
					}
					return true
				})
			}
		}
	}
}

// Close closes, it's a block call
func (m *FakeDispatcherManagerManager) Close(ctx context.Context) error {
	return nil
}

type DispatcherManager struct {
	id model.ChangeFeedID

	maintainerID messaging.ServerId
	dispatchers  utils.Map[*common.TableSpan, *Dispatcher]
}

func NewDispatcherManager(id model.ChangeFeedID,
	maintainerID messaging.ServerId) *DispatcherManager {
	return &DispatcherManager{
		id:           id,
		maintainerID: maintainerID,
		dispatchers:  utils.NewBtreeMap[*common.TableSpan, *Dispatcher](),
	}
}

func (m *DispatcherManager) handleDispatchTableSpanRequest(
	request *heartbeatpb.ScheduleDispatcherRequest,
) *common.TableSpan {
	tableSpan := &common.TableSpan{
		TableSpan: request.GetConfig().Span,
	}
	if request.GetScheduleAction() == heartbeatpb.ScheduleAction_Create {
		span, ok := m.dispatchers.Get(tableSpan)
		if !ok {
			span = NewDispatcher(m.id, tableSpan, request.GetIsSecondary())
			m.dispatchers.ReplaceOrInsert(tableSpan, span)
			//threadpool.GetTaskSchedulerInstance().MaintainerTaskScheduler.Submit(span, threadpool.CPUTask, time.Now())
		}
		span.removing.Store(false)
		span.isSecondary.Store(request.IsSecondary)
		if !request.IsSecondary {
			span.state = heartbeatpb.ComponentState_Working
		}
	} else {
		span, ok := m.dispatchers.Get(tableSpan)
		if !ok {
			log.Warn("ignore remove span request, "+
				"since the span not found",
				zap.Any("span", span),
				zap.Any("request", request))
			return tableSpan
		}
		span.removing.Store(true)
	}
	return nil
}

type Dispatcher struct {
	ID    *common.TableSpan
	cfID  model.ChangeFeedID
	state heartbeatpb.ComponentState

	removing       *atomic.Bool
	isSecondary    *atomic.Bool
	lastReportTime time.Time
}

func NewDispatcher(cfID model.ChangeFeedID, ID *common.TableSpan, isSecondary bool) *Dispatcher {
	d := &Dispatcher{
		cfID:        cfID,
		ID:          ID,
		removing:    atomic.NewBool(false),
		state:       heartbeatpb.ComponentState_Prepared,
		isSecondary: atomic.NewBool(isSecondary),
	}
	if !isSecondary {
		d.state = heartbeatpb.ComponentState_Working
	}
	return d
}

func (d *Dispatcher) Execute() (threadpool.TaskStatus, time.Time) {
	// removing, cancel the task
	if d.removing.Load() {
		d.state = heartbeatpb.ComponentState_Stopped
		return threadpool.Done, time.Time{}
	}
	// not on the primary status, skip running
	if d.isSecondary.Load() {
		return threadpool.CPUTask, time.Now().Add(500 * time.Millisecond)
	}
	d.state = heartbeatpb.ComponentState_Working
	//todo: handle messages
	return threadpool.CPUTask, time.Now().Add(500 * time.Millisecond)
}
