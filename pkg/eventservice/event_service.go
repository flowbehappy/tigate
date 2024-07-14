package eventservice

import (
	"context"
	"sync/atomic"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/google/uuid"
)

const (
	defaultScanCount = 1024
)

type GlobalID uuid.UUID

func (id GlobalID) String() string {
	return uuid.UUID(id).String()
}

type EventAcceptor interface {
	// GetID returns the ID of the acceptor.
	GetID() GlobalID
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64
	GetTopic() string
	GetServerID() GlobalID
	GetTableSpan() *common.TableSpan
	GetStartTs() common.Ts
}

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type EventService interface {
	//TODO: Be careful that the a same acceptor may be registered multiple times, think about how to handle this case.
	RegisterAcceptor(acceptor EventAcceptor) error
	DeregisterAcceptor(clusterID uint64, accepterID GlobalID) error
}

type TableSpanStat interface {
	GetTableSpan() common.TableSpan
	UpdateWatermark(watermark common.Ts)
}

// Store the progress of the acceptor, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this acceptor.
type acceptorStat struct {
	acceptor EventAcceptor
	// The watermark of the events that have been sent to the acceptor.
	watermark atomic.Uint64
}

// tableSpanStat store the latest progress of the table span in the event store.
// logService has to provide a way to get the latest progress of the table span.
type tableSpanStat struct {
	span      *common.TableSpan
	acceptors map[GlobalID]*acceptorStat
	// The watermark of the events that have been stored in the event store.
	watermark atomic.Uint64
	notify    chan struct{}
}

func (s *tableSpanStat) GetTableSpan() *common.TableSpan {
	return s.span
}

// UpdateWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (s *tableSpanStat) UpdateWatermark(watermark common.Ts) {
	if uint64(watermark) > s.watermark.Load() {
		s.watermark.Store(uint64(watermark))
	}
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// getScanTaskStartTs calculates the startTs of the table span.
// The startTs of the table span is the minimum of the watermarks of all the acceptors.
func (s *tableSpanStat) getScanTaskStartTs() uint64 {
	startTs := s.watermark.Load()
	for _, ac := range s.acceptors {
		ts := ac.watermark.Load()
		if ts < startTs {
			startTs = ts
		}
	}
	return startTs
}

type singleScanTask struct {
	clusterID uint64
	span      *common.TableSpan
	startTs   common.Ts
	endTs     common.Ts
	// The number of events to scan, 0 means no limit.
	count int
}

type batchScanTask struct {
	tasks []*singleScanTask
}

type cluster struct {
	id        uint64
	spans     map[*common.TableSpan]*tableSpanStat
	acceptors map[GlobalID]*acceptorStat
	mc        messaging.MessageCenter
	// changedSpanCh is used to notify some tableSpan has new events.
	changedSpanCh     chan *common.TableSpan
	taskPool          *scanTaskPool
	pushEventTaskPool *pushEventTaskPool
}

func (c *cluster) runGenerateScanTask(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case span := <-c.changedSpanCh:
				stat := c.spans[span]
				// The span may be deleted. In such case, we just the stale notification.
				if stat == nil {
					continue
				}
				startTs := stat.getScanTaskStartTs()
				task := &singleScanTask{
					clusterID: c.id,
					span:      span,
					startTs:   common.Ts(startTs),
					endTs:     common.Ts(stat.watermark.Load()),
					count:     defaultScanCount,
				}
				c.taskPool.addTask(task)
			}
		}
	}()
}

type scanTaskPool struct {
	tasks []*singleScanTask
}

// addTask adds a task to the pool, and merge the task if the task is overlapped with the existing tasks.
func (p *scanTaskPool) addTask(task *singleScanTask) {
	p.tasks = append(p.tasks, task)
}

type pushEventTaskPool struct {
	pendingTasks chan *batchScanTask
	workerPool   *threadpool.TaskScheduler
}

func (p *pushEventTaskPool) addTask(task *batchScanTask) {
	p.pendingTasks <- task
}

func (p *pushEventTaskPool) run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case task := <-p.pendingTasks:
				eventBuf := make([]*common.TxnEvent, 0, 1024)
				go func() {
				}()
			}
		}
	}()
}

type eventService struct {
	messageSender messaging.MessageSender
	cluster       map[uint64]cluster
	workerPool    *threadpool.TaskSchedulerInstance
}

func NewEventService(sender messaging.MessageSender) EventService {
	return &eventService{
		messageSender: sender,
		cluster:       make(map[uint64]cluster),
	}
}

func (s *eventService) RegisterAcceptor(acceptor EventAcceptor) error {
	clusterID := acceptor.GetClusterID()
	startTs := acceptor.GetStartTs()
	span := acceptor.GetTableSpan()

	c, ok := s.cluster[clusterID]
	if !ok {
		c = cluster{
			id:        clusterID,
			acceptors: make(map[GlobalID]*acceptorStat),
		}
		s.cluster[clusterID] = c
	}
	// add the acceptor to the cluster.
	ac := &acceptorStat{
		acceptor: acceptor,
	}
	ac.watermark.Store(uint64(startTs))
	c.acceptors[acceptor.GetID()] = ac

	// add the acceptor to the table span it wants to listen.
	stat, ok := c.spans[span]
	if !ok {
		stat = &tableSpanStat{
			span:      acceptor.GetTableSpan(),
			acceptors: make(map[GlobalID]*acceptorStat),
		}
		c.spans[span] = stat
	}
	stat.acceptors[acceptor.GetID()] = ac
	return nil
}

func (s *eventService) DeregisterAcceptor(clusterID uint64, accepterID GlobalID) error {
	c, ok := s.cluster[clusterID]
	if !ok {
		return nil
	}
	_, ok = c.acceptors[accepterID]
	if !ok {
		return nil
	}
	//TODO: release the resources of the acceptor.
	delete(c.acceptors, accepterID)
	return nil
}
