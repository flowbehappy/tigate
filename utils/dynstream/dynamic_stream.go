package dynstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/pkg/common/compare"
)

type HandlerId string

type Event interface {
	HandlerId() HandlerId
	Weight() int
}

type EventStat[T Event] struct {
	Event T

	InQueueTime time.Time
	StartTime   time.Time
	DoneTime    atomic.Value // Done time is set by worker goroutine and used by bakcground goroutine.
}

func (e *EventStat[T]) handleTime(now time.Time) time.Duration {
	doneTime := e.DoneTime.Load().(time.Time)
	if doneTime.IsZero() {
		// The event is not done yet, use the current time.
		// Note that we need to count in the event being handled.
		doneTime = now
	}
	return doneTime.Sub(e.StartTime)
}

func (e *EventStat[T]) waitTime() time.Duration {
	return e.StartTime.Sub(e.InQueueTime)
}

type Handler[T Event] interface {
	Handle(events []T)
}

type Batcher[T Event] interface {
	IsBatch(batch []*EventStat[T], next *EventStat[T]) bool
}

type handlerStat[T Event] struct {
	handler      Handler[T]
	inQueueCount atomic.Int32
}

type statusType int

const (
	normal statusType = iota
	slow
	idle
)

type streamStatus[T Event] struct {
	stream *stream[T]
	status statusType

	slowHandlers []HandlerId
}

type eventBatch[T Event] []*EventStat[T]

type streamStat struct {
	avg           time.Duration
	countOn       int64
	queueLen      int64
	estimatedWait time.Duration
	acutalWait    time.Duration
	stuck         bool
}

type stream[T Event] struct {
	id uint64

	zero T

	worker    Handler[T]
	batcher   Batcher[T]
	batchSize int

	reportInterval time.Duration
	stuckTime      time.Duration
	statMap        map[HandlerId]*handlerStat[T]

	incame     int // How many events are put into the stream.
	inChan     chan T
	waitQueue  *Deque[*EventStat[T]]
	handleChan chan []*EventStat[T]

	batch eventBatch[T]

	latestStat *RingBuffer[*EventStat[T]]
	reportChan chan streamStat

	ready atomic.Bool

	formerStreams []*(stream[T])

	remainingEvents []T

	hasDone sync.WaitGroup
}

func newStream[T Event](
	id uint64,
	acceptedIds []HandlerId,

	worker Handler[T],
	batcher Batcher[T],
	batchSize int, // 256?
	reportInterval time.Duration, // 1 second
	stuckTime time.Duration, // 500 milliseconds
	reportChan chan streamStat,

	formerStreams ...*stream[T]) *stream[T] {
	s := &stream[T]{
		worker:         worker,
		batcher:        batcher,
		batchSize:      batchSize,
		reportInterval: reportInterval,
		stuckTime:      stuckTime,
		statMap:        make(map[HandlerId]*handlerStat[T], len(acceptedIds)),
		inChan:         make(chan T, 64),
		waitQueue:      NewDequeDefault[*EventStat[T]](),
		handleChan:     make(chan []*EventStat[T]), // No need to buffer this channel.
		latestStat:     NewRingBuffer[*EventStat[T]](16),
		reportChan:     reportChan,
		formerStreams:  formerStreams,
	}

	for _, id := range acceptedIds {
		s.statMap[id] = &handlerStat[T]{
			handler: worker,
			// latest:  NewBoundedRingBuffer[T](checkLastSize),
		}
	}
	return s
}

func (s *stream[T]) readyChan() chan T {
	if !s.ready.Load() {
		return nil
	}
	return s.inChan
}

func (s *stream[T]) wait() ([]T, chan T) {
	s.hasDone.Wait()
	return s.remainingEvents, s.inChan
}

func (s *stream[T]) close() {
	close(s.inChan)
}

func (s *stream[T]) reportStat() {
	itr := s.latestStat.Iterator()
	count := int64(s.latestStat.Size())
	stuck := false
	now := time.Now()
	var totalTime time.Duration
	var actualWait time.Duration
	for e, ok := itr.Next(); ok; e, ok = itr.Next() {
		totalTime += e.handleTime(now)
		actualWait = compare.Max(actualWait, e.waitTime())
	}
	if e, ok := s.latestStat.Head(); ok {
		stuck = e.handleTime(now) > s.reportInterval
	}
	avg := time.Duration(0)
	if count != 0 {
		avg = totalTime / time.Duration(count)
	}
	estimatedWait := avg
	if e, ok := s.waitQueue.Front(); ok {
		estimatedWait = now.Sub(e.InQueueTime) + avg
	}
	if e, ok := s.waitQueue.Back(); ok {
		estimatedWait = compare.Max(estimatedWait, now.Sub(e.InQueueTime)+avg*time.Duration(s.waitQueue.Length()))
	}

	// We don't what to block the report channel
	select {
	case s.reportChan <- streamStat{
		avg:           avg,
		countOn:       count,
		queueLen:      int64(s.waitQueue.Length()),
		estimatedWait: estimatedWait,
		acutalWait:    actualWait,
		stuck:         stuck,
	}:
	default:
	}
}

func (s *stream[T]) handleEventLoop() {
	for {
		events, ok := <-s.handleChan
		if !ok {
			break
		}

		pureEvents := make([]T, len(events))
		for i, e := range events {
			pureEvents[i] = e.Event
		}

		s.worker.Handle(pureEvents)

		now := time.Now()
		for _, e := range events {
			e.DoneTime.Store(now)
		}
	}
}

func (s *stream[T]) backgroundLoop() {
	pushEventToWaitQueue := func(e T) {
		event := &EventStat[T]{
			Event:       e,
			InQueueTime: time.Now(),
		}
		s.waitQueue.PushBack(event)

		s.incame++
	}

	nextReport := time.Now().Add(s.reportInterval)

	for {
		for len(s.batch) < s.batchSize {
			waitEvent, ok := s.waitQueue.Front()
			if !ok || !s.batcher.IsBatch(s.batch, waitEvent) {
				break
			}
			s.waitQueue.PopBack()
			s.batch = append(s.batch, waitEvent)
		}
		send := len(s.batch) != 0

		if send {
			select {
			case <-time.After(nextReport.Sub(time.Now())):
				s.reportStat()
				nextReport = time.Now().Add(s.reportInterval)
			case e, ok := <-s.inChan: // Listen to the new events and put them into the wait queue.
				if !ok {
					close(s.handleChan)
					return
				}
				pushEventToWaitQueue(e)
			case s.handleChan <- s.batch: // Send the batch to the worker.
				handling := s.batch
				s.batch = nil
				now := time.Now()
				for _, e := range handling {
					e.StartTime = now
					e.DoneTime.Store(time.Time{})
					e.Event = s.zero     // Clear the event to release memory.
					s.latestStat.Push(e) // Store the timing statistics.
				}
			}
		} else {
			select {
			case <-time.After(nextReport.Sub(time.Now())):
				s.reportStat()
				nextReport = time.Now().Add(s.reportInterval)
			case e, ok := <-s.inChan:
				if !ok {
					close(s.handleChan)
					return
				}
				pushEventToWaitQueue(e)
			}
		}
	}
}

func (s *stream[T]) start() {
	s.hasDone.Add(1)

	// Start worker to handle events.
	go s.handleEventLoop()

	go s.backgroundLoop()

	// Close and wait for the former streams to finish, and move the remaining events to the inChan.
	// If the close signal is received, close the inChan and wait for the worker to finish.
	go func() {
		for i := 0; i < len(s.formerStreams); i++ {
			fs := s.formerStreams[i]
			fs.close()
		}
		buf := make([]T, 0, len(s.inChan))
		// Move the former remaining events from former streams to buffer
		for i := 0; i < len(s.formerStreams); i++ {
			fs := s.formerStreams[i]
			lastEvents, c := fs.wait()
			buf = append(buf, lastEvents...)
			for e := range c {
				buf = append(buf, e)
			}
		}

		beforeClose := func(buf []T) {
			close(s.inChan)

			// Drain the remaining events in the inChan.
			remains := make([]T, 0, 1+len(s.inChan)+len(buf))

			if e := <-workerRemain; e.b {
				remains = append(remains, e.e)
			}
			for e := range s.inChan {
				remains = append(remains, e)
			}
			// Move the events in buf to unhandled.
			remains = append(remains, buf...)
			s.remainingEvents = remains

			s.hasDone.Done()
		}

		// Push the remaining events from former streams to the inChan.
		for i, e := range buf {
			select {
			case s.inChan <- e:
			case <-s.closeSignal:
				beforeClose(buf[i:])
				return
			}
		}

		// Clear the buffer to release memory.
		buf = nil

		// Everything is good, ready to accept new events.
		s.ready.Store(true)

		// Wait for the close signal.
		<-s.closeSignal

		beforeClose(nil)
	}()
}

type DynamicStream[T any] struct {
	sourceChan chan T

	workerGen WorkerGenerator[T]
	idleTime  time.Duration
	busyTime  time.Duration
	maxWorker int

	wg          sync.WaitGroup
	closeSignal chan struct{}
}

// sourceChan: the source channel to read data from
// workerGen: the worker generator to generate worker
func NewDynamicStream[T any](
	sourceChan chan T, workerGen WorkerGenerator[T],
	idleTime time.Duration, busyTime time.Duration, maxWorker int) *DynamicStream[T] {
	return &DynamicStream[T]{
		sourceChan: sourceChan,
		workerGen:  workerGen,
		idleTime:   idleTime,
		busyTime:   busyTime,
		maxWorker:  maxWorker,
	}
}

func (ds *DynamicStream[T]) Start() {
	ds.wg.Add(1)
	go ds.loop()
}

func (ds *DynamicStream[T]) Stop() {
	select {
	case ds.closeSignal <- struct{}{}:
	default:
		break
	}
	ds.wg.Wait()
}

func (ds *DynamicStream[T]) loop() {
	defer ds.wg.Done()
}
