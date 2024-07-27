package dynstream

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/utils/deque"
	"github.com/flowbehappy/tigate/utils/ringbuffer"
)

type batch[T Event, D any] struct {
	events []*EventWrap[T, D]

	startTime time.Time
	doneTime  time.Time
}

func (b *batch[T, D]) isRunning() ([]Path, bool) {
	if !b.doneTime.IsZero() {
		return nil, false
	}
	paths := make([]Path, len(b.events))
	for i, e := range b.events {
		paths[i] = e.Path()
	}
	return paths, true
}

// Calculate the time to handle the batch.
func (b *batch[T, D]) handleTime(now time.Time) (runTime time.Duration, running bool) {
	doneTime := b.doneTime
	if doneTime.IsZero() {
		// The batch is not done yet, use the current time.
		doneTime = now
		running = true
	} else {
		running = false
	}
	runTime = doneTime.Sub(b.startTime)
	return runTime, running
}

type pathStat struct {
	path      Path
	totalTime time.Duration
	waitLen   int64
}

type streamStat[D any] struct {
	id uint64

	avg           time.Duration // The average time to handle an event, based on the latest handled events.
	countOn       int64         // The number of latest events, including the event being handled.
	queueLen      int64         // The length of the wait queue.
	estimatedWait time.Duration // The longgest estimated wait time of all waiting events.
	actualWait    time.Duration // The actual wait time of the latest handled events.

	pathStats []pathStat // Sorted by the total time in descending order.
}

type stream[T Event, D any] struct {
	id uint64

	zeroT T

	worker    Handler[T, D]
	batcher   Batcher[T, D]
	batchSize int

	reportInterval time.Duration

	// The statistics of the paths.
	// Keep in mind that the pathInfo instance are shared in the pathMap here, the pathMap in DynamicStream,
	// and the pathInfo in the EventWrap. They are all reference to the same instances.
	// It is designed to avoid the frequent mapping by path in different places.
	pathMap map[Path]*pathInfo[T, D]

	incame int // How many events are put into the stream.

	inChan     chan *EventWrap[T, D]          // The buffer channel to receive the events.
	waitQueue  *deque.Deque[*EventWrap[T, D]] // The queue to store the waiting events.
	handleChan chan *batch[T, D]              // A synchronized channel to send the batch to the worker. We only handle one batch at a time.
	doneChan   chan *batch[T, D]              // A buffer channel to receive the done batch.

	batchEvents []*EventWrap[T, D]

	latestHandled *ringbuffer.RingBuffer[*batch[T, D]]
	reportChan    chan *streamStat[D]

	formerStreams []*(stream[T, D])

	hasClosed atomic.Bool

	handleDone     sync.WaitGroup
	backgroundDone sync.WaitGroup
}

func newStream[T Event, D any](
	id uint64,
	acceptedPaths []*pathInfo[T, D],

	worker Handler[T, D],
	batcher Batcher[T, D],
	batchSize int, // 256?
	reportInterval time.Duration, // 200 milliseconds?
	reportChan chan *streamStat[D],

	formerStreams ...*stream[T, D]) *stream[T, D] {
	s := &stream[T, D]{
		worker:         worker,
		batcher:        batcher,
		batchSize:      batchSize,
		reportInterval: reportInterval,
		pathMap:        make(map[Path]*pathInfo[T, D], len(acceptedPaths)),
		inChan:         make(chan *EventWrap[T, D], 64),
		waitQueue:      deque.NewDequeDefault[*EventWrap[T, D]](),
		handleChan:     make(chan *batch[T, D]),
		doneChan:       make(chan *batch[T, D], 8),
		latestHandled:  ringbuffer.NewRingBuffer[*batch[T, D]](8),
		reportChan:     reportChan,
		formerStreams:  formerStreams,
	}

	for _, p := range acceptedPaths {
		s.pathMap[p.path] = p
	}

	return s
}

func (s *stream[T, D]) start() {
	// Start worker to handle events.
	s.handleDone.Add(1)
	go s.handleEventLoop()

	// Start manaing events and statistics.
	s.backgroundDone.Add(1)
	go s.backgroundLoop()
}

func (s *stream[T, D]) in() chan *EventWrap[T, D] {
	return s.inChan
}

func (s *stream[T, D]) wait() {
	s.handleDone.Wait()
	return
}

// Close the stream and return the paths of the running events.
func (s *stream[T, D]) close() ([]Path, bool) {
	if s.hasClosed.CompareAndSwap(false, true) {
		close(s.inChan)
	}
	s.backgroundDone.Wait()
	if batch, ok := s.latestHandled.Tail(); ok {
		return batch.isRunning()
	}
	return nil, false
}

func (s *stream[T, D]) handleEventLoop() {
	defer func() {
		close(s.doneChan)
		s.handleDone.Done()
	}()

	for {
		batch, ok := <-s.handleChan
		if !ok {
			break
		}

		s.worker.Handle(batch.events)

		s.doneChan <- batch
	}
}

func (s *stream[T, D]) backgroundLoop() {
	defer func() {
		close(s.handleChan)
		// Move all events in the inChan to the waitQueue.
		now := time.Now()
		for e := range s.inChan {
			e.inQueueTime = now
			s.waitQueue.PushBack(e)
		}

		// Drop the batches. This stream is closing, no need to report the statistics.
		// for range s.doneChan {}

		s.backgroundDone.Done()
	}()

	// Move the remaining events in the former streams to this stream.

	for _, stream := range s.formerStreams {
		for _, e := range stream.batchEvents {
			if _, ok := s.pathMap[e.Path()]; ok {
				s.waitQueue.PushBack(e)
			}
		}
		itr := stream.waitQueue.ForwardIterator()
		for e, ok := itr.Next(); ok; e, ok = itr.Next() {
			if _, ok := s.pathMap[e.Path()]; ok {
				s.waitQueue.PushBack(e)
			}
		}

		// It is guranteed that the events in stream.inChan are already moved to the waitQueue after stream.close() return.
		// So we don't need to do it here.
	}

	// Some former streams might still be running.
	// We need to wait for the stream we are interested in to finish.
	for _, stream := range s.formerStreams {
		paths, running := stream.close()
		if !running {
			continue
		}
		for _, path := range paths {
			if _, ok := s.pathMap[path]; ok {
				stream.wait()
			}
		}
	}

	pushEventToWaitQueue := func(e *EventWrap[T, D]) {
		s.waitQueue.PushBack(e)
		e.inQueueTime = time.Now()
		e.pathInfo.waitLen++
		s.incame++
	}

	afterDone := func(batch *batch[T, D]) {
		now := time.Now()
		batch.doneTime = now

		len := len(batch.events)
		for _, e := range batch.events {
			// Note that they share the handle time, so we need to divide the time by the number of events.
			e.pathInfo.totalTime += now.Sub(batch.startTime) / time.Duration(len)

			// Clear the reference to release memory.
			// We don't need them anymore. But the batch is still in the ring buffer.
			e.event = s.zeroT
			e.pathInfo = nil
		}
	}

	nextReport := time.Now().Add(s.reportInterval)

	for {
		for len(s.batchEvents) < s.batchSize {
			waitEvent, ok := s.waitQueue.Front()
			if !ok || !s.batcher.IsBatch(s.batchEvents, waitEvent) {
				break
			}
			s.waitQueue.PopBack()
			s.batchEvents = append(s.batchEvents, waitEvent)
		}
		send := len(s.batchEvents) != 0

		if send {
			batch := &batch[T, D]{events: s.batchEvents}
			select {
			case <-time.After(nextReport.Sub(time.Now())):
				s.reportStat()
				nextReport = time.Now().Add(s.reportInterval)
			case e, ok := <-s.inChan: // Listen to the new events and put them into the wait queue.
				if !ok {
					return
				}
				pushEventToWaitQueue(e)
			case done, ok := <-s.doneChan: // Receive the done batch and update the statistics.
				if !ok {
					return
				}
				afterDone(done)
			case s.handleChan <- batch: // Send the batch to the worker.
				s.batchEvents = nil

				batch.startTime = time.Now()
				s.latestHandled.PushTail(batch) // Store the timing statistics.
				for _, e := range batch.events {
					e.pathInfo.waitLen--
				}
			}
		} else {
			select {
			case <-time.After(nextReport.Sub(time.Now())):
				s.reportStat()
				nextReport = time.Now().Add(s.reportInterval)
			case e, ok := <-s.inChan:
				if !ok {
					return
				}
				pushEventToWaitQueue(e)
			case done, ok := <-s.doneChan:
				if !ok {
					return
				}
				afterDone(done)
			}
		}
	}
}

func (s *stream[T, D]) reportStat() {
	count := int64(s.latestHandled.Size())
	now := time.Now()
	var totalTime time.Duration
	var actualWait time.Duration
	eventCount := 0

	itr := s.latestHandled.Iterator()
	for batch, ok := itr.Next(); ok; batch, ok = itr.Next() {
		rt, _ := batch.handleTime(now)
		totalTime += rt
		eventCount += len(batch.events)

		for _, e := range batch.events {
			actualWait = max(actualWait, batch.startTime.Sub(e.inQueueTime))
		}
	}
	avg := time.Duration(0)
	if count != 0 {
		avg = totalTime / time.Duration(count)
	}

	var runningTime time.Duration
	if batch, ok := s.latestHandled.Tail(); ok {
		if rt, running := batch.handleTime(now); running {
			runningTime = rt
		}
	}

	estimatedWait := avg
	if e, ok := s.waitQueue.Front(); ok {
		// There could be a very slow event being handled.
		estimatedWait = now.Sub(e.inQueueTime) + max(runningTime, avg)
	}
	if e, ok := s.waitQueue.Back(); ok {
		estimatedWait = max(estimatedWait, now.Sub(e.inQueueTime)+avg*time.Duration(s.waitQueue.Length()))
	}

	// We don't want to block here
	select {
	case s.reportChan <- &streamStat[D]{
		avg:           avg,
		countOn:       count,
		queueLen:      int64(s.waitQueue.Length()),
		estimatedWait: estimatedWait,
		actualWait:    actualWait,
		pathStats:     s.resetPathStat(),
	}:
	default:
	}
}

func (s *stream[T, D]) resetPathStat() []pathStat {
	pathStats := make([]pathStat, len(s.pathMap))
	// We need to add the time of the running events to the total time.
	// To make the scheduler to notice the long running events.
	if batch, ok := s.latestHandled.Tail(); ok && batch.doneTime.IsZero() {
		len := len(batch.events)
		for _, e := range batch.events {
			// It doesn't matter that we add the time to the total time earlier,
			// beside when they are done.
			// Because the total time will be reset to 0 after this call.
			e.pathInfo.totalTime += time.Now().Sub(batch.startTime) / time.Duration(len)
		}
	}

	for _, stat := range s.pathMap {
		pathStats = append(pathStats, pathStat{
			path:      stat.path,
			totalTime: stat.totalTime,
			waitLen:   stat.waitLen,
		})
		stat.totalTime = 0
	}
	// Sort the path stats by the total time in descending order.
	sort.Slice(pathStats, func(i, j int) bool {
		return pathStats[i].totalTime > pathStats[j].totalTime
	})
	return pathStats
}
