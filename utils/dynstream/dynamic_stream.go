package dynstream

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowbehappy/tigate/pkg/apperror"
)

type cmdType int

const (
	// For a AddPath request, first send to distributor to check whether the path exists or not,
	// then send to the scheduler goroutine to decide the stream to add the path,
	// finally send to the distributor to add the path to the stream.
	// It is the complexity we need to pay without using a lock.
	typeAddPathCheck cmdType = iota
	typeAddPathSched
	typeAddPathDist

	typeRemovePathSched
	typeRemovePathDist
	typeMergeStream
	typeSplitStream
)

type cmd struct {
	cmdType cmdType
	command any
	done    sync.WaitGroup
	error   *apperror.AppError
}

type addPath[T Event, D any] struct {
	paths []PathAndDest[D]
}

type removePath struct {
	path []Path
}

type mergeStream struct {
}

type splitStream struct {
}

type pathInfo[T Event, D any] struct {
	// Note that although this struct is used by multiple goroutines, it doesn't need synchronization because
	// 1. path & dest are immutable.
	// 2. totalTime & waitLen are only accessed by the background goroutine in the stream.
	// 3. stream is only accessed by the distributor goroutine in DynamicStream.
	// We use one struct to store them together to avoid mapping by path in different places in many times.

	path Path
	dest D

	totalTime time.Duration // The total time to handle the events in a period of time.
	waitLen   int64         // Current waiting events count.

	stream *stream[T, D]
}

type DynamicStream[T Event, D any] struct {
	sourceChan chan T
	handler    Handler[T, D]

	expectedLatency time.Duration
	minStream       int
	reportInterval  time.Duration

	pathMap   map[Path]*pathInfo[T, D]
	streamMap map[int64]*stream[T, D]

	streamMinHeap *streamHeap
	streamMaxHeap *streamHeap
	streamStats   map[int64]*statAndIndex

	cmdChanSched chan *cmd
	cmdChanDist  chan *cmd
	reportChan   chan *streamStat

	streamIdGen atomic.Int64

	wg          sync.WaitGroup
	closeSignal chan struct{}
}

func NewDynamicStream[T Event, D any](expectedLatency time.Duration) *DynamicStream[T, D] {
	ds := &DynamicStream[T, D]{
		sourceChan: make(chan T, 1024),

		pathMap: make(map[Path]*pathInfo[T, D]),

		expectedLatency: expectedLatency,
		minStream:       runtime.NumCPU() * 2,
		reportInterval:  200 * time.Millisecond,

		closeSignal: make(chan struct{}),
	}

	ds.wg.Add(2)
	go ds.distributorLoop()
	go ds.schedulerLoop()

	return ds
}

func (ds *DynamicStream[T, D]) Send(e T) { ds.sourceChan <- e }
func (ds *DynamicStream[T, D]) Close()   { close(ds.closeSignal) }

// Wait all goroutines to exit.
// Don't guarantee the events are all handled.
func (ds *DynamicStream[T, D]) Wait() { ds.wg.Wait() }

func (ds *DynamicStream[T, D]) AddPath(path Path, dest D) error {
	return ds.AddPathBatch(PathAndDest[D]{Path: path, Dest: dest})
}
func (ds *DynamicStream[T, D]) RemovePath(path Path) error {
	return ds.RemovePathBatch(path)
}

// Returns ErrorTypeDuplicate if the path already exists.
func (ds *DynamicStream[T, D]) AddPathBatch(paths ...PathAndDest[D]) error {

	cmd := &cmd{
		cmdType: typeAddPathSched,
		cmd:     &addPath[T, D]{stream: stream, paths: pis},
	}
	cmd.done.Add(1)
	ds.cmdChanSched <- cmd
	cmd.done.Wait()
	return cmd.error
}

// Returns ErrorTypeNotExist if the path doesn't exist.
func (ds *DynamicStream[T, D]) RemovePathBatch(path ...Path) error {
	cmd := &cmd{
		cmdType: typeRemovePathSched,
		cmd:     &removePath{path: path},
	}
	cmd.done.Add(1)
	ds.cmdChanSched <- cmd
	cmd.done.Wait()
	return cmd.error
}

func (ds *DynamicStream[T, D]) distributorLoop() {
	defer func() {
		for _, pi := range ds.pathMap {
			pi.stream.close()
		}
		ds.wg.Done()
	}()

	for {
		select {
		case e := <-ds.sourceChan:
			pi, ok := ds.pathMap[e.Path()]
			if !ok {
				return
			}
			pi.stream.in() <- &eventWrap[T, D]{event: e, pathInfo: pi}
		case cmd := <-ds.cmdChan:
			switch cmd.cmdType {
			case typeAddPath:
				ds.handleAddPath(cmd)
			case typeRemovePath:
			case typeMergeStream:
			case typeSplitStream:
			default:
				panic("unknown cmd type")
			}
		case <-ds.closeSignal:
			return
		}
	}
}

func (ds *DynamicStream[T, D]) schedulerLoop() {
	defer ds.wg.Done()
	for {
		select {
		case stat := <-ds.reportChan:
			if old, ok := ds.streamStats[stat.id]; ok {
			}

		case <-ds.closeSignal:
			return
		}
	}
}

func (ds *DynamicStream[T, D]) handleAddPathCheck(c *cmd) {
	addPath := c.command.(*addPath[T, D])

	for _, path := range addPath.paths {
		if _, ok := ds.pathMap[path.Path]; ok {
			c.error = apperror.NewAppError(apperror.ErrorTypeDuplicate, fmt.Sprintf("path %s already exists", path.Path))
			c.done.Done()
			return
		}
	}

	ds.cmdChanSched <- &cmd{cmdType: typeAddPathSched, command: addPath}
}

func (ds *DynamicStream[T, D]) handleAddPathSched(cmd *cmd) {
	paths := cmd.command.(*addPath[T, D]).paths

	stream := newStream(ds.streamIdGen.Add(1), ds.expectedLatency, ds.reportInterval, ds.handler, ds.reportChan)
	pis := make([]*pathInfo[T, D], 0, len(paths))
	for _, path := range paths {
		if _, ok := ds.pathMap[path.Path]; ok {
			cmd.error = apperror.NewAppError(apperror.ErrorTypeDuplicate, fmt.Sprintf("path %s already exists", path.Path))
			cmd.done.Done()
			return
		}
		pi := &pathInfo[T, D]{path: path.Path, dest: path.Dest, stream: stream}
		pis = append(pis, pi)
	}
}
