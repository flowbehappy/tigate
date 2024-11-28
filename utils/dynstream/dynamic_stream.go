package dynstream

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	. "github.com/pingcap/ticdc/pkg/apperror"
	. "github.com/pingcap/ticdc/utils"
	"github.com/pingcap/ticdc/utils/deque"
)

const TrackTopPaths = 16
const BusyStreamRatio = 0.3
const BusyPathRatio = 0.1
const IdlePathRatio = 0.02

type cmdType int

const (
	typeAddPath cmdType = iota
	typeRemovePath
	typeArrangeStream
	typeReportAndSchedule // For test only
)

type ruleType int

const (
	createSoloPath ruleType = iota
	removeSoloPath
	shuffleStreams
)

type command struct {
	cmdType cmdType
	cmd     interface{}

	wg *sync.WaitGroup
}

type addPathCmd[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	settings AreaSettings

	path PathAndDest[P, D]
	pi   *pathInfo[A, P, T, D, H]
	err  error
}

type removePathCmd[P Path] struct {
	path P
	err  error
}

type arrangeStreamCmd[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	oldStreams []*stream[A, P, T, D, H]

	newStreams     []*stream[A, P, T, D, H]
	newStreamPaths [][]*pathInfo[A, P, T, D, H]
}

type reportAndScheduleCmd struct {
	rule   ruleType
	period time.Duration
}

// Use to store the statistics of a stream
// It is only used by the scheduler
type streamInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	stream     *stream[A, P, T, D, H]
	streamStat streamStat[A, P, T, D, H]
	pathMap    map[*pathInfo[A, P, T, D, H]]struct{}
}

func (si *streamInfo[A, P, T, D, H]) runtime() time.Duration {
	return si.streamStat.processingTime
}

func (si *streamInfo[A, P, T, D, H]) busyRatio(period time.Duration) float64 {
	if si.streamStat.processingTime == 0 {
		return 0
	}
	if period != 0 {
		return float64(si.streamStat.processingTime) / float64(period)
	}
	return float64(si.streamStat.processingTime) / float64(si.streamStat.elapsedTime)
}

func (si *streamInfo[A, P, T, D, H]) period() time.Duration {
	return si.streamStat.elapsedTime
}

type sortedSIs[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] []*streamInfo[A, P, T, D, H]

// implement sort.Interface
func (s sortedSIs[A, P, T, D, H]) Len() int           { return len(s) }
func (s sortedSIs[A, P, T, D, H]) Less(i, j int) bool { return s[i].runtime() < s[j].runtime() }
func (s sortedSIs[A, P, T, D, H]) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// This is the implementation of the DynamicStream interface.
// We use two goroutines
// 1. The distributor to distribute the events to the streams
// 2. The scheduler to balance the load of the streams
//
// A stream can handle events from multiple paths.
// Events from the same path are only processed by one particular stream at the same time.
// The scheduler use several strategies to balance the load of the streams, while the final balance
// actions are moving the paths between the streams.
type dynamicStreamImpl[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	trackTopPaths   int
	baseStreamCount int

	handler H // handler of the dynamic stream, all the events are finally passed to the handler
	option  Option

	memControl *memControl[A, P, T, D, H]

	// Fields if UseBuffer is true
	// inChan -> buffer -> outChan(eventChan) ->  streams
	//        ^ receiver                      ^ distributor
	bufferCount atomic.Int64
	inChan      chan T // The channel to receive the incoming events by receiver
	outChan     chan T // The channel to send the events to the streams by distributor

	wakeBufferCount atomic.Int64
	wakeInChan      chan P // The channel to receive the wake signal by receiver
	wakeOutChan     chan P // The channel to send the wake signal to the streams by distributor

	// The channel used to receive and send the events by the distributor
	// It is the same as the outChan if UseBuffer is true
	eventChan chan T
	wakeChan  chan P

	feedbackChan chan Feedback[A, P, D] // The channel to report the feedback to outside listener

	reportChan chan streamStat[A, P, T, D, H] // The channel to receive the report by scheduler
	cmdToSched chan *command                  // The channel to send the commands to the scheduler
	cmdToDist  chan *command                  // The channel to send the commands to the distributor

	// The streams to handle the events. Only used in the scheduler.
	// We put it here mainly to make the tests easier.
	streamInfos []*streamInfo[A, P, T, D, H]

	hasClosed atomic.Bool

	schedWg sync.WaitGroup
	distWg  sync.WaitGroup

	eventExtraSize int
	startTime      time.Time

	_statAllStreamPendingLen atomic.Int64
	_statMinHandledTS        atomic.Uint64
	_statAddPathCount        atomic.Uint64
	_statRemovePathCount     atomic.Uint64
	_statArrangeStreamCount  atomic.Uint64
}

func newDynamicStreamImpl[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	handler H,
	option Option,
	feedbackChan ...chan Feedback[A, P, D],
) *dynamicStreamImpl[A, P, T, D, H] {
	if unsafe.Sizeof(int(0)) != 8 {
		// We need int to be int64, because we use int as the data size everywhere.
		panic("int is not int64")
	}

	option.fix()
	eventExtraSize := 0
	var zero T
	if reflect.TypeOf(zero).Kind() == reflect.Pointer {
		eventExtraSize = int(unsafe.Sizeof(eventWrap[A, P, T, D, H]{}))
	} else {
		a := unsafe.Sizeof(eventWrap[A, P, T, D, H]{})
		b := unsafe.Sizeof(zero)
		eventExtraSize = int(a - b)
		// eventExtraSize = int(unsafe.Sizeof(eventWrap[A, P, T, D, H]{}) - unsafe.Sizeof(zero))
	}
	ds := &dynamicStreamImpl[A, P, T, D, H]{
		handler: handler,
		option:  option,

		trackTopPaths:   TrackTopPaths,
		baseStreamCount: option.StreamCount,

		reportChan: make(chan streamStat[A, P, T, D, H], 64),
		cmdToSched: make(chan *command, 64),
		cmdToDist:  make(chan *command, option.StreamCount),

		streamInfos:    make([]*streamInfo[A, P, T, D, H], 0, option.StreamCount),
		eventExtraSize: eventExtraSize,
		startTime:      time.Now(),
	}
	if option.UseBuffer {
		ds.inChan = make(chan T, option.InputChanSize)
		ds.outChan = make(chan T, 64)
		ds.wakeInChan = make(chan P, 64)
		ds.wakeOutChan = make(chan P, 64)

		ds.eventChan = ds.outChan
		ds.wakeChan = ds.wakeOutChan
	} else {
		ds.eventChan = make(chan T, option.InputChanSize)
		ds.wakeChan = make(chan P, 64)
	}
	if option.EnableMemoryControl {
		if len(feedbackChan) == 0 {
			ds.feedbackChan = make(chan Feedback[A, P, D], 1024)
		} else {
			ds.feedbackChan = feedbackChan[0]
		}
		ds.memControl = newMemControl[A, P, T, D, H]()
	}
	return ds
}

func (d *dynamicStreamImpl[A, P, T, D, H]) In(path ...P) chan<- T {
	if d.option.UseBuffer {
		return d.inChan
	} else {
		return d.eventChan
	}
}

func (d *dynamicStreamImpl[A, P, T, D, H]) Wake(path ...P) chan<- P {
	if d.option.UseBuffer {
		return d.wakeInChan
	} else {
		return d.wakeChan
	}
}

func (d *dynamicStreamImpl[A, P, T, D, H]) Feedback() <-chan Feedback[A, P, D] {
	return d.feedbackChan
}

func (d *dynamicStreamImpl[A, P, T, D, H]) Start() {
	if d.option.UseBuffer {
		go receiver(d.inChan, d.outChan, &d.bufferCount)
		go receiver(d.wakeInChan, d.wakeOutChan, &d.wakeBufferCount)
	}

	d.schedWg.Add(1)
	go d.scheduler()
	d.distWg.Add(1)
	go d.distributor()
}

func (d *dynamicStreamImpl[A, P, T, D, H]) Close() {
	if d.hasClosed.CompareAndSwap(false, true) {
		if d.option.UseBuffer {
			close(d.inChan)
			close(d.wakeInChan)
		} else {
			close(d.eventChan)
			close(d.wakeChan)
		}
		close(d.cmdToSched)
		if d.feedbackChan != nil {
			close(d.feedbackChan)
		}
	}
	d.schedWg.Wait()
}

func (d *dynamicStreamImpl[A, P, T, D, H]) addPath(settings AreaSettings, path PathAndDest[P, D]) error {
	add := &addPathCmd[A, P, T, D, H]{
		settings: settings,
		path:     path,
	}
	cmd := &command{
		cmdType: typeAddPath,
		cmd:     add,
		wg:      &sync.WaitGroup{},
	}

	cmd.wg.Add(2) // need to wait for both scheduler and distributor to finish the command
	d.cmdToSched <- cmd
	cmd.wg.Wait()
	return add.err
}

func (d *dynamicStreamImpl[A, P, T, D, H]) AddPath(path P, dest D, settings ...AreaSettings) error {
	s := NewAreaSettings()
	if len(settings) != 0 {
		s = settings[0]
	}
	return d.addPath(s, PathAndDest[P, D]{Path: path, Dest: dest})
}

func (d *dynamicStreamImpl[A, P, T, D, H]) removePath(path P) error {
	remove := &removePathCmd[P]{path: path}
	cmd := &command{
		cmdType: typeRemovePath,
		cmd:     remove,
		wg:      &sync.WaitGroup{},
	}

	cmd.wg.Add(2) // need to wait for both scheduler and distributor
	d.cmdToSched <- cmd
	cmd.wg.Wait()
	return remove.err
}

func (d *dynamicStreamImpl[A, P, T, D, H]) RemovePath(path P) error {
	return d.removePath(path)
}

func (d *dynamicStreamImpl[A, P, T, D, H]) SetAreaSettings(area A, settings AreaSettings) {
	if d.memControl != nil {
		d.memControl.setAreaSettings(area, settings)
	}
}

func (d *dynamicStreamImpl[A, P, T, D, H]) GetMetrics() Metrics {
	m := Metrics{
		PendingQueueLen: int(d._statAllStreamPendingLen.Load()),
		MinHandledTS:    d._statMinHandledTS.Load(),
		AddPath:         int(d._statAddPathCount.Load()),
		RemovePath:      int(d._statRemovePathCount.Load()),
		ArrangeStream:   int(d._statArrangeStreamCount.Load()),
	}
	if d.option.UseBuffer {
		m.EventChanSize = int(d.bufferCount.Load()) + len(d.inChan) + len(d.outChan)
	} else {
		m.EventChanSize = len(d.eventChan)
	}
	return m
}

// Make the scheduler to balance immediately. Only used for test.
func (d *dynamicStreamImpl[A, P, T, D, H]) reportAndSchedule(rule ruleType, period time.Duration) {
	rs := &reportAndScheduleCmd{rule: rule, period: period}
	cmd := &command{
		cmdType: typeReportAndSchedule,
		cmd:     rs,
		wg:      &sync.WaitGroup{},
	}
	cmd.wg.Add(2)
	d.cmdToSched <- cmd
	cmd.wg.Wait()
}

func (d *dynamicStreamImpl[A, P, T, D, H]) scheduler() {
	defer func() {
		close(d.cmdToDist)
		d.distWg.Wait()

		for _, si := range d.streamInfos {
			si.stream.close()
		}

		d.schedWg.Done()
	}()

	nextStreamId := 0
	nextStreamIndex := NewRoundRobin(d.baseStreamCount)

	createStream := func() *stream[A, P, T, D, H] {
		nextStreamId++
		return newStream(nextStreamId, d.handler, d.reportChan, d.trackTopPaths, d.option)
	}
	nextStream := func() *streamInfo[A, P, T, D, H] {
		// We use round-robin to assign the paths to the streams
		s := d.streamInfos[nextStreamIndex.Next()]
		return s
	}

	genStreamInfoMap := func(sis []*streamInfo[A, P, T, D, H]) map[int]*streamInfo[A, P, T, D, H] {
		m := make(map[int]*streamInfo[A, P, T, D, H], len(sis))
		for _, si := range sis {
			m[si.stream.id] = si
		}
		return m
	}

	for i := 0; i < d.baseStreamCount; i++ {
		stream := createStream()
		si := &streamInfo[A, P, T, D, H]{
			stream:  stream,
			pathMap: make(map[*pathInfo[A, P, T, D, H]]struct{}),
		}

		d.streamInfos = append(d.streamInfos, si)

		// Strictly speaking, we should start the stream in the distributor, but we start it here anyway.
		stream.start(nil)
	}

	streamInfoMap := genStreamInfoMap(d.streamInfos)
	globalPathMap := make(map[P]*pathInfo[A, P, T, D, H])

	// return whether the path info exists
	// used to filter stale path state info reported from stream
	checkAndDeletePathFromStream := func(si *streamInfo[A, P, T, D, H], pi *pathInfo[A, P, T, D, H]) bool {
		if _, ok := globalPathMap[pi.path]; !ok {
			return false
		}
		if _, ok := si.pathMap[pi]; !ok {
			panic("The path should exist in the stream")
		}
		delete(si.pathMap, pi)
		return true
	}

	doSchedule := func(rule ruleType, testPeriod time.Duration, wg *sync.WaitGroup) {
		// The goal of scheduler is to balance the load of the streams, with minimum changes.
		// First of all, we have consistent number (baseStreamCount) of basic streams, and unlimited number of solo streams.
		// They are all in the d.streamInfos. The first baseStreamCount streams are the basic streams, and the rest are solo streams.
		// When a path is added, it is assigned to a basic stream with round-robin strategy. It could be imbalance, but we balance it
		// by three rules later:
		// 1. If a stream is too busy, we make the busy path a solo stream.
		// 2. If a solo stream is idle, we combine it into a base stream.
		// 3. If the most busy stream is too busy and the least busy stream is not busy, we shuffle the paths between them.
		// We use round-robin to apply the rules to the streams.
		// Since the number of streams is small, we don't need to worry about the performance of iterating all the streams.
		switch rule {
		case createSoloPath:
			newSoloStreamInfos := make([]*streamInfo[A, P, T, D, H], 0)
			arranges := make([]*arrangeStreamCmd[A, P, T, D, H], 0)
			newStreamInfos := make([]*streamInfo[A, P, T, D, H], 0, len(d.streamInfos))

			for i := 0; i < d.baseStreamCount; i++ {
				si := d.streamInfos[i]
				period := si.period()
				if testPeriod != 0 {
					period = testPeriod
				}
				if si.busyRatio(period) < BusyStreamRatio {
					newStreamInfos = append(newStreamInfos, si)
					continue
				}
				soloStreamInfos := make([]*streamInfo[A, P, T, D, H], 0)
				for _, ps := range si.streamStat.getMostBusyPaths() {
					if ps.busyRatio(period) < BusyPathRatio {
						continue
					}
					if !checkAndDeletePathFromStream(si, ps.pathInfo) {
						continue
					}
					soloStream := createStream()
					soloStreamInfo := &streamInfo[A, P, T, D, H]{
						stream:  soloStream,
						pathMap: map[*pathInfo[A, P, T, D, H]]struct{}{ps.pathInfo: {}},
					}

					soloStreamInfos = append(soloStreamInfos, soloStreamInfo)
				}

				if len(soloStreamInfos) != 0 {
					newCurrentStream := createStream()
					newCurrentStreamInfo := &streamInfo[A, P, T, D, H]{
						stream:  newCurrentStream,
						pathMap: si.pathMap, // The solo paths are removed from the current stream already
					}

					newStreams := make([]*stream[A, P, T, D, H], 0, len(soloStreamInfos)+1)
					newStreamPaths := make([][]*pathInfo[A, P, T, D, H], 0, len(soloStreamInfos)+1)

					for _, si := range soloStreamInfos {
						newStreams = append(newStreams, si.stream)
						newStreamPaths = append(newStreamPaths, SetToSlice(si.pathMap))
					}
					newStreams = append(newStreams, newCurrentStream)
					newStreamPaths = append(newStreamPaths, SetToSlice(newCurrentStreamInfo.pathMap))

					arranges = append(arranges, &arrangeStreamCmd[A, P, T, D, H]{
						oldStreams:     []*stream[A, P, T, D, H]{si.stream},
						newStreams:     newStreams,
						newStreamPaths: newStreamPaths,
					})

					newStreamInfos = append(newStreamInfos, newCurrentStreamInfo)
					newSoloStreamInfos = append(newSoloStreamInfos, soloStreamInfos...)
				} else {
					// Although the stream is busy, none of the paths are busy, we don't need to create solo streams.
					newStreamInfos = append(newStreamInfos, si)
				}
			}
			newStreamInfos = append(newStreamInfos, d.streamInfos[d.baseStreamCount:]...)
			newStreamInfos = append(newStreamInfos, newSoloStreamInfos...)

			if len(arranges) != 0 {
				d.streamInfos = newStreamInfos
				streamInfoMap = genStreamInfoMap(newStreamInfos)

				cmd := &command{
					cmdType: typeArrangeStream,
					cmd:     arranges,
					wg:      wg,
				}
				d.cmdToDist <- cmd
			} else {
				if wg != nil {
					wg.Done()
				}
			}
		case removeSoloPath:
			normalSoloStreamInfos := make([]*streamInfo[A, P, T, D, H], 0, len(d.streamInfos))

			idleSoloPaths := make([]*pathInfo[A, P, T, D, H], 0)
			idleSoloStreams := make([]*stream[A, P, T, D, H], 0)
			idleSoloStreamInfos := make([]*streamInfo[A, P, T, D, H], 0)
			for i := d.baseStreamCount; i < len(d.streamInfos); i++ {
				si := d.streamInfos[i]
				if len(si.pathMap) == 0 {
					// The solo stream is empty, we should remove it
					idleSoloStreams = append(idleSoloStreams, si.stream)
					idleSoloStreamInfos = append(idleSoloStreamInfos, si)
				} else if si.busyRatio(testPeriod) < IdlePathRatio {
					if len(si.pathMap) != 1 {
						panic("The solo stream should have only one path")
					}
					idleSoloPaths = append(idleSoloPaths, OneInSet(si.pathMap))
					idleSoloStreams = append(idleSoloStreams, si.stream)
					idleSoloStreamInfos = append(idleSoloStreamInfos, si)
				} else {
					normalSoloStreamInfos = append(normalSoloStreamInfos, si)
					continue
				}
			}

			if len(idleSoloStreamInfos) != 0 {
				// Select the least busy stream from the basic streams, and combine the solo paths into it.
				baseStreamInfos := make([]*streamInfo[A, P, T, D, H], 0, d.baseStreamCount)
				baseStreamInfos = append(baseStreamInfos, d.streamInfos[:d.baseStreamCount]...)
				sort.Sort(sortedSIs[A, P, T, D, H](baseStreamInfos))
				mostIdleStream := baseStreamInfos[0]

				newPaths := make([]*pathInfo[A, P, T, D, H], 0, len(idleSoloPaths)+len(mostIdleStream.pathMap))
				newPaths = CopySetToSlice(mostIdleStream.pathMap, newPaths)
				newPaths = append(newPaths, idleSoloPaths...)

				newStream := createStream()
				newStreamInfo := &streamInfo[A, P, T, D, H]{
					stream:  newStream,
					pathMap: SliceToSet(newPaths),
				}

				oldStreams := idleSoloStreams[:]
				oldStreams = append(oldStreams, mostIdleStream.stream)

				arrange := &arrangeStreamCmd[A, P, T, D, H]{
					oldStreams:     oldStreams,
					newStreams:     []*stream[A, P, T, D, H]{newStream},
					newStreamPaths: [][]*pathInfo[A, P, T, D, H]{newPaths},
				}

				newStreamInfos := make([]*streamInfo[A, P, T, D, H], 0, len(d.streamInfos)-len(idleSoloStreamInfos))
				newStreamInfos = append(newStreamInfos, newStreamInfo)
				newStreamInfos = append(newStreamInfos, baseStreamInfos[1:]...)
				newStreamInfos = append(newStreamInfos, normalSoloStreamInfos...)

				d.streamInfos = newStreamInfos
				streamInfoMap = genStreamInfoMap(newStreamInfos)

				d.cmdToDist <- &command{
					cmdType: typeArrangeStream,
					cmd:     []*arrangeStreamCmd[A, P, T, D, H]{arrange},
					wg:      wg,
				}
			} else {
				if wg != nil {
					wg.Done()
				}
			}
		case shuffleStreams:
			arranges := make([]*arrangeStreamCmd[A, P, T, D, H], 0)
			newStreamInfos := make([]*streamInfo[A, P, T, D, H], 0, len(d.streamInfos))

			baseStreamInfos := make([]*streamInfo[A, P, T, D, H], 0, d.baseStreamCount)
			baseStreamInfos = append(baseStreamInfos, d.streamInfos[:d.baseStreamCount]...)
			sort.Sort(sortedSIs[A, P, T, D, H](baseStreamInfos))

			for i := 0; i < d.baseStreamCount/2; i++ {
				leastBusy := baseStreamInfos[i]
				mostBusy := baseStreamInfos[d.baseStreamCount-1-i]

				if mostBusy.busyRatio(testPeriod) < BusyStreamRatio ||
					mostBusy.busyRatio(testPeriod) < leastBusy.busyRatio(testPeriod)*2 ||
					len(mostBusy.pathMap) == 1 {
					newStreamInfos = append(newStreamInfos, leastBusy, mostBusy)
					continue
				}

				totalPathsCount := len(mostBusy.pathMap) + len(leastBusy.pathMap)

				pathsChoices := [][]*pathInfo[A, P, T, D, H]{make([]*pathInfo[A, P, T, D, H], 0, totalPathsCount/2+1), make([]*pathInfo[A, P, T, D, H], 0, totalPathsCount/2+1)}
				nextIdx := NewRoundRobin(2)

				// We only fully shuffle the most busy paths from two streams.
				// The reset paths can mostly stay together as before.
				// We would like paths to keep staying in the same stream when possible.
				// It might create some optimization opportunities for golang runtime or the OS.
				addedPaths := 0
				for _, ps := range mostBusy.streamStat.getMostBusyPaths() {
					// To make the following shuffle easier
					if !checkAndDeletePathFromStream(mostBusy, ps.pathInfo) {
						continue
					}
					idx := nextIdx.Next()
					pathsChoices[idx] = append(pathsChoices[idx], ps.pathInfo)
					addedPaths++
				}
				for _, ps := range leastBusy.streamStat.getMostBusyPaths() {
					// To make the following shuffle easier
					if !checkAndDeletePathFromStream(leastBusy, ps.pathInfo) {
						continue
					}
					idx := nextIdx.Next()
					pathsChoices[idx] = append(pathsChoices[idx], ps.pathInfo)
					addedPaths++
				}

				stream1Paths := pathsChoices[0]
				stream2Paths := pathsChoices[1]

				len1 := len(mostBusy.pathMap)
				len2 := len(leastBusy.pathMap)
				stream1Moves := len1
				if len1 >= len2*2 && len1 >= d.trackTopPaths*2 {
					stream1Moves = (len1 + len2) / 2
				}
				i := 0
				for pi := range mostBusy.pathMap {
					if i < stream1Moves {
						stream1Paths = append(stream1Paths, pi)
						i++
					} else {
						stream2Paths = append(stream2Paths, pi)
					}
					addedPaths++
				}
				for pi := range leastBusy.pathMap {
					if i < stream1Moves {
						stream1Paths = append(stream1Paths, pi)
						i++
					} else {
						stream2Paths = append(stream2Paths, pi)
					}
					addedPaths++
				}

				if addedPaths != totalPathsCount || len(stream1Paths)+len(stream2Paths) != totalPathsCount {
					panic(fmt.Sprintf("The paths are not added correctly: %d, %d, %d", addedPaths, totalPathsCount, len(stream1Paths)+len(stream2Paths)))
				}

				stream1 := createStream()
				stream1Info := &streamInfo[A, P, T, D, H]{
					stream:  stream1,
					pathMap: SliceToSet(stream1Paths),
				}
				stream2 := createStream()
				stream2Info := &streamInfo[A, P, T, D, H]{
					stream:  stream2,
					pathMap: SliceToSet(stream2Paths),
				}
				// Note that we should never send pathMap instances to the distributor.
				// Instead, we put the paths to streamXPaths and send it.
				// Because pathMap will be changed later.
				arranges = append(arranges, &arrangeStreamCmd[A, P, T, D, H]{
					oldStreams:     []*stream[A, P, T, D, H]{mostBusy.stream, leastBusy.stream},
					newStreams:     []*stream[A, P, T, D, H]{stream1, stream2},
					newStreamPaths: [][]*pathInfo[A, P, T, D, H]{stream1Paths, stream2Paths},
				})

				newStreamInfos = append(newStreamInfos, stream1Info, stream2Info)
			}

			if d.baseStreamCount%2 != 0 {
				newStreamInfos = append(newStreamInfos, baseStreamInfos[d.baseStreamCount/2])
			}

			if len(arranges) != 0 {
				d.streamInfos = newStreamInfos
				streamInfoMap = genStreamInfoMap(newStreamInfos)

				cmd := &command{
					cmdType: typeArrangeStream,
					cmd:     arranges,
					wg:      wg,
				}
				d.cmdToDist <- cmd
			} else {
				if wg != nil {
					wg.Done()
				}
			}
		default:
			panic("Unknown rule")
		}
	}

	scheduleRule := NewRoundRobin(3)
	schedulerTicker := time.NewTicker(d.option.SchedulerInterval)
	statTicker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case cmd, ok := <-d.cmdToSched:
			if !ok {
				return
			}
			switch cmd.cmdType {
			case typeAddPath:
				add := cmd.cmd.(*addPathCmd[A, P, T, D, H])
				path := add.path.Path
				if _, ok := globalPathMap[path]; ok {
					add.err = NewAppErrorS(ErrorTypeDuplicate)
				} else {
					area := d.handler.GetArea(path, add.path.Dest)
					pi := newPathInfo[A, P, T, D, H](area, path, add.path.Dest)
					si := nextStream()
					pi.setStream(si.stream)
					si.pathMap[pi] = struct{}{}
					globalPathMap[path] = pi
					add.pi = pi
				}
				cmd.wg.Done()
				d.cmdToDist <- cmd
			case typeRemovePath:
				remove := cmd.cmd.(*removePathCmd[P])
				path := remove.path
				err := NewAppErrorS(ErrorTypeNotExist)
				pi, ok := globalPathMap[path]
				if !ok {
					remove.err = err
				} else {
					// Here we iterate all the streams to remove the path.
					// It is not the most efficient, but the number of streams is small.
					// And we don't want to keep a reverse map from path to stream, as it is too complex.
					//
					// Note that we cannot get the stream from the pathInfo as follow. Because pathInfo.stream
					// is updated by the distributor. And the distributor is not guaranteed to finish the update.
					//   delete(streamInfoMap[pi.stream.id].pathMap, pi)
					for _, si := range d.streamInfos {
						delete(si.pathMap, pi)
					}
					delete(globalPathMap, path)
					// If it is a solo path, we don't need to remove the empty solo stream in here.
					// The empty solo stream will be removed in the removeSoloPath rule.
				}
				cmd.wg.Done()
				// We send the command to distributor even if some paths don't exist, to remove the existed paths in the distributor.
				d.cmdToDist <- cmd
			case typeReportAndSchedule:
				// Make all the streams to report the statistics and do the schedule
				// Only used by tests
				reportAndSchedule := cmd.cmd.(*reportAndScheduleCmd)
				for _, si := range d.streamInfos {
					si.stream.reportNow <- struct{}{}
				}
				// Wait for all the streams to report the statistics
				for range d.streamInfos {
					stat := <-d.reportChan
					si, ok := streamInfoMap[stat.id]
					if !ok {
						// The stream is removed already
						continue
					}
					si.streamStat = stat
				}
				// Do the schedule
				doSchedule(reportAndSchedule.rule, reportAndSchedule.period, cmd.wg)
				cmd.wg.Done()
			default:
				panic("Unknown command type")
			}
		case stat := <-d.reportChan:
			si, ok := streamInfoMap[stat.id]
			if !ok {
				// The stream is removed already
				continue
			}
			si.streamStat = stat
		case <-schedulerTicker.C:
			doSchedule(ruleType(scheduleRule.Next()), 0, nil)
		case <-statTicker.C:
			// Update the metrics
			if d.memControl != nil {
				d.memControl.updateMetrics()
			}
			allStreamPendingLen := 0
			minHandledTS := uint64(0)
			for _, si := range d.streamInfos {
				allStreamPendingLen += si.stream.getPendingSize()
				handledTs := si.stream._statMinHandledTS.Load()
				if minHandledTS == 0 || (minHandledTS > handledTs && handledTs != 0) {
					minHandledTS = handledTs
				}
			}
			d._statAllStreamPendingLen.Store(int64(allStreamPendingLen))
			d._statMinHandledTS.Store(minHandledTS)
		}
	}
}

func (d *dynamicStreamImpl[A, P, T, D, H]) distributor() {
	defer d.distWg.Done()

	pathMap := make(map[P]*pathInfo[A, P, T, D, H])

	for {
		select {
		case e, ok := <-d.eventChan:
			if !ok {
				return
			}
			eventType := d.handler.GetType(e)
			if pi, ok := pathMap[d.handler.Path(e)]; ok {
				e := eventWrap[A, P, T, D, H]{
					event:     e,
					pathInfo:  pi,
					paused:    d.handler.IsPaused(e),
					eventType: eventType,
					eventSize: d.eventExtraSize + d.handler.GetSize(e),
					timestamp: d.handler.GetTimestamp(e),
					queueTime: time.Now(),
				}
				if e.timestamp == 0 {
					e.timestamp = (Timestamp)(e.queueTime.Sub(d.startTime))
				}
				pi.stream.in() <- e
			} else {
				// Otherwise, drop the event and notify the handler
				d.handler.OnDrop(e)
			}
		case p, ok := <-d.wakeChan:
			if !ok {
				return
			}
			if pi, ok := pathMap[p]; ok {
				pi.stream.in() <- eventWrap[A, P, T, D, H]{wake: true, pathInfo: pi}
			}
			// Otherwise, drop the wake signal
		case cmd, ok := <-d.cmdToDist:
			if !ok {
				return
			}
			switch cmd.cmdType {
			case typeAddPath:
				add := cmd.cmd.(*addPathCmd[A, P, T, D, H])
				// We don't need to add the path in distributor if there was an error
				// occurred in the scheduler.
				if add.err == nil {
					path := add.pi.path
					if _, ok := pathMap[path]; ok {
						panic(fmt.Sprintf("Path %v already exists in distributor", path))
					}
					pathMap[path] = add.pi
					if d.memControl != nil {
						d.memControl.addPathToArea(add.pi, add.settings, d.feedbackChan)
					}
				}
				cmd.wg.Done()
				d._statAddPathCount.Add(1)
			case typeRemovePath:
				remove := cmd.cmd.(*removePathCmd[P])
				// We don't need to remove the path in distributor if there was an error
				// occurred in the scheduler.
				if remove.err == nil {
					path := remove.path
					pi, ok := pathMap[path]
					if ok {
						pi.removed = true
						delete(pathMap, path)
						// The removal of the path from the memory control is done in the stream where the path belongs to.
						// We cannot remove the path from the memory control here, because the stream is updating the memory control with the path.
						// Send an empty event to the stream to notify the stream to remove the path
						pi.stream.in() <- eventWrap[A, P, T, D, H]{pathInfo: pi}
						// Don't close the stream here. The stream is processing other paths.
					} else {
						panic(fmt.Sprintf("Path %v doesn't exist in distributor", path))
					}
				}
				cmd.wg.Done()
				d._statRemovePathCount.Add(1)
			case typeArrangeStream:
				arranges := cmd.cmd.([]*arrangeStreamCmd[A, P, T, D, H])
				for _, arrange := range arranges {
					for i, paths := range arrange.newStreamPaths {
						newStream := arrange.newStreams[i]
						for _, pi := range paths {
							if _, ok := pathMap[pi.path]; !ok {
								panic(fmt.Sprintf("Path %v doesn't exist in distributor", pi.path))
							}
							// We must set the stream of this path here immediately.
							// Because we might send the event to the path shortly, before the asynchronous initialization of the stream finish.
							pi.setStream(newStream)
						}
						// Streams must be started and closed in the distributor.
						// Otherwise, the distributor will send the events to the closed streams.
						newStream.start(paths, arrange.oldStreams...)
					}
					d._statArrangeStreamCount.Add(1)
				}
				if cmd.wg != nil {
					cmd.wg.Done()
				}
			default:
				panic("Unknown command type")
			}
		}
	}
}

func receiver[E any](inChan <-chan E, outChan chan<- E, bufferCount *atomic.Int64) {
	defer close(outChan)

	buffer := deque.NewDequeDefault[E]()

	for {
		event, ok := buffer.FrontRef()
		if !ok {
			e, ok := <-inChan
			if !ok {
				return
			}
			buffer.PushBack(e)
			bufferCount.Add(1)
		} else {
			select {
			case e, ok := <-inChan:
				if !ok {
					return
				}
				buffer.PushBack(e)
				bufferCount.Add(1)
			case outChan <- *event:
				buffer.PopFront()
				bufferCount.Add(-1)
			}
		}
	}
}
