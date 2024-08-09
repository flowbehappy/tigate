package dynstream

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/flowbehappy/tigate/pkg/apperror"
	. "github.com/flowbehappy/tigate/utils"
)

const TrackTopPaths = 64
const BusyStreamRatio = 0.5
const BusyPathRatio = 0.25
const IdlePathRatio = 0.02

const DefaultSchedulerInterval = 5 * time.Second
const DefaultReportInterval = 500 * time.Millisecond

// We don't need lots of streams because the hanle of events should be CPU-bound and should not be blocked by any waiting.
const DefaultStreamCount = 128

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

type cmd struct {
	cmdType cmdType
	cmd     interface{}
}

type addPathCmd[T Event, D any] struct {
	paths []PathAndDest[D]
	pis   []*pathInfo[T, D]
	error error

	wg sync.WaitGroup
}

type removePathCmd struct {
	paths  []Path
	errors []error

	wg sync.WaitGroup
}

type arrangeStreamCmd[T Event, D any] struct {
	oldStreams []*stream[T, D]

	newStreams     []*stream[T, D]
	newStreamPaths [][]*pathInfo[T, D]
}

type reportAndScheduleCmd struct {
	rule   ruleType
	period time.Duration
	wg     sync.WaitGroup
}

type streamInfo[T Event, D any] struct {
	stream     *stream[T, D]
	streamStat *streamStat[T, D]
	pathMap    map[*pathInfo[T, D]]struct{}
}

func (si *streamInfo[T, D]) runtime() time.Duration {
	if si.streamStat != nil {
		return si.streamStat.totalTime
	} else {
		return 0
	}
}

func (si *streamInfo[T, D]) busyRatio(period time.Duration) float64 {
	if si.streamStat != nil && si.streamStat.totalTime != 0 {
		if period != 0 {
			return float64(si.streamStat.totalTime) / float64(period)
		} else {
			return float64(si.streamStat.totalTime) / float64(si.streamStat.period)
		}
	} else {
		return 0
	}
}

func (si *streamInfo[T, D]) period() time.Duration {
	if si.streamStat != nil {
		return si.streamStat.period
	} else {
		return 0
	}
}

type sortedSIs[T Event, D any] []*streamInfo[T, D]

// implement sort.Interface
func (s sortedSIs[T, D]) Len() int           { return len(s) }
func (s sortedSIs[T, D]) Less(i, j int) bool { return s[i].runtime() < s[j].runtime() }
func (s sortedSIs[T, D]) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type DynamicStream[T Event, D any] struct {
	schedulerInterval time.Duration
	reportInterval    time.Duration
	trackTopPaths     int
	baseStreamCount   int

	handler Handler[T, D]

	eventChan  chan T                 // The channel to receive the incomming events by distributor
	reportChan chan *streamStat[T, D] // The channel to receive the report by scheduler
	cmdToSchd  chan *cmd              // Send the commands to the scheduler
	cmdToDist  chan *cmd              // Send the commands to the distributor

	// The streams to handle the events. Only used in the scheduler.
	// We put it here mainly to make the tests easier.
	streamInfos []*streamInfo[T, D]

	hasClosed atomic.Bool

	schdDone sync.WaitGroup
	distDone sync.WaitGroup
}

func NewDynamicStreamDefault[T Event, D any](handler Handler[T, D]) *DynamicStream[T, D] {
	streamCount := max(DefaultStreamCount, runtime.NumCPU())
	return NewDynamicStream[T, D](handler, DefaultSchedulerInterval, DefaultReportInterval, streamCount)
}

func NewDynamicStream[T Event, D any](
	handler Handler[T, D],
	schedulerInterval time.Duration,
	reportInterval time.Duration,
	streamCount int,
) *DynamicStream[T, D] {
	return &DynamicStream[T, D]{
		handler: handler,

		schedulerInterval: schedulerInterval,
		reportInterval:    reportInterval,
		trackTopPaths:     TrackTopPaths,
		baseStreamCount:   streamCount,

		eventChan:  make(chan T, 1024),
		reportChan: make(chan *streamStat[T, D], 64),
		cmdToSchd:  make(chan *cmd, 64),
		cmdToDist:  make(chan *cmd, streamCount),

		streamInfos: make([]*streamInfo[T, D], 0, streamCount),
	}
}

func (d *DynamicStream[T, D]) In() chan<- T {
	return d.eventChan
}

func (d *DynamicStream[T, D]) Start() {
	d.schdDone.Add(1)
	go d.scheduler()
	d.distDone.Add(1)
	go d.distributor()
}

func (d *DynamicStream[T, D]) Close() {
	if d.hasClosed.CompareAndSwap(false, true) {
		close(d.cmdToSchd)
	}
	d.schdDone.Wait()
}

func (d *DynamicStream[T, D]) scheduler() {
	defer func() {
		close(d.cmdToDist)
		d.distDone.Wait()

		for _, si := range d.streamInfos {
			si.stream.close()
		}

		d.schdDone.Done()
	}()

	nextStreamId := 0
	nextStreamIndex := NewRoundRobin(d.baseStreamCount)

	newStream := func() *stream[T, D] {
		nextStreamId++
		return newStream[T, D](nextStreamId, d.handler, d.reportChan, d.reportInterval, d.trackTopPaths)
	}
	nextStream := func() *streamInfo[T, D] {
		// We use round-robin to assign the paths to the streams
		s := d.streamInfos[nextStreamIndex.Next()]
		return s
	}
	genStreamInfoMap := func(sis []*streamInfo[T, D]) map[int]*streamInfo[T, D] {
		m := make(map[int]*streamInfo[T, D], len(d.streamInfos))
		for _, si := range sis {
			m[si.stream.id] = si
		}
		return m
	}

	for i := 0; i < d.baseStreamCount; i++ {
		stream := newStream()
		si := &streamInfo[T, D]{
			stream:  stream,
			pathMap: make(map[*pathInfo[T, D]]struct{}),
		}

		d.streamInfos = append(d.streamInfos, si)

		// Strictly speaking, we should start the stream in the distributor, but we start it here anyway.
		stream.start(nil)
	}

	streamInfoMap := genStreamInfoMap(d.streamInfos)
	globalPathMap := make(map[Path]struct{}) // Use to check the path duplication

	scheduleRule := NewRoundRobin(3)
	doSchedule := func(rule ruleType, testPeriod time.Duration) {
		// The goal of scheduler is to balance the load of the streams, with mimimum changes.
		// We have three rules:
		// 1. If some stream is too busy, we make the busy paths solo in a new stream.
		// 2. If some solo stream is idle, we combine them and move them into a base stream.
		// 3. If the most busy stream is too busy and the least busy stream is not busy, we shuffle the paths between them.
		// We use round-robin to apply the rules to the streams.
		// Since the number of streams is small, we don't need to worry about the performance of iterating all the streams.

		if rule == createSoloPath {
			newSoloStreamInfos := make([]*streamInfo[T, D], 0)
			arranges := make([]*arrangeStreamCmd[T, D], 0)
			newStreamInfos := make([]*streamInfo[T, D], 0, len(d.streamInfos))

			for i := 0; i < d.baseStreamCount; i++ {
				si := d.streamInfos[i]
				if si.busyRatio(testPeriod) < BusyStreamRatio {
					newStreamInfos = append(newStreamInfos, si)
					continue
				}
				soloStreamInfos := make([]*streamInfo[T, D], 0)
				for _, ps := range si.streamStat.mostBusyPath.All() {
					period := si.period()
					if testPeriod != 0 {
						period = testPeriod
					}
					if ps.busyRatio(period) < BusyPathRatio {
						continue
					}
					soloStream := newStream()
					soloStreamInfo := &streamInfo[T, D]{
						stream:  soloStream,
						pathMap: map[*pathInfo[T, D]]struct{}{ps.pathInfo: {}},
					}

					if _, ok := si.pathMap[ps.pathInfo]; !ok {
						panic("The path should exist in the stream")
					}
					delete(si.pathMap, ps.pathInfo)

					soloStreamInfos = append(soloStreamInfos, soloStreamInfo)
				}

				if len(soloStreamInfos) != 0 {
					newCurrentStream := newStream()
					newCurrentStreamInfo := &streamInfo[T, D]{
						stream:  newCurrentStream,
						pathMap: si.pathMap, // The solo paths are removed from the current stream already
					}

					newStreams := make([]*stream[T, D], 0, len(soloStreamInfos)+1)
					newStreamPaths := make([][]*pathInfo[T, D], 0, len(soloStreamInfos)+1)

					for _, si := range soloStreamInfos {
						newStreams = append(newStreams, si.stream)
						newStreamPaths = append(newStreamPaths, SetToSlice(si.pathMap))
					}
					newStreams = append(newStreams, newCurrentStream)
					newStreamPaths = append(newStreamPaths, SetToSlice(newCurrentStreamInfo.pathMap))

					arranges = append(arranges, &arrangeStreamCmd[T, D]{
						oldStreams:     []*stream[T, D]{si.stream},
						newStreams:     newStreams,
						newStreamPaths: newStreamPaths,
					})

					newStreamInfos = append(newStreamInfos, newCurrentStreamInfo)
					newSoloStreamInfos = append(newSoloStreamInfos, soloStreamInfos...)
				}
			}
			newStreamInfos = append(newStreamInfos, d.streamInfos[d.baseStreamCount:]...)
			newStreamInfos = append(newStreamInfos, newSoloStreamInfos...)

			if len(arranges) != 0 {
				d.streamInfos = newStreamInfos
				streamInfoMap = genStreamInfoMap(newStreamInfos)

				for _, arrange := range arranges {
					cmd := &cmd{
						cmdType: typeArrangeStream,
						cmd:     arrange,
					}
					d.cmdToDist <- cmd
				}
			}
		} else if rule == removeSoloPath {
			normalSoloStreamInfos := make([]*streamInfo[T, D], 0, len(d.streamInfos))

			idleSoloPaths := make([]*pathInfo[T, D], 0)
			idleSoloStreams := make([]*stream[T, D], 0)
			idleSoloStreamInfos := make([]*streamInfo[T, D], 0)
			for i := d.baseStreamCount; i < len(d.streamInfos); i++ {
				si := d.streamInfos[i]
				if si.busyRatio(testPeriod) >= IdlePathRatio {
					normalSoloStreamInfos = append(normalSoloStreamInfos, si)
					continue
				}
				if len(si.pathMap) != 1 {
					panic("The solo stream should have only one path")
				}
				idleSoloPaths = append(idleSoloPaths, OneInSet(si.pathMap))
				idleSoloStreams = append(idleSoloStreams, si.stream)
				idleSoloStreamInfos = append(idleSoloStreamInfos, si)
			}

			if len(idleSoloStreamInfos) != 0 {
				baseStreamInfos := make([]*streamInfo[T, D], 0, d.baseStreamCount)
				baseStreamInfos = append(baseStreamInfos, d.streamInfos[:d.baseStreamCount]...)
				sort.Sort(sortedSIs[T, D](baseStreamInfos))
				mostIdleStream := baseStreamInfos[0]

				newPaths := make([]*pathInfo[T, D], 0, len(idleSoloPaths)+len(mostIdleStream.pathMap))
				newPaths = CopySetToSlice(mostIdleStream.pathMap, newPaths)
				newPaths = append(newPaths, idleSoloPaths...)

				newStream := newStream()
				newStreamInfo := &streamInfo[T, D]{
					stream:  newStream,
					pathMap: SliceToSet(newPaths),
				}

				oldStreams := idleSoloStreams[:]
				oldStreams = append(oldStreams, mostIdleStream.stream)

				arrange := &arrangeStreamCmd[T, D]{
					oldStreams:     oldStreams,
					newStreams:     []*stream[T, D]{newStream},
					newStreamPaths: [][]*pathInfo[T, D]{newPaths},
				}

				newStreamInfos := make([]*streamInfo[T, D], 0, len(d.streamInfos)-len(idleSoloStreamInfos))
				newStreamInfos = append(newStreamInfos, newStreamInfo)
				newStreamInfos = append(newStreamInfos, baseStreamInfos[1:]...)
				newStreamInfos = append(newStreamInfos, normalSoloStreamInfos...)

				d.streamInfos = newStreamInfos
				streamInfoMap = genStreamInfoMap(newStreamInfos)

				d.cmdToDist <- &cmd{
					cmdType: typeArrangeStream,
					cmd:     arrange,
				}
			}
		} else if rule == shuffleStreams {
			arranges := make([]*arrangeStreamCmd[T, D], 0)
			newStreamInfos := make([]*streamInfo[T, D], 0, len(d.streamInfos))

			baseStreamInfos := make([]*streamInfo[T, D], 0, d.baseStreamCount)
			baseStreamInfos = append(baseStreamInfos, d.streamInfos[:d.baseStreamCount]...)
			sort.Sort(sortedSIs[T, D](baseStreamInfos))

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

				pathsChoices := [][]*pathInfo[T, D]{make([]*pathInfo[T, D], 0, totalPathsCount/2+1), make([]*pathInfo[T, D], 0, totalPathsCount/2+1)}
				nextIdx := NewRoundRobin(2)

				// We only fully shuffle the most busy paths from two streams.
				// The reset paths can mostly stay together as before.
				// We would like paths to keep staying in the same stream when possible.
				// It might create some optimization opportunities for golang runtime or the OS.

				for _, ps := range mostBusy.streamStat.mostBusyPath.All() {
					idx := nextIdx.Next()
					pathsChoices[idx] = append(pathsChoices[idx], ps.pathInfo)
					if _, ok := mostBusy.pathMap[ps.pathInfo]; !ok {
						panic("The path should exist in the stream")
					}
					// To make the following shuffle easier
					delete(mostBusy.pathMap, ps.pathInfo)
				}
				for _, ps := range leastBusy.streamStat.mostBusyPath.All() {
					idx := nextIdx.Next()
					pathsChoices[idx] = append(pathsChoices[idx], ps.pathInfo)
					if _, ok := leastBusy.pathMap[ps.pathInfo]; !ok {
						panic("The path should exist in the stream")
					}
					// To make the following shuffle easier
					delete(leastBusy.pathMap, ps.pathInfo)
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
				}
				for pi := range leastBusy.pathMap {
					if i < stream1Moves {
						stream1Paths = append(stream1Paths, pi)
						i++
					} else {
						stream2Paths = append(stream2Paths, pi)
					}
				}

				stream1 := newStream()
				stream1Info := &streamInfo[T, D]{
					stream:  stream1,
					pathMap: SliceToSet(stream1Paths),
				}
				stream2 := newStream()
				stream2Info := &streamInfo[T, D]{
					stream:  stream2,
					pathMap: SliceToSet(stream2Paths),
				}
				// Note that we should never send pathMap instances to the distributor.
				// Instead, we put the paths to streamXPaths and send it.
				// Because pathMap will be changed later.
				arranges = append(arranges, &arrangeStreamCmd[T, D]{
					oldStreams:     []*stream[T, D]{mostBusy.stream, leastBusy.stream},
					newStreams:     []*stream[T, D]{stream1, stream2},
					newStreamPaths: [][]*pathInfo[T, D]{stream1Paths, stream2Paths},
				})

				newStreamInfos = append(newStreamInfos, stream1Info, stream2Info)
			}

			if d.baseStreamCount%2 != 0 {
				newStreamInfos = append(newStreamInfos, baseStreamInfos[d.baseStreamCount/2])
			}

			if len(arranges) != 0 {
				d.streamInfos = newStreamInfos
				streamInfoMap = genStreamInfoMap(newStreamInfos)

				for _, arrange := range arranges {
					cmd := &cmd{
						cmdType: typeArrangeStream,
						cmd:     arrange,
					}
					d.cmdToDist <- cmd
				}
			}
		} else {
			panic("Unknown rule")
		}

		// Normally, we don't need to reset the statistics, but who knows.
		// for _, si := range d.streamInfos {
		// 	si.streamStat = nil
		// }
	}

	nextSchedule := time.Now().Add(d.schedulerInterval)
	timerChan := time.After(time.Until(nextSchedule))
Loop:
	for {
		select {
		case cmd, ok := <-d.cmdToSchd:
			if !ok {
				return
			}
			switch cmd.cmdType {
			case typeAddPath:
				add := cmd.cmd.(*addPathCmd[T, D])

				// Make sure the paths don't exist already
				for _, pd := range add.paths {
					if _, ok := globalPathMap[pd.Path]; ok {
						add.error = NewAppErrorS(ErrorTypeDuplicate)
						add.wg.Done()
						continue Loop
					}
				}

				pis := make([]*pathInfo[T, D], 0, len(add.paths))
				for _, pd := range add.paths {
					pi := newPathInfo[T, D](pd.Path, pd.Dest)
					si := nextStream()
					pi.stream = si.stream
					si.pathMap[pi] = struct{}{}
					globalPathMap[pd.Path] = struct{}{}

					pis = append(pis, pi)
				}
				add.pis = pis

				add.wg.Done()
				d.cmdToDist <- cmd
			case typeRemovePath:
				remove := cmd.cmd.(*removePathCmd)
				errors := make([]error, len(remove.paths))
				hasError := false
				e := NewAppErrorS(ErrorTypeNotExist)
				for _, p := range remove.paths {
					if _, ok := globalPathMap[p]; !ok {
						errors = append(errors, e)
						hasError = true
						continue
					}
					delete(globalPathMap, p)
					errors = append(errors, nil)
				}
				remove.wg.Done()
				if hasError {
					remove.errors = errors
				}
				// We send the command to distributor even if some paths don't exist, to remove the existed paths in the distributor.
				d.cmdToDist <- cmd
			case typeReportAndSchedule:
				reportAndSchedule := cmd.cmd.(*reportAndScheduleCmd)
				// Make all the streams to report the statistics
				// Only used by tests
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
				doSchedule(reportAndSchedule.rule, reportAndSchedule.period)
				reportAndSchedule.wg.Done()
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
		case <-timerChan:
			nextSchedule = time.Now().Add(d.schedulerInterval)
			timerChan = time.After(time.Until(nextSchedule))
			doSchedule(ruleType(scheduleRule.Next()), 0)
		}
	}
}

func (d *DynamicStream[T, D]) distributor() {
	defer d.distDone.Done()

	pathMap := make(map[Path]*pathInfo[T, D])

	for {
		select {
		case e := <-d.eventChan:
			if pi, ok := pathMap[e.Path()]; ok {
				pi.stream.in() <- &eventWrap[T, D]{event: e, pathInfo: pi}
			}
			// Otherwise, drop the event
		case cmd, ok := <-d.cmdToDist:
			if !ok {
				return
			}
			switch cmd.cmdType {
			case typeAddPath:
				add := cmd.cmd.(*addPathCmd[T, D])
				for _, pi := range add.pis {
					if _, ok := pathMap[pi.path]; ok {
						panic(fmt.Sprintf("Path %v already exists in distributor", pi.path))
					}
					pathMap[pi.path] = pi
				}
				add.wg.Done()
			case typeRemovePath:
				remove := cmd.cmd.(*removePathCmd)
				for _, p := range remove.paths {
					delete(pathMap, p)
				}
				remove.wg.Done()
			case typeArrangeStream:
				arrange := cmd.cmd.(*arrangeStreamCmd[T, D])
				for i, paths := range arrange.newStreamPaths {
					newStream := arrange.newStreams[i]
					for _, pi := range paths {
						if _, ok := pathMap[pi.path]; !ok {
							panic(fmt.Sprintf("Path %v doesn't exist in distributor", pi.path))
						}
						pi.stream = newStream
					}
					// Streams must be started and closed in the distributor.
					// Otherwise, the distributor will send the events to the closed streams.
					newStream.start(paths)
				}
			default:
				panic("Unknown command type")
			}
		}
	}
}

// AddPath adds the paths to the dynamic stream to receive the events.
// An event with a path not already added will be dropped.
// If some paths already exist, it will return an ErrorTypeDuplicate error. And no paths are added.
// If the stream is closed, it will return an ErrorTypeClosed error.
func (d *DynamicStream[T, D]) AddPath(paths ...PathAndDest[D]) error {
	if d.hasClosed.Load() {
		return NewAppErrorS(ErrorTypeClosed)
	}
	add := &addPathCmd[T, D]{paths: paths}
	cmd := &cmd{
		cmdType: typeAddPath,
		cmd:     add,
	}
	add.wg.Add(2) // need to wait for both scheduler and distributor
	d.cmdToSchd <- cmd
	add.wg.Wait()
	return add.error
}

// RemovePath removes the paths from the dynamic stream. Futher events with the paths will be dropped.
// If some paths don't exist, it will return ErrorTypeNotExist errors. But the existed paths are still removed.
// If all paths are removed successfully, return nil.
func (d *DynamicStream[T, D]) RemovePath(paths ...Path) []error {
	remove := &removePathCmd{paths: paths}
	cmd := &cmd{
		cmdType: typeRemovePath,
		cmd:     remove,
	}
	remove.wg.Add(2) // need to wait for both scheduler and distributor
	d.cmdToSchd <- cmd
	remove.wg.Wait()
	return remove.errors
}
