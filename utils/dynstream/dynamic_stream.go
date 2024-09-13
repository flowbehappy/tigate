package dynstream

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/flowbehappy/tigate/pkg/apperror"
	. "github.com/flowbehappy/tigate/utils"
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

type cmd struct {
	cmdType cmdType
	cmd     interface{}
}

type addPathCmd[P Path, T Event, D Dest] struct {
	paths []PathAndDest[P, D]
	pis   []*pathInfo[P, T, D]
	error error

	wg sync.WaitGroup
}

type removePathCmd[P Path] struct {
	paths  []P
	errors []error

	wg sync.WaitGroup
}

type arrangeStreamCmd[P Path, T Event, D Dest] struct {
	oldStreams []*stream[P, T, D]

	newStreams     []*stream[P, T, D]
	newStreamPaths [][]*pathInfo[P, T, D]
}

type reportAndScheduleCmd struct {
	rule   ruleType
	period time.Duration
	wg     sync.WaitGroup
}

type streamInfo[P Path, T Event, D Dest] struct {
	stream     *stream[P, T, D]
	streamStat streamStat[P, T, D]
	pathMap    map[*pathInfo[P, T, D]]struct{}
}

func (si *streamInfo[P, T, D]) runtime() time.Duration {
	return si.streamStat.totalTime
}

func (si *streamInfo[P, T, D]) busyRatio(period time.Duration) float64 {
	if si.streamStat.totalTime != 0 {
		if period != 0 {
			return float64(si.streamStat.totalTime) / float64(period)
		} else {
			return float64(si.streamStat.totalTime) / float64(si.streamStat.period)
		}
	} else {
		return 0
	}
}

func (si *streamInfo[P, T, D]) period() time.Duration {
	return si.streamStat.period
}

type sortedSIs[P Path, T Event, D Dest] []*streamInfo[P, T, D]

// implement sort.Interface
func (s sortedSIs[P, T, D]) Len() int           { return len(s) }
func (s sortedSIs[P, T, D]) Less(i, j int) bool { return s[i].runtime() < s[j].runtime() }
func (s sortedSIs[P, T, D]) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// This is the implementation of the DynamicStream interface.
// We use two goroutines
// 1. The distributor to distribute the events to the streams
// 2. The scheduler to balance the load of the streams
//
// A stream can handle events from multiple paths.
// Events from the same path are only processed by one particular stream at the same time.
// The scheduler use several strategies to balance the load of the streams, while the final balanace
// actions are moving the paths between the streams.
type dynamicStreamImpl[P Path, T Event, D Dest] struct {
	trackTopPaths   int
	baseStreamCount int

	handler Handler[P, T, D]
	option  Option

	eventChan chan T // The channel to receive the incomming events by distributor
	wakeChan  chan P // The channel to receive the wake signal by distributor

	reportChan chan streamStat[P, T, D] // The channel to receive the report by scheduler
	cmdToSchd  chan *cmd                // Send the commands to the scheduler
	cmdToDist  chan *cmd                // Send the commands to the distributor

	// The streams to handle the events. Only used in the scheduler.
	// We put it here mainly to make the tests easier.
	streamInfos []*streamInfo[P, T, D]

	hasClosed atomic.Bool

	schdDone sync.WaitGroup
	distDone sync.WaitGroup
}

func newDynamicStreamImpl[P Path, T Event, D Dest](
	handler Handler[P, T, D],
	option Option,
) *dynamicStreamImpl[P, T, D] {
	return &dynamicStreamImpl[P, T, D]{
		handler: handler,
		option:  option,

		trackTopPaths:   TrackTopPaths,
		baseStreamCount: option.StreamCount,

		eventChan:  make(chan T, 1024),
		wakeChan:   make(chan P, 64),
		reportChan: make(chan streamStat[P, T, D], 64),
		cmdToSchd:  make(chan *cmd, 64),
		cmdToDist:  make(chan *cmd, option.StreamCount),

		streamInfos: make([]*streamInfo[P, T, D], 0, option.StreamCount),
	}
}

func (d *dynamicStreamImpl[P, T, D]) In() chan<- T {
	return d.eventChan
}

func (d *dynamicStreamImpl[P, T, D]) Wake() chan<- P {
	return d.wakeChan
}

func (d *dynamicStreamImpl[P, T, D]) Start() {
	d.schdDone.Add(1)
	go d.scheduler()
	d.distDone.Add(1)
	go d.distributor()
}

func (d *dynamicStreamImpl[P, T, D]) Close() {
	if d.hasClosed.CompareAndSwap(false, true) {
		close(d.cmdToSchd)
	}
	d.schdDone.Wait()
}

func (d *dynamicStreamImpl[P, T, D]) AddPaths(paths ...PathAndDest[P, D]) error {
	if d.hasClosed.Load() {
		return NewAppErrorS(ErrorTypeClosed)
	}
	add := &addPathCmd[P, T, D]{paths: paths}
	cmd := &cmd{
		cmdType: typeAddPath,
		cmd:     add,
	}
	add.wg.Add(2) // need to wait for both scheduler and distributor
	d.cmdToSchd <- cmd
	add.wg.Wait()
	return add.error
}

func (d *dynamicStreamImpl[P, T, D]) AddPath(path P, dest D) error {
	return d.AddPaths(PathAndDest[P, D]{Path: path, Dest: dest})
}

func (d *dynamicStreamImpl[P, T, D]) RemovePaths(paths ...P) []error {
	remove := &removePathCmd[P]{paths: paths}
	cmd := &cmd{
		cmdType: typeRemovePath,
		cmd:     remove,
	}
	remove.wg.Add(2) // need to wait for both scheduler and distributor
	d.cmdToSchd <- cmd
	remove.wg.Wait()
	return remove.errors
}

func (d *dynamicStreamImpl[P, T, D]) scheduler() {
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

	newStream := func() *stream[P, T, D] {
		nextStreamId++
		return newStream[P, T, D](nextStreamId, d.handler, d.reportChan, d.trackTopPaths, d.option)
	}
	nextStream := func() *streamInfo[P, T, D] {
		// We use round-robin to assign the paths to the streams
		s := d.streamInfos[nextStreamIndex.Next()]
		return s
	}
	genStreamInfoMap := func(sis []*streamInfo[P, T, D]) map[int]*streamInfo[P, T, D] {
		m := make(map[int]*streamInfo[P, T, D], len(d.streamInfos))
		for _, si := range sis {
			m[si.stream.id] = si
		}
		return m
	}

	for i := 0; i < d.baseStreamCount; i++ {
		stream := newStream()
		si := &streamInfo[P, T, D]{
			stream:  stream,
			pathMap: make(map[*pathInfo[P, T, D]]struct{}),
		}

		d.streamInfos = append(d.streamInfos, si)

		// Strictly speaking, we should start the stream in the distributor, but we start it here anyway.
		stream.start(nil)
	}

	streamInfoMap := genStreamInfoMap(d.streamInfos)
	globalPathMap := make(map[P]struct{}) // Use to check the path duplication

	scheduleRule := NewRoundRobin(3)
	doSchedule := func(rule ruleType, testPeriod time.Duration) {
		// The goal of scheduler is to balance the load of the streams, with mimimum changes.
		// First of all, we have consistent number (baseStreamCount) of basic streams, and unlimited number of solo streams.
		// They are all in the d.streamInfos. The first baseStreamCount streams are the basic streams, and the rest are solo streams.
		// When a path is added, it is assigned to a basic stream with round-robin strategy. It could be imbalance, but we balance it
		// by three rules later:
		// 1. If a stream is too busy, we make the busy path a solo stream.
		// 2. If a solo stream is idle, we combine it into a base stream.
		// 3. If the most busy stream is too busy and the least busy stream is not busy, we shuffle the paths between them.
		// We use round-robin to apply the rules to the streams.
		// Since the number of streams is small, we don't need to worry about the performance of iterating all the streams.

		if rule == createSoloPath {
			newSoloStreamInfos := make([]*streamInfo[P, T, D], 0)
			arranges := make([]*arrangeStreamCmd[P, T, D], 0)
			newStreamInfos := make([]*streamInfo[P, T, D], 0, len(d.streamInfos))

			for i := 0; i < d.baseStreamCount; i++ {
				si := d.streamInfos[i]
				if si.busyRatio(testPeriod) < BusyStreamRatio {
					newStreamInfos = append(newStreamInfos, si)
					continue
				}
				soloStreamInfos := make([]*streamInfo[P, T, D], 0)
				for _, ps := range si.streamStat.mostBusyPath.All() {
					period := si.period()
					if testPeriod != 0 {
						period = testPeriod
					}
					if ps.busyRatio(period) < BusyPathRatio {
						continue
					}
					soloStream := newStream()
					soloStreamInfo := &streamInfo[P, T, D]{
						stream:  soloStream,
						pathMap: map[*pathInfo[P, T, D]]struct{}{ps.pathInfo: {}},
					}

					if _, ok := si.pathMap[ps.pathInfo]; !ok {
						panic("The path should exist in the stream")
					}
					delete(si.pathMap, ps.pathInfo)

					soloStreamInfos = append(soloStreamInfos, soloStreamInfo)
				}

				if len(soloStreamInfos) != 0 {
					newCurrentStream := newStream()
					newCurrentStreamInfo := &streamInfo[P, T, D]{
						stream:  newCurrentStream,
						pathMap: si.pathMap, // The solo paths are removed from the current stream already
					}

					newStreams := make([]*stream[P, T, D], 0, len(soloStreamInfos)+1)
					newStreamPaths := make([][]*pathInfo[P, T, D], 0, len(soloStreamInfos)+1)

					for _, si := range soloStreamInfos {
						newStreams = append(newStreams, si.stream)
						newStreamPaths = append(newStreamPaths, SetToSlice(si.pathMap))
					}
					newStreams = append(newStreams, newCurrentStream)
					newStreamPaths = append(newStreamPaths, SetToSlice(newCurrentStreamInfo.pathMap))

					arranges = append(arranges, &arrangeStreamCmd[P, T, D]{
						oldStreams:     []*stream[P, T, D]{si.stream},
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
			normalSoloStreamInfos := make([]*streamInfo[P, T, D], 0, len(d.streamInfos))

			idleSoloPaths := make([]*pathInfo[P, T, D], 0)
			idleSoloStreams := make([]*stream[P, T, D], 0)
			idleSoloStreamInfos := make([]*streamInfo[P, T, D], 0)
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
				baseStreamInfos := make([]*streamInfo[P, T, D], 0, d.baseStreamCount)
				baseStreamInfos = append(baseStreamInfos, d.streamInfos[:d.baseStreamCount]...)
				sort.Sort(sortedSIs[P, T, D](baseStreamInfos))
				mostIdleStream := baseStreamInfos[0]

				newPaths := make([]*pathInfo[P, T, D], 0, len(idleSoloPaths)+len(mostIdleStream.pathMap))
				newPaths = CopySetToSlice(mostIdleStream.pathMap, newPaths)
				newPaths = append(newPaths, idleSoloPaths...)

				newStream := newStream()
				newStreamInfo := &streamInfo[P, T, D]{
					stream:  newStream,
					pathMap: SliceToSet(newPaths),
				}

				oldStreams := idleSoloStreams[:]
				oldStreams = append(oldStreams, mostIdleStream.stream)

				arrange := &arrangeStreamCmd[P, T, D]{
					oldStreams:     oldStreams,
					newStreams:     []*stream[P, T, D]{newStream},
					newStreamPaths: [][]*pathInfo[P, T, D]{newPaths},
				}

				newStreamInfos := make([]*streamInfo[P, T, D], 0, len(d.streamInfos)-len(idleSoloStreamInfos))
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
			arranges := make([]*arrangeStreamCmd[P, T, D], 0)
			newStreamInfos := make([]*streamInfo[P, T, D], 0, len(d.streamInfos))

			baseStreamInfos := make([]*streamInfo[P, T, D], 0, d.baseStreamCount)
			baseStreamInfos = append(baseStreamInfos, d.streamInfos[:d.baseStreamCount]...)
			sort.Sort(sortedSIs[P, T, D](baseStreamInfos))

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

				pathsChoices := [][]*pathInfo[P, T, D]{make([]*pathInfo[P, T, D], 0, totalPathsCount/2+1), make([]*pathInfo[P, T, D], 0, totalPathsCount/2+1)}
				nextIdx := NewRoundRobin(2)

				// We only fully shuffle the most busy paths from two streams.
				// The reset paths can mostly stay together as before.
				// We would like paths to keep staying in the same stream when possible.
				// It might create some optimization opportunities for golang runtime or the OS.
				addedPaths := 0
				for _, ps := range mostBusy.streamStat.mostBusyPath.All() {
					idx := nextIdx.Next()
					pathsChoices[idx] = append(pathsChoices[idx], ps.pathInfo)
					if _, ok := mostBusy.pathMap[ps.pathInfo]; !ok {
						panic("The path should exist in the stream")
					}
					// To make the following shuffle easier
					delete(mostBusy.pathMap, ps.pathInfo)
					addedPaths++
				}
				for _, ps := range leastBusy.streamStat.mostBusyPath.All() {
					idx := nextIdx.Next()
					pathsChoices[idx] = append(pathsChoices[idx], ps.pathInfo)
					if _, ok := leastBusy.pathMap[ps.pathInfo]; !ok {
						panic("The path should exist in the stream")
					}
					// To make the following shuffle easier
					delete(leastBusy.pathMap, ps.pathInfo)
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

				stream1 := newStream()
				stream1Info := &streamInfo[P, T, D]{
					stream:  stream1,
					pathMap: SliceToSet(stream1Paths),
				}
				stream2 := newStream()
				stream2Info := &streamInfo[P, T, D]{
					stream:  stream2,
					pathMap: SliceToSet(stream2Paths),
				}
				// Note that we should never send pathMap instances to the distributor.
				// Instead, we put the paths to streamXPaths and send it.
				// Because pathMap will be changed later.
				arranges = append(arranges, &arrangeStreamCmd[P, T, D]{
					oldStreams:     []*stream[P, T, D]{mostBusy.stream, leastBusy.stream},
					newStreams:     []*stream[P, T, D]{stream1, stream2},
					newStreamPaths: [][]*pathInfo[P, T, D]{stream1Paths, stream2Paths},
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
	}

	nextSchedule := time.Now().Add(d.option.SchedulerInterval)
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
				add := cmd.cmd.(*addPathCmd[P, T, D])

				// Make sure the paths don't exist already
				for _, pd := range add.paths {
					if _, ok := globalPathMap[pd.Path]; ok {
						add.error = NewAppErrorS(ErrorTypeDuplicate)
						add.wg.Done()
						continue Loop
					}
				}

				pis := make([]*pathInfo[P, T, D], 0, len(add.paths))
				for _, pd := range add.paths {
					pi := newPathInfo[P, T, D](pd.Path, pd.Dest)
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
				remove := cmd.cmd.(*removePathCmd[P])
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
			nextSchedule = time.Now().Add(d.option.SchedulerInterval)
			timerChan = time.After(time.Until(nextSchedule))
			doSchedule(ruleType(scheduleRule.Next()), 0)
		}
	}
}

func (d *dynamicStreamImpl[P, T, D]) distributor() {
	defer d.distDone.Done()

	pathMap := make(map[P]*pathInfo[P, T, D])

	for {
		select {
		case e := <-d.eventChan:
			if pi, ok := pathMap[d.handler.Path(e)]; ok {
				pi.stream.in() <- eventWrap[P, T, D]{event: e, pathInfo: pi}
			}
			// Otherwise, drop the event
		case p := <-d.wakeChan:
			if pi, ok := pathMap[p]; ok {
				pi.stream.in() <- eventWrap[P, T, D]{wake: true, pathInfo: pi}
			}
			// Otherwise, drop the wake signal
		case cmd, ok := <-d.cmdToDist:
			if !ok {
				return
			}
			switch cmd.cmdType {
			case typeAddPath:
				add := cmd.cmd.(*addPathCmd[P, T, D])
				for _, pi := range add.pis {
					if _, ok := pathMap[pi.path]; ok {
						panic(fmt.Sprintf("Path %v already exists in distributor", pi.path))
					}
					pathMap[pi.path] = pi
				}
				add.wg.Done()
			case typeRemovePath:
				remove := cmd.cmd.(*removePathCmd[P])
				for _, p := range remove.paths {
					pi, ok := pathMap[p]
					if ok {
						pi.removed = true
						delete(pathMap, p)
					}
				}
				remove.wg.Done()
			case typeArrangeStream:
				arrange := cmd.cmd.(*arrangeStreamCmd[P, T, D])
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
					newStream.start(paths, arrange.oldStreams...)
				}
			default:
				panic("Unknown command type")
			}
		}
	}
}
