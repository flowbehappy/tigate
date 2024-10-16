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

type command struct {
	cmdType cmdType
	cmd     interface{}
}

type addPathCmd[A Area, P Path, T Event, D Dest] struct {
	paths  []PathAndDest[P, D]
	pis    []*pathInfo[A, P, T, D]
	errors []error

	wg sync.WaitGroup
}

type removePathCmd[P Path] struct {
	paths      []P
	existPaths []P
	errors     []error

	wg sync.WaitGroup
}

type arrangeStreamCmd[A Area, P Path, T Event, D Dest] struct {
	oldStreams []*stream[A, P, T, D]

	newStreams     []*stream[A, P, T, D]
	newStreamPaths [][]*pathInfo[A, P, T, D]
}

type reportAndScheduleCmd struct {
	rule   ruleType
	period time.Duration
	wg     sync.WaitGroup
}

// Use to store the statistics of a stream
// It is only used by the scheduler
type streamInfo[A Area, P Path, T Event, D Dest] struct {
	stream     *stream[A, P, T, D]
	streamStat streamStat[A, P, T, D]
	pathMap    map[*pathInfo[A, P, T, D]]struct{}
}

func (si *streamInfo[A, P, T, D]) runtime() time.Duration {
	return si.streamStat.totalTime
}

func (si *streamInfo[A, P, T, D]) busyRatio(period time.Duration) float64 {
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

func (si *streamInfo[A, P, T, D]) period() time.Duration {
	return si.streamStat.period
}

type sortedSIs[A Area, P Path, T Event, D Dest] []*streamInfo[A, P, T, D]

// implement sort.Interface
func (s sortedSIs[A, P, T, D]) Len() int           { return len(s) }
func (s sortedSIs[A, P, T, D]) Less(i, j int) bool { return s[i].runtime() < s[j].runtime() }
func (s sortedSIs[A, P, T, D]) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// This is the implementation of the DynamicStream interface.
// We use two goroutines
// 1. The distributor to distribute the events to the streams
// 2. The scheduler to balance the load of the streams
//
// A stream can handle events from multiple paths.
// Events from the same path are only processed by one particular stream at the same time.
// The scheduler use several strategies to balance the load of the streams, while the final balanace
// actions are moving the paths between the streams.
type dynamicStreamImpl[A Area, P Path, T Event, D Dest] struct {
	trackTopPaths   int
	baseStreamCount int

	handler Handler[P, T, D]
	option  OptionEnhanced[A, P, T, D]

	eventChan chan T // The channel to receive the incomming events by distributor
	wakeChan  chan P // The channel to receive the wake signal by distributor

	reportChan chan streamStat[A, P, T, D] // The channel to receive the report by scheduler
	cmdToSchd  chan *command               // Send the commands to the scheduler
	cmdToDist  chan *command               // Send the commands to the distributor

	// The streams to handle the events. Only used in the scheduler.
	// We put it here mainly to make the tests easier.
	streamInfos []*streamInfo[A, P, T, D]

	hasClosed atomic.Bool

	schdDone sync.WaitGroup
	distDone sync.WaitGroup

	startTime time.Time
}

func newDynamicStreamImpl[A Area, P Path, T Event, D Dest](
	handler Handler[P, T, D],
	option OptionEnhanced[A, P, T, D],
) *dynamicStreamImpl[A, P, T, D] {
	option.fix()
	return &dynamicStreamImpl[A, P, T, D]{
		handler: handler,
		option:  option,

		trackTopPaths:   TrackTopPaths,
		baseStreamCount: option.StreamCount,

		eventChan:  make(chan T, 1024),
		wakeChan:   make(chan P, 64),
		reportChan: make(chan streamStat[A, P, T, D], 64),
		cmdToSchd:  make(chan *command, 64),
		cmdToDist:  make(chan *command, option.StreamCount),

		streamInfos: make([]*streamInfo[A, P, T, D], 0, option.StreamCount),
		startTime:   time.Now(),
	}
}

func (d *dynamicStreamImpl[A, P, T, D]) In() chan<- T {
	return d.eventChan
}

func (d *dynamicStreamImpl[A, P, T, D]) Wake() chan<- P {
	return d.wakeChan
}

func (d *dynamicStreamImpl[A, P, T, D]) Start() {
	d.schdDone.Add(1)
	go d.scheduler()
	d.distDone.Add(1)
	go d.distributor()
}

func (d *dynamicStreamImpl[A, P, T, D]) Close() {
	if d.hasClosed.CompareAndSwap(false, true) {
		close(d.cmdToSchd)
	}
	d.schdDone.Wait()
}

func (d *dynamicStreamImpl[A, P, T, D]) AddPaths(paths ...PathAndDest[P, D]) []error {
	add := &addPathCmd[A, P, T, D]{paths: paths}
	cmd := &command{
		cmdType: typeAddPath,
		cmd:     add,
	}
	add.wg.Add(2) // need to wait for both scheduler and distributor
	d.cmdToSchd <- cmd
	add.wg.Wait()
	return add.errors
}

func (d *dynamicStreamImpl[A, P, T, D]) AddPath(path P, dest D) error {
	errors := d.AddPaths(PathAndDest[P, D]{Path: path, Dest: dest})
	if len(errors) != 0 {
		return errors[0]
	}
	return nil
}

func (d *dynamicStreamImpl[A, P, T, D]) RemovePaths(paths ...P) []error {
	remove := &removePathCmd[P]{paths: paths}
	cmd := &command{
		cmdType: typeRemovePath,
		cmd:     remove,
	}
	remove.wg.Add(2) // need to wait for both scheduler and distributor
	d.cmdToSchd <- cmd
	remove.wg.Wait()
	return remove.errors
}

func (d *dynamicStreamImpl[A, P, T, D]) RemovePath(path P) error {
	errs := d.RemovePaths(path)
	if len(errs) != 0 {
		return errs[0]
	}
	return nil
}

func (d *dynamicStreamImpl[A, P, T, D]) scheduler() {
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

	newStream := func() *stream[A, P, T, D] {
		nextStreamId++
		return newStream[A, P, T, D](nextStreamId, d.handler, d.reportChan, d.trackTopPaths, d.option)
	}
	nextStream := func() *streamInfo[A, P, T, D] {
		// We use round-robin to assign the paths to the streams
		s := d.streamInfos[nextStreamIndex.Next()]
		return s
	}
	genStreamInfoMap := func(sis []*streamInfo[A, P, T, D]) map[int]*streamInfo[A, P, T, D] {
		m := make(map[int]*streamInfo[A, P, T, D], len(d.streamInfos))
		for _, si := range sis {
			m[si.stream.id] = si
		}
		return m
	}

	for i := 0; i < d.baseStreamCount; i++ {
		stream := newStream()
		si := &streamInfo[A, P, T, D]{
			stream:  stream,
			pathMap: make(map[*pathInfo[A, P, T, D]]struct{}),
		}

		d.streamInfos = append(d.streamInfos, si)

		// Strictly speaking, we should start the stream in the distributor, but we start it here anyway.
		stream.start(nil)
	}

	streamInfoMap := genStreamInfoMap(d.streamInfos)
	globalPathMap := make(map[P]*pathInfo[A, P, T, D])

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
			newSoloStreamInfos := make([]*streamInfo[A, P, T, D], 0)
			arranges := make([]*arrangeStreamCmd[A, P, T, D], 0)
			newStreamInfos := make([]*streamInfo[A, P, T, D], 0, len(d.streamInfos))

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
				soloStreamInfos := make([]*streamInfo[A, P, T, D], 0)
				for _, ps := range si.streamStat.getMostBusyPaths() {
					if ps.busyRatio(period) < BusyPathRatio {
						continue
					}
					soloStream := newStream()
					soloStreamInfo := &streamInfo[A, P, T, D]{
						stream:  soloStream,
						pathMap: map[*pathInfo[A, P, T, D]]struct{}{ps.pathInfo: {}},
					}

					if _, ok := si.pathMap[ps.pathInfo]; !ok {
						panic("The path should exist in the stream")
					}
					delete(si.pathMap, ps.pathInfo)

					soloStreamInfos = append(soloStreamInfos, soloStreamInfo)
				}

				if len(soloStreamInfos) != 0 {
					newCurrentStream := newStream()
					newCurrentStreamInfo := &streamInfo[A, P, T, D]{
						stream:  newCurrentStream,
						pathMap: si.pathMap, // The solo paths are removed from the current stream already
					}

					newStreams := make([]*stream[A, P, T, D], 0, len(soloStreamInfos)+1)
					newStreamPaths := make([][]*pathInfo[A, P, T, D], 0, len(soloStreamInfos)+1)

					for _, si := range soloStreamInfos {
						newStreams = append(newStreams, si.stream)
						newStreamPaths = append(newStreamPaths, SetToSlice(si.pathMap))
					}
					newStreams = append(newStreams, newCurrentStream)
					newStreamPaths = append(newStreamPaths, SetToSlice(newCurrentStreamInfo.pathMap))

					arranges = append(arranges, &arrangeStreamCmd[A, P, T, D]{
						oldStreams:     []*stream[A, P, T, D]{si.stream},
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

				for _, arrange := range arranges {
					cmd := &command{
						cmdType: typeArrangeStream,
						cmd:     arrange,
					}
					d.cmdToDist <- cmd
				}
			}
		} else if rule == removeSoloPath {
			normalSoloStreamInfos := make([]*streamInfo[A, P, T, D], 0, len(d.streamInfos))

			idleSoloPaths := make([]*pathInfo[A, P, T, D], 0)
			idleSoloStreams := make([]*stream[A, P, T, D], 0)
			idleSoloStreamInfos := make([]*streamInfo[A, P, T, D], 0)
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
				baseStreamInfos := make([]*streamInfo[A, P, T, D], 0, d.baseStreamCount)
				baseStreamInfos = append(baseStreamInfos, d.streamInfos[:d.baseStreamCount]...)
				sort.Sort(sortedSIs[A, P, T, D](baseStreamInfos))
				mostIdleStream := baseStreamInfos[0]

				newPaths := make([]*pathInfo[A, P, T, D], 0, len(idleSoloPaths)+len(mostIdleStream.pathMap))
				newPaths = CopySetToSlice(mostIdleStream.pathMap, newPaths)
				newPaths = append(newPaths, idleSoloPaths...)

				newStream := newStream()
				newStreamInfo := &streamInfo[A, P, T, D]{
					stream:  newStream,
					pathMap: SliceToSet(newPaths),
				}

				oldStreams := idleSoloStreams[:]
				oldStreams = append(oldStreams, mostIdleStream.stream)

				arrange := &arrangeStreamCmd[A, P, T, D]{
					oldStreams:     oldStreams,
					newStreams:     []*stream[A, P, T, D]{newStream},
					newStreamPaths: [][]*pathInfo[A, P, T, D]{newPaths},
				}

				newStreamInfos := make([]*streamInfo[A, P, T, D], 0, len(d.streamInfos)-len(idleSoloStreamInfos))
				newStreamInfos = append(newStreamInfos, newStreamInfo)
				newStreamInfos = append(newStreamInfos, baseStreamInfos[1:]...)
				newStreamInfos = append(newStreamInfos, normalSoloStreamInfos...)

				d.streamInfos = newStreamInfos
				streamInfoMap = genStreamInfoMap(newStreamInfos)

				d.cmdToDist <- &command{
					cmdType: typeArrangeStream,
					cmd:     arrange,
				}
			}
		} else if rule == shuffleStreams {
			arranges := make([]*arrangeStreamCmd[A, P, T, D], 0)
			newStreamInfos := make([]*streamInfo[A, P, T, D], 0, len(d.streamInfos))

			baseStreamInfos := make([]*streamInfo[A, P, T, D], 0, d.baseStreamCount)
			baseStreamInfos = append(baseStreamInfos, d.streamInfos[:d.baseStreamCount]...)
			sort.Sort(sortedSIs[A, P, T, D](baseStreamInfos))

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

				pathsChoices := [][]*pathInfo[A, P, T, D]{make([]*pathInfo[A, P, T, D], 0, totalPathsCount/2+1), make([]*pathInfo[A, P, T, D], 0, totalPathsCount/2+1)}
				nextIdx := NewRoundRobin(2)

				// We only fully shuffle the most busy paths from two streams.
				// The reset paths can mostly stay together as before.
				// We would like paths to keep staying in the same stream when possible.
				// It might create some optimization opportunities for golang runtime or the OS.
				addedPaths := 0
				for _, ps := range mostBusy.streamStat.getMostBusyPaths() {
					idx := nextIdx.Next()
					pathsChoices[idx] = append(pathsChoices[idx], ps.pathInfo)
					if _, ok := mostBusy.pathMap[ps.pathInfo]; !ok {
						panic("The path should exist in the stream")
					}
					// To make the following shuffle easier
					delete(mostBusy.pathMap, ps.pathInfo)
					addedPaths++
				}
				for _, ps := range leastBusy.streamStat.getMostBusyPaths() {
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
				stream1Info := &streamInfo[A, P, T, D]{
					stream:  stream1,
					pathMap: SliceToSet(stream1Paths),
				}
				stream2 := newStream()
				stream2Info := &streamInfo[A, P, T, D]{
					stream:  stream2,
					pathMap: SliceToSet(stream2Paths),
				}
				// Note that we should never send pathMap instances to the distributor.
				// Instead, we put the paths to streamXPaths and send it.
				// Because pathMap will be changed later.
				arranges = append(arranges, &arrangeStreamCmd[A, P, T, D]{
					oldStreams:     []*stream[A, P, T, D]{mostBusy.stream, leastBusy.stream},
					newStreams:     []*stream[A, P, T, D]{stream1, stream2},
					newStreamPaths: [][]*pathInfo[A, P, T, D]{stream1Paths, stream2Paths},
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
					cmd := &command{
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
	for {
		select {
		case cmd, ok := <-d.cmdToSchd:
			if !ok {
				return
			}
			switch cmd.cmdType {
			case typeAddPath:
				add := cmd.cmd.(*addPathCmd[A, P, T, D])
				add.pis = make([]*pathInfo[A, P, T, D], 0, len(add.paths))
				errors := make([]error, 0, len(add.paths))
				hasError := false
				for _, pd := range add.paths {
					if _, ok := globalPathMap[pd.Path]; ok {
						errors = append(errors, NewAppErrorS(ErrorTypeDuplicate))
						hasError = true
					} else {
						var area A
						if d.option.AreaGetter != nil {
							area = d.option.AreaGetter.PathArea(pd.Path)
						}
						pi := newPathInfo[A, P, T, D](area, pd.Path, pd.Dest)
						si := nextStream()
						pi.stream = si.stream
						si.pathMap[pi] = struct{}{}
						globalPathMap[pd.Path] = pi

						add.pis = append(add.pis, pi)
						errors = append(errors, nil)
					}
				}

				if hasError {
					add.errors = errors
				}
				add.wg.Done()

				d.cmdToDist <- cmd
			case typeRemovePath:
				remove := cmd.cmd.(*removePathCmd[P])
				remove.existPaths = make([]P, 0, len(remove.paths))
				errors := make([]error, 0, len(remove.paths))
				hasError := false
				e := NewAppErrorS(ErrorTypeNotExist)

				for _, p := range remove.paths {
					pi, ok := globalPathMap[p]
					if !ok {
						errors = append(errors, e)
						hasError = true
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
						delete(globalPathMap, p)

						remove.existPaths = append(remove.existPaths, p)
						errors = append(errors, nil)

						// If it is a solo path, we don't need to remove the empty solo stream in here.
						// The empty solo stream will be removed in the removeSoloPath rule.
					}
				}
				if hasError {
					remove.errors = errors
				}
				remove.wg.Done()

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

func (d *dynamicStreamImpl[A, P, T, D]) distributor() {
	defer d.distDone.Done()

	pathMap := make(map[P]*pathInfo[A, P, T, D])

	for {
		select {
		case e := <-d.eventChan:
			if pi, ok := pathMap[d.handler.Path(e)]; ok {
				e := eventWrap[A, P, T, D]{
					event:     e,
					pathInfo:  pi,
					queueTime: time.Now(),
				}
				if d.option.TypeGetter != nil {
					e.eType = d.option.TypeGetter.Type(e.event)
				}
				if d.option.TimestampGetter != nil {
					e.timestamp = d.option.TimestampGetter.Timestamp(e.event)
				} else {
					e.timestamp = (Timestamp)(e.queueTime.Sub(d.startTime))
				}
				pi.stream.in() <- e
			}
			// Otherwise, drop the event
		case p := <-d.wakeChan:
			if pi, ok := pathMap[p]; ok {
				pi.stream.in() <- eventWrap[A, P, T, D]{wake: true, pathInfo: pi}
			}
			// Otherwise, drop the wake signal
		case cmd, ok := <-d.cmdToDist:
			if !ok {
				return
			}
			switch cmd.cmdType {
			case typeAddPath:
				add := cmd.cmd.(*addPathCmd[A, P, T, D])
				for _, pi := range add.pis {
					if _, ok := pathMap[pi.path]; ok {
						panic(fmt.Sprintf("Path %v already exists in distributor", pi.path))
					}
					pathMap[pi.path] = pi
					pi.stream.addPath(pi)
				}
				add.wg.Done()
			case typeRemovePath:
				remove := cmd.cmd.(*removePathCmd[P])
				for _, p := range remove.existPaths {
					pi, ok := pathMap[p]
					if ok {
						pi.removed = true
						delete(pathMap, p)
						pi.stream.removePath(pi)
						// Don't close the stream here. The stream is processing other paths.
					} else {
						panic(fmt.Sprintf("Path %v doesn't exist in distributor", p))
					}
				}
				remove.wg.Done()
			case typeArrangeStream:
				arrange := cmd.cmd.(*arrangeStreamCmd[A, P, T, D])
				for i, paths := range arrange.newStreamPaths {
					newStream := arrange.newStreams[i]
					for _, pi := range paths {
						if _, ok := pathMap[pi.path]; !ok {
							panic(fmt.Sprintf("Path %v doesn't exist in distributor", pi.path))
						}
						pi.stream.removePath(pi)
						newStream.addPath(pi)
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
