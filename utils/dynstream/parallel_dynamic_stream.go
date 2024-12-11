package dynstream

import (
	"reflect"
	"sync"
	"unsafe"

	. "github.com/pingcap/ticdc/pkg/apperror"
)

// Use a hasher to select target stream for the path.
// It implements the DynamicStream interface.
type parallelDynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	handler    H
	pathHasher PathHasher[P]
	streams    []*stream[A, P, T, D, H]
	pathMap    map[P]*pathInfo[A, P, T, D, H]

	eventExtraSize int
	memControl     *memControl[A, P, T, D, H] // TODO: implement memory control

	mutex sync.RWMutex

	feedbackChan chan Feedback[A, P, D]

	_statAddPathCount    int
	_statRemovePathCount int
}

func newParallelDynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](hasher PathHasher[P], handler H, option Option) *parallelDynamicStream[A, P, T, D, H] {
	option.fix()
	eventExtraSize := 0
	var zero T
	if reflect.TypeOf(zero).Kind() == reflect.Pointer {
		eventExtraSize = int(unsafe.Sizeof(eventWrap[A, P, T, D, H]{}))
	} else {
		a := unsafe.Sizeof(eventWrap[A, P, T, D, H]{})
		b := unsafe.Sizeof(zero)
		eventExtraSize = int(a - b)
	}

	s := &parallelDynamicStream[A, P, T, D, H]{
		handler:        handler,
		pathHasher:     hasher,
		pathMap:        make(map[P]*pathInfo[A, P, T, D, H]),
		eventExtraSize: eventExtraSize,
	}
	if option.EnableMemoryControl {
		s.feedbackChan = make(chan Feedback[A, P, D], 1024)
	}
	for i := range option.StreamCount {
		s.streams = append(s.streams, newStream(i, handler, nil, 0, option))
	}
	return s
}

func (s *parallelDynamicStream[A, P, T, D, H]) Start() {
	for _, ds := range s.streams {
		ds.start(nil)
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) Close() {
	for _, ds := range s.streams {
		ds.close()
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) hash(path P) int {
	hash := s.pathHasher(path)
	return int(hash % uint64(len(s.streams)))
}

func (s *parallelDynamicStream[A, P, T, D, H]) Push(path P, e T) {
	var pi *pathInfo[A, P, T, D, H]
	var ok bool
	{
		s.mutex.RLock()
		defer s.mutex.RUnlock()
		if pi, ok = s.pathMap[path]; !ok {
			s.handler.OnDrop(e)
			return
		}
	}

	ew := eventWrap[A, P, T, D, H]{
		event:     e,
		pathInfo:  pi,
		paused:    s.handler.IsPaused(e),
		eventType: s.handler.GetType(e),
		eventSize: s.eventExtraSize + s.handler.GetSize(e),
	}
	pi.stream.in().Push(ew)
}

func (s *parallelDynamicStream[A, P, T, D, H]) Wake(path P) {
	var pi *pathInfo[A, P, T, D, H]
	var ok bool
	{
		s.mutex.RLock()
		defer s.mutex.RUnlock()
		if pi, ok = s.pathMap[path]; !ok {
			return
		}
	}

	pi.stream.in().Push(eventWrap[A, P, T, D, H]{wake: true, pathInfo: pi})
}

func (s *parallelDynamicStream[A, P, T, D, H]) Feedback() <-chan Feedback[A, P, D] {
	return s.feedbackChan
}

func (s *parallelDynamicStream[A, P, T, D, H]) AddPath(path P, dest D, as ...AreaSettings) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, ok := s.pathMap[path]
	if ok {
		return NewAppErrorS(ErrorTypeDuplicate)
	}

	area := s.handler.GetArea(path, dest)
	pi := newPathInfo[A, P, T, D, H](area, path, dest)
	pi.setStream(s.streams[s.hash(path)])
	s.pathMap[path] = pi
	if s.memControl != nil {
		setting := AreaSettings{}
		if len(as) > 0 {
			setting = as[0]
		}
		s.memControl.addPathToArea(pi, setting, s.feedbackChan)
	}
	pi.stream.in().Push(eventWrap[A, P, T, D, H]{pathInfo: pi, newPath: true})

	s._statAddPathCount++
	return nil
}

func (s *parallelDynamicStream[A, P, T, D, H]) RemovePath(path P) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	pi, ok := s.pathMap[path]
	if !ok {
		return NewAppErrorS(ErrorTypeNotExist)
	}

	pi.removed = true

	if s.memControl != nil {
		s.memControl.removePathFromArea(pi)
	}
	pi.stream.in().Push(eventWrap[A, P, T, D, H]{pathInfo: pi})
	delete(s.pathMap, path)

	s._statRemovePathCount++
	return nil
}

func (s *parallelDynamicStream[A, P, T, D, H]) SetAreaSettings(area A, settings AreaSettings) {
	if s.memControl != nil {
		s.memControl.setAreaSettings(area, settings)
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) GetMetrics() Metrics {
	metrics := Metrics{}
	for _, ds := range s.streams {
		size := ds.getPendingSize()
		metrics.PendingQueueLen += size
	}
	metrics.AddPath = s._statAddPathCount
	metrics.RemovePath = s._statRemovePathCount
	return metrics
}
