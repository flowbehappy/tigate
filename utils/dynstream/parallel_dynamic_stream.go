package dynstream

// Use a hasher to select target stream for the path.
// It implements the DynamicStream interface.
type parallelDynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	pathHasher     PathHasher[P]
	dynamicStreams []*dynamicStreamImpl[A, P, T, D, H]
	feedbackChan   chan Feedback[A, P, D]
}

func newParallelDynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](streamCount int, hasher PathHasher[P], handler H, option Option) *parallelDynamicStream[A, P, T, D, H] {
	s := &parallelDynamicStream[A, P, T, D, H]{
		pathHasher: hasher,
	}
	if option.EnableMemoryControl {
		s.feedbackChan = make(chan Feedback[A, P, D], 1024)
	}
	for range streamCount {
		s.dynamicStreams = append(s.dynamicStreams, newDynamicStreamImpl(handler, option, s.feedbackChan))
	}
	return s
}

func (s *parallelDynamicStream[A, P, T, D, H]) Start() {
	for _, ds := range s.dynamicStreams {
		ds.Start()
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) Close() {
	for _, ds := range s.dynamicStreams {
		ds.Close()
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) hash(path ...P) int {
	if len(path) == 0 {
		panic("no path")
	}
	hash := s.pathHasher.HashPath(path[0])
	return int(hash % uint64(len(s.dynamicStreams)))
}

func (s *parallelDynamicStream[A, P, T, D, H]) In(path ...P) chan<- T {
	return s.dynamicStreams[s.hash(path...)].In()
}

func (s *parallelDynamicStream[A, P, T, D, H]) Wake(path ...P) chan<- P {
	return s.dynamicStreams[s.hash(path...)].Wake()
}

func (s *parallelDynamicStream[A, P, T, D, H]) Feedback() <-chan Feedback[A, P, D] {
	return s.feedbackChan
}

func (s *parallelDynamicStream[A, P, T, D, H]) AddPath(path P, dest D, area ...AreaSettings) error {
	return s.dynamicStreams[s.hash(path)].AddPath(path, dest, area...)
}

func (s *parallelDynamicStream[A, P, T, D, H]) RemovePath(path P) error {
	return s.dynamicStreams[s.hash(path)].RemovePath(path)
}

func (s *parallelDynamicStream[A, P, T, D, H]) SetAreaSettings(area A, settings AreaSettings) {
	for _, ds := range s.dynamicStreams {
		ds.SetAreaSettings(area, settings)
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) GetMetrics() Metrics {
	metrics := Metrics{}
	for _, ds := range s.dynamicStreams {
		subMetrics := ds.GetMetrics()
		metrics.EventChanSize += subMetrics.EventChanSize
		metrics.PendingQueueLen += subMetrics.PendingQueueLen
		metrics.AddPath += subMetrics.AddPath
		metrics.RemovePath += subMetrics.RemovePath
		metrics.ArrangeStream.CreateSolo += subMetrics.ArrangeStream.CreateSolo
		metrics.ArrangeStream.RemoveSolo += subMetrics.ArrangeStream.RemoveSolo
		metrics.ArrangeStream.Shuffle += subMetrics.ArrangeStream.Shuffle
	}
	return metrics
}
