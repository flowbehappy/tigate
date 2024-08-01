package dynstream

import "container/heap"

type statAndIndex struct {
	stream *streamStat
	index  int
}

type streamHeap struct {
	min     bool
	streams []*statAndIndex
}

func newStreamMinHeap(min bool) *streamHeap { return newStreamHeap(true) }
func newStreamMaxHeap(min bool) *streamHeap { return newStreamHeap(false) }
func newStreamHeap(min bool) *streamHeap {
	return &streamHeap{
		min:     min,
		streams: make([]*statAndIndex, 0),
	}
}

// ====================================
// Notice: Don't call those methods below directly! They are only called by heap package.

func (h *streamHeap) Len() int { return len(h.streams) }
func (h *streamHeap) Less(i, j int) bool {
	if h.min {
		return h.streams[i].stream.futureWaitInPeriod < h.streams[j].stream.futureWaitInPeriod
	} else {
		return h.streams[i].stream.futureWaitInPeriod > h.streams[j].stream.futureWaitInPeriod
	}
}
func (h *streamHeap) Swap(i, j int) {
	h.streams[i], h.streams[j] = h.streams[j], h.streams[i]
	h.streams[i].index, h.streams[j].index = h.streams[j].index, h.streams[i].index
}
func (h *streamHeap) Push(x interface{}) {
	index := len(h.streams)
	h.streams = append(h.streams, x.(*statAndIndex))
	h.streams[index].index = index
}

// Pop the last element of the slice.
func (h *streamHeap) Pop() interface{} {
	old := h.streams
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.streams = old[0 : n-1]
	return x
}

// ====================================

func (h *streamHeap) addOrUpdateTask(st *statAndIndex) {
	if st.index >= 0 {
		heap.Fix(h, st.index)
	} else {
		heap.Push(h, st)
	}
}

func (h *streamHeap) popTopTask() *statAndIndex {
	if h.Len() == 0 {
		return nil
	}
	st := heap.Pop(h).(*statAndIndex)
	st.index = -1 // Mark the task is not in the heap
	return st
}
func (h *streamHeap) peekTopTask() *statAndIndex {
	if h.Len() == 0 {
		return nil
	}
	return h.streams[0]
}

func (h *streamHeap) removeTask(st *statAndIndex) bool {
	if st.index >= 0 {
		heap.Remove(h, st.index)
		st.index = -1 // Mark the task is not in the heap
		return true
	}
	return false
}
