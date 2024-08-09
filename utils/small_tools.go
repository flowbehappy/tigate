package utils

type Switcher struct {
	value bool
}

func NewSwitcher(initial ...bool) *Switcher {
	v := false
	if len(initial) > 0 {
		v = initial[0]
	}
	return &Switcher{
		value: v,
	}
}

// Switch the value and return the old value.
func (s *Switcher) Switch() bool {
	v := s.value
	s.value = !s.value
	return v
}

type RoundRobin struct {
	index int
	cap   int
}

func NewRoundRobin(cap int, initial ...int) *RoundRobin {
	v := 0
	if len(initial) > 0 {
		v = initial[0]
	}
	return &RoundRobin{
		index: v,
		cap:   cap,
	}
}

// Move to next index and return the old index.
func (r *RoundRobin) Next() int {
	i := r.index
	r.index = (r.index + 1) % r.cap
	return i
}

func SetToSlice[E comparable](m map[E]struct{}) []E {
	s := make([]E, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	return s
}

func SliceToSet[E comparable](s []E) map[E]struct{} {
	m := make(map[E]struct{}, len(s))
	for _, k := range s {
		m[k] = struct{}{}
	}
	return m
}

func OneInSet[E comparable](s map[E]struct{}) E {
	for k := range s {
		return k
	}
	panic("Empty set")
}

func OneInMap[K comparable, V any](m map[K]V) (K, V) {
	for k, v := range m {
		return k, v
	}
	panic("Empty map")
}

func CopyMapToMap[K comparable, V any](from map[K]V, to map[K]V) {
	for k, v := range from {
		to[k] = v
	}
}

func CopySetToSet[E comparable](from map[E]struct{}, to map[E]struct{}) {
	for k := range from {
		to[k] = struct{}{}
	}
}

func CopySliceToSet[E comparable](from []E, to map[E]struct{}) {
	for _, k := range from {
		to[k] = struct{}{}
	}
}

func CopySetToSlice[E comparable](from map[E]struct{}, to []E) []E {
	for k := range from {
		to = append(to, k)
	}
	return to
}
