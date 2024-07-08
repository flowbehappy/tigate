package schemastore

// Flag is a uint64 flag to show a 64 bit mask
type Flag uint64

// HasAll means has all flags
func (f *Flag) HasAll(flags ...Flag) bool {
	for _, flag := range flags {
		if flag&*f == 0 {
			return false
		}
	}
	return true
}

// HasOne means has one of the flags
func (f *Flag) HasOne(flags ...Flag) bool {
	for _, flag := range flags {
		if flag&*f != 0 {
			return true
		}
	}
	return false
}

// Add add flags
func (f *Flag) Add(flags ...Flag) {
	for _, flag := range flags {
		*f |= flag
	}
}

// Remove remove flags
func (f *Flag) Remove(flags ...Flag) {
	for _, flag := range flags {
		*f ^= flag
	}
}

// Clear clear all flags
func (f *Flag) Clear() {
	*f ^= *f
}
