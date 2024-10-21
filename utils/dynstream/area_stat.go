package dynstream

// AreaStat is used to store the statistics of an area.
// It is a global level struct, not stream level.
type areaStat[A Area] struct {
	area A
}
