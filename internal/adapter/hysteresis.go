package adapter

import "sync"

// Thresholds reports when a value crosses one of a set of levels, with a
// margin so that a reading sitting on a boundary does not fire repeatedly.
//
// Charge readings jitter by a percent or two. cb-battery updates at roughly
// 1.7 Hz, so an unguarded comparison against a level would emit an event on
// most polls once the pack settled near it.
type Thresholds struct {
	mu     sync.Mutex
	levels []int
	margin int
	last   map[string]int
	seen   map[string]bool
}

// NewThresholds returns a Thresholds for the given levels. margin is how far
// past a level the value must move before that level can fire again.
func NewThresholds(levels []int, margin int) *Thresholds {
	cp := make([]int, len(levels))
	copy(cp, levels)
	return &Thresholds{
		levels: cp,
		margin: margin,
		last:   make(map[string]int),
		seen:   make(map[string]bool),
	}
}

// Cross records value for key and reports the level it just crossed, if any.
//
// The first observation for a key never fires: there is no previous value, so
// there is no crossing, only a starting point.
func (t *Thresholds) Cross(key string, value int) (level int, crossed bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	prev, ok := t.last[key]
	if !ok || !t.seen[key] {
		t.last[key] = value
		t.seen[key] = true
		return 0, false
	}

	for _, l := range t.levels {
		switch {
		case prev >= l && value < l:
			t.last[key] = value
			return l, true
		case prev <= l && value > l+t.margin:
			t.last[key] = value
			return l, true
		}
	}

	// Only update last[key] if value is clear of all margin bands.
	// This prevents re-firing when value jitters near a level.
	for _, l := range t.levels {
		if value >= l-t.margin && value <= l+t.margin {
			return 0, false
		}
	}
	t.last[key] = value
	return 0, false
}
