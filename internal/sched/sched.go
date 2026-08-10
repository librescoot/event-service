// Package sched holds pending one-shot fires for deferred rule steps.
//
// A pending step costs one runtime timer and one map entry. It holds no
// worker and no goroutine of its own, which is the whole reason a rule can
// say "then, thirty seconds later" without occupying the pool.
package sched

import (
	"sync"
	"sync/atomic"
	"time"
)

// entry tracks one scheduled fire. claimed is set by whichever of the fire
// callback or the cancel function reaches it first; the other side is then
// a no-op, so fn runs at most once no matter how the timer and the cancel
// interleave.
type entry struct {
	timer   *time.Timer
	claimed atomic.Bool
}

// Scheduler is a registry of pending one-shot timers. The zero value is not
// usable; construct one with New.
type Scheduler struct {
	mu      sync.Mutex
	nextID  uint64
	entries map[uint64]*entry
	stopped bool
}

// New returns a ready Scheduler.
func New() *Scheduler {
	return &Scheduler{
		entries: make(map[uint64]*entry),
	}
}

// At runs fn on its own goroutine after d. The returned function cancels
// the pending fire; it is safe to call any number of times, and reports
// whether it actually prevented the fire.
func (s *Scheduler) At(d time.Duration, fn func()) (cancel func() bool) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return func() bool { return false }
	}

	id := s.nextID
	s.nextID++
	e := &entry{}
	s.entries[id] = e

	e.timer = time.AfterFunc(d, func() {
		if !e.claimed.CompareAndSwap(false, true) {
			return
		}
		s.remove(id)
		fn()
	})
	s.mu.Unlock()

	return func() bool {
		if !e.claimed.CompareAndSwap(false, true) {
			return false
		}
		e.timer.Stop()
		s.remove(id)
		return true
	}
}

// remove drops id from the registry if still present.
func (s *Scheduler) remove(id uint64) {
	s.mu.Lock()
	delete(s.entries, id)
	s.mu.Unlock()
}

// Pending returns the number of fires that have not yet run or been
// cancelled. An entry leaves the count as soon as fire-or-cancel is claimed,
// before fn runs, so a fire already in progress on its own goroutine is not
// included; Pending() reaching 0 does not mean every fn has returned.
func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// Stop cancels every pending fire. It does not wait for a fire already in
// progress; the caller owns that.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	s.stopped = true
	entries := s.entries
	s.entries = make(map[uint64]*entry)
	s.mu.Unlock()

	for _, e := range entries {
		if e.claimed.CompareAndSwap(false, true) {
			e.timer.Stop()
		}
	}
}
