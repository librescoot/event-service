package sched

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestAtFires(t *testing.T) {
	s := New()
	defer s.Stop()
	fired := make(chan struct{})
	s.At(time.Millisecond, func() { close(fired) })
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("timer did not fire")
	}
}

func TestCancelPreventsFire(t *testing.T) {
	s := New()
	defer s.Stop()
	var n atomic.Int32
	cancel := s.At(time.Hour, func() { n.Add(1) })
	if !cancel() {
		t.Fatal("cancel should report that it prevented the fire")
	}
	if cancel() {
		t.Fatal("second cancel should report false, not double-count")
	}
	if got := n.Load(); got != 0 {
		t.Fatalf("fn ran %d times, want 0", got)
	}
}

// Cancel racing an imminent fire must resolve one way or the other, never
// both: a run that is cancelled and also advances would double-dispatch.
//
// At these timings the Go runtime already serialises Stop against a fresh
// AfterFunc timer's own fire internally, so this loop passes even against
// an implementation that drops the atomic claim and relies only on
// Timer.Stop()'s return value. It cannot by itself prove the claim is doing
// anything, and it is not the test that does:
// TestAClaimedEntryDoesNotFireWhenItsTimerLandsAnyway reaches the window
// directly instead of waiting for the runtime to hand it over. What this one
// covers is the ordinary race, end to end, at timings a rule actually meets.
func TestCancelRacingFireRunsFnAtMostOnce(t *testing.T) {
	for i := 0; i < 200; i++ {
		s := New()
		var n atomic.Int32
		cancel := s.At(time.Millisecond, func() { n.Add(1) })
		go cancel()
		time.Sleep(3 * time.Millisecond)
		if got := n.Load(); got > 1 {
			t.Fatalf("fn ran %d times, want at most 1", got)
		}
		s.Stop()
	}
}

func TestPendingCountsAndDrains(t *testing.T) {
	s := New()
	cancel := s.At(time.Hour, func() {})
	s.At(time.Hour, func() {})
	if got := s.Pending(); got != 2 {
		t.Fatalf("Pending() = %d, want 2", got)
	}
	cancel()
	if got := s.Pending(); got != 1 {
		t.Fatalf("after cancel Pending() = %d, want 1", got)
	}
	s.Stop()
	if got := s.Pending(); got != 0 {
		t.Fatalf("after Stop Pending() = %d, want 0", got)
	}
}

func TestStopPreventsPendingFires(t *testing.T) {
	s := New()
	var n atomic.Int32
	s.At(20*time.Millisecond, func() { n.Add(1) })
	s.Stop()
	time.Sleep(80 * time.Millisecond)
	if got := n.Load(); got != 0 {
		t.Fatalf("fn ran %d times after Stop, want 0", got)
	}
}

// A fired entry must not stay in the registry, or a long-lived process
// accumulates one map entry per event forever.
func TestFiredEntryIsRemoved(t *testing.T) {
	s := New()
	defer s.Stop()
	fired := make(chan struct{})
	s.At(time.Millisecond, func() { close(fired) })
	<-fired
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.Pending() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Pending() = %d after fire, want 0", s.Pending())
}

// TestAClaimedEntryDoesNotFireWhenItsTimerLandsAnyway pins the claim on the
// fire side. A cancel arriving after AfterFunc has already been triggered
// cannot take the fire back through Timer.Stop, which returns false and does
// nothing; the claim is the only thing between that cancel and fn running
// anyway, and a cancel that reported it prevented the fire while the fire
// happened is a run that is both cancelled and still advancing.
//
// The entry is claimed here the way that cancel claims it, and its timer is
// deliberately left running, so the window is reached without racing the
// runtime for it.
func TestAClaimedEntryDoesNotFireWhenItsTimerLandsAnyway(t *testing.T) {
	s := New()
	defer s.Stop()

	var n atomic.Int32
	s.At(20*time.Millisecond, func() { n.Add(1) })

	s.mu.Lock()
	var e *entry
	for _, v := range s.entries {
		e = v
	}
	s.mu.Unlock()
	if e == nil {
		t.Fatal("the scheduled entry is not in the registry")
	}
	if !e.claimed.CompareAndSwap(false, true) {
		t.Fatal("the entry was already claimed before the test claimed it")
	}

	time.Sleep(100 * time.Millisecond)
	if got := n.Load(); got != 0 {
		t.Fatalf("fn ran %d time(s) after its entry was claimed, want 0", got)
	}
}
