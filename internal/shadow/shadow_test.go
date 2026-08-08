package shadow

import (
	"sync"
	"testing"
)

func TestObserveFirstValueIsAChangeWithEmptyPrev(t *testing.T) {
	s := NewStore()
	prev, changed := s.Observe("vehicle", "state", "parked")
	if !changed {
		t.Error("first observation should count as a change")
	}
	if prev != "" {
		t.Errorf("prev = %q, want empty", prev)
	}
}

func TestObserveSameValueIsNotAChange(t *testing.T) {
	s := NewStore()
	s.Observe("vehicle", "state", "parked")
	prev, changed := s.Observe("vehicle", "state", "parked")
	if changed {
		t.Error("repeat of the same value should not count as a change")
	}
	if prev != "parked" {
		t.Errorf("prev = %q, want parked", prev)
	}
}

func TestObserveReturnsPreviousValue(t *testing.T) {
	s := NewStore()
	s.Observe("vehicle", "state", "stand-by")
	prev, changed := s.Observe("vehicle", "state", "parked")
	if !changed {
		t.Error("changed = false, want true")
	}
	if prev != "stand-by" {
		t.Errorf("prev = %q, want stand-by", prev)
	}
}

func TestFieldsAreIndependentAcrossHashes(t *testing.T) {
	s := NewStore()
	s.Observe("battery:0", "charge", "80")
	prev, changed := s.Observe("battery:1", "charge", "80")
	if !changed || prev != "" {
		t.Errorf("battery:1 should be independent of battery:0, got prev=%q changed=%v", prev, changed)
	}
}

func TestSeedDoesNotReportChanges(t *testing.T) {
	s := NewStore()
	s.Seed("vehicle", map[string]string{"state": "parked", "kickstand": "down"})
	if got := s.Get("vehicle", "state"); got != "parked" {
		t.Errorf("Get after Seed = %q, want parked", got)
	}
	_, changed := s.Observe("vehicle", "state", "parked")
	if changed {
		t.Error("value seeded then observed unchanged should not report a change")
	}
}

func TestGetUnknownReturnsEmpty(t *testing.T) {
	s := NewStore()
	if got := s.Get("nope", "nope"); got != "" {
		t.Errorf("Get(unknown) = %q, want empty", got)
	}
}

func TestObserveEmptyStringAsFirstValue(t *testing.T) {
	s := NewStore()
	prev, changed := s.Observe("config", "fallback", "")
	if !changed {
		t.Error("first observation of empty string should count as a change")
	}
	if prev != "" {
		t.Errorf("prev = %q, want empty", prev)
	}
	prev, changed = s.Observe("config", "fallback", "")
	if changed {
		t.Error("repeat of empty string should not count as a change")
	}
	if prev != "" {
		t.Errorf("prev = %q, want empty", prev)
	}
}

func TestConcurrentObserveAndGet(t *testing.T) {
	s := NewStore()
	numGoroutines := 8
	iterationsPerGoroutine := 500
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterationsPerGoroutine; j++ {
				hash := "hash-0"
				field := "field-0"
				value := "value-" + string(rune(id)) + "-" + string(rune(j%256))
				_, _ = s.Observe(hash, field, value)
				_ = s.Get(hash, field)
			}
		}(i)
	}

	wg.Wait()

	got := s.Get("hash-0", "field-0")
	if got == "" {
		t.Error("after concurrent writes, field should have a value")
	}
}
