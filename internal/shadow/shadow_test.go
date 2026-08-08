package shadow

import "testing"

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
