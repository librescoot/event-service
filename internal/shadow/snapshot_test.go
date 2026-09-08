package shadow

import "testing"

func TestSnapshotDoesNotFollowLiveUpdates(t *testing.T) {
	s := NewStore()
	s.Observe("vehicle", "state", "parked")
	lookup := s.Snapshot()
	s.Observe("vehicle", "state", "stand-by")
	s.Observe("new", "field", "value")
	if lookup("vehicle", "state") != "parked" || lookup("new", "field") != "" {
		t.Fatal("snapshot changed after a live update")
	}
	if s.Get("vehicle", "state") != "stand-by" {
		t.Fatal("snapshot affected the live store")
	}
}
