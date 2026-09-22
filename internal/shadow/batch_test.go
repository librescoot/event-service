package shadow

import (
	"strconv"
	"sync"
	"testing"
)

func TestObserveBatchChangesAndInvalidation(t *testing.T) {
	s := NewStore()
	s.Seed("h", map[string]string{"a": "old", "b": "old", "invalid": "stale"})
	changes := s.ObserveBatch("h", map[string]string{"b": "new", "a": "new"}, []string{"invalid"})
	if len(changes) != 2 || changes[0] != (Change{Field: "a", From: "old", To: "new"}) || changes[1].Field != "b" {
		t.Fatalf("changes = %#v", changes)
	}
	if s.Get("h", "invalid") != "" {
		t.Fatal("oversized value remained usable")
	}
	if got := s.ObserveBatch("h", map[string]string{"a": "new", "b": "new"}, nil); len(got) != 0 {
		t.Fatal("unchanged batch generated changes")
	}
}

func TestObserveBatchSnapshotIsAtomic(t *testing.T) {
	s := NewStore()
	s.Seed("h", map[string]string{"a": "0", "b": "0"})
	var wg sync.WaitGroup
	bad := make(chan bool, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			lookup := s.Snapshot()
			if lookup("h", "a") != lookup("h", "b") {
				bad <- true
				return
			}
		}
	}()
	for i := 1; i <= 1000; i++ {
		value := strconv.Itoa(i)
		s.ObserveBatch("h", map[string]string{"a": value, "b": value}, nil)
	}
	wg.Wait()
	if len(bad) != 0 {
		t.Fatal("snapshot observed a partially installed batch")
	}
}
