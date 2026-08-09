package action

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/librescoot/eventbus"
)

type nopLog struct{}

func (nopLog) Printf(string, ...any) {}

type fakeAction struct {
	mu       sync.Mutex
	calls    int
	block    chan struct{}
	failWith error
}

func (f *fakeAction) Kind() string { return "fake" }

func (f *fakeAction) Do(ctx context.Context, e eventbus.Event) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.block != nil {
		<-f.block
	}
	return f.failWith
}

func (f *fakeAction) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestPoolRunsSubmittedActions(t *testing.T) {
	p := NewPool(2, 8, nopLog{})
	p.Start()
	defer p.Stop()

	a := &fakeAction{}
	for i := 0; i < 5; i++ {
		if !p.Submit(a, eventbus.Event{Topic: "x.y"}, "r") {
			t.Fatalf("submit %d was rejected with a free queue", i)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for a.count() < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := a.count(); got != 5 {
		t.Errorf("ran %d actions, want 5", got)
	}
	if s := p.Stats(); s.Dispatched != 5 {
		t.Errorf("Dispatched = %d, want 5", s.Dispatched)
	}
}

// A hung action must not stall submission or grow memory without bound: it
// occupies a worker, the queue fills, and further submits are refused.
func TestPoolRefusesWhenQueueFullAndNeverBlocksTheCaller(t *testing.T) {
	block := make(chan struct{})
	a := &fakeAction{block: block}

	p := NewPool(1, 2, nopLog{})
	p.Start()
	defer func() { close(block); p.Stop() }()

	accepted := 0
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			if p.Submit(a, eventbus.Event{Topic: "x.y"}, "r") {
				accepted++
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Submit blocked; it must never block the caller")
	}

	if accepted >= 50 {
		t.Errorf("accepted %d of 50 with one hung worker; the queue must refuse", accepted)
	}
	if s := p.Stats(); s.Dropped == 0 {
		t.Error("Dropped must count refused submissions so they are visible")
	}
}

func TestPoolCountsFailures(t *testing.T) {
	p := NewPool(1, 4, nopLog{})
	p.Start()
	defer p.Stop()

	a := &fakeAction{failWith: context.DeadlineExceeded}
	p.Submit(a, eventbus.Event{Topic: "x.y"}, "r")

	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().Failed == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s := p.Stats(); s.Failed != 1 {
		t.Errorf("Failed = %d, want 1", s.Failed)
	}
}

func TestPoolStopWaitsForInFlightWork(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := NewPool(1, 4, nopLog{})
	p.Start()

	var finished bool
	var mu sync.Mutex
	p.Submit(actionFunc(func(ctx context.Context, e eventbus.Event) error {
		close(started)
		<-release
		mu.Lock()
		finished = true
		mu.Unlock()
		return nil
	}), eventbus.Event{Topic: "x.y"}, "r")

	<-started
	close(release)
	p.Stop()

	mu.Lock()
	defer mu.Unlock()
	if !finished {
		t.Error("Stop returned before in-flight work finished")
	}
}
