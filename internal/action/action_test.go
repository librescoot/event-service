package action

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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

// releaser returns the channel a pinned worker waits on and the function that
// lets it go. Releasing twice is safe, so a test can defer the release and
// still call it at the point it means to.
//
// A pinned worker must always wait on this channel together with the action's
// own context, never on this channel alone. t.Fatal runs runtime.Goexit, and
// whether the release still happens after that comes down to defer against
// cleanup ordering that a later edit can invert without noticing: a deferred
// p.Stop() registered after a deferred release runs first, and waits for the
// worker that the release it beat was going to free. Selecting on the context
// as well makes Pool.Stop itself the thing that frees the worker, and that
// holds however the two are ordered.
func releaser() (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, sync.OnceFunc(func() { close(ch) })
}

// awaitSignal waits for ch to be closed, and says which signal never arrived
// rather than letting the test hang until the package deadline.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestPoolRunsSubmittedActions(t *testing.T) {
	p := NewPool(2, 8, nopLog{})
	p.Start()
	defer p.Stop()

	a := &fakeAction{}
	for i := 0; i < 5; i++ {
		if !p.Submit(a, eventbus.Event{Topic: "x.y"}, "r", nil) {
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
			if p.Submit(a, eventbus.Event{Topic: "x.y"}, "r", nil) {
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
	p.Submit(a, eventbus.Event{Topic: "x.y"}, "r", nil)

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
	release, letGo := releaser()
	defer letGo()
	p := NewPool(1, 4, nopLog{})
	p.Start()

	var finished bool
	var mu sync.Mutex
	p.Submit(actionFunc(func(ctx context.Context, e eventbus.Event) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		mu.Lock()
		finished = true
		mu.Unlock()
		return nil
	}), eventbus.Event{Topic: "x.y"}, "r", nil)

	awaitSignal(t, started, "the job to reach a worker")
	letGo()
	p.Stop()

	mu.Lock()
	defer mu.Unlock()
	if !finished {
		t.Error("Stop returned before in-flight work finished")
	}
}

// A Submit that lands strictly after Stop has returned must be refused, not
// sent: jobs is never closed, so if this ever tried to send on it directly
// there would be nothing to panic on, but a naive implementation that closes
// the job queue in Stop would panic right here.
func TestSubmitAfterStopReturnsFalseWithoutPanic(t *testing.T) {
	p := NewPool(1, 4, nopLog{})
	p.Start()
	p.Stop()

	a := &fakeAction{}
	if p.Submit(a, eventbus.Event{Topic: "x.y"}, "r", nil) {
		t.Error("Submit after Stop returned true, want false")
	}
	if s := p.Stats(); s.Dropped == 0 {
		t.Error("Dropped must count a post-Stop submission")
	}
}

// TestStopCancelsRunningActionsContext proves Stop reaches a running action
// through its context rather than merely waiting it out: an action that
// watches ctx.Done() and returns must make Stop return quickly, even though
// the action would otherwise run forever. Without Pool wiring runCtx through
// to Do, this test hangs until the outer test timeout instead of passing.
func TestStopCancelsRunningActionsContext(t *testing.T) {
	started := make(chan struct{})
	var sawCancel atomic.Bool

	p := NewPool(1, 4, nopLog{})
	p.Start()

	p.Submit(actionFunc(func(ctx context.Context, e eventbus.Event) error {
		close(started)
		<-ctx.Done()
		sawCancel.Store(true)
		return ctx.Err()
	}), eventbus.Event{Topic: "x.y"}, "r", nil)

	awaitSignal(t, started, "the job to reach a worker")

	stopReturned := make(chan struct{})
	go func() {
		p.Stop()
		close(stopReturned)
	}()

	select {
	case <-stopReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return; a hung action must be canceled, not waited out")
	}

	if !sawCancel.Load() {
		t.Error("the running action's context was never canceled")
	}
}

// TestStopCountsAbandonedQueuedJobsAsDropped proves that jobs still sitting
// in the queue when every worker has exited are counted into Dropped, not
// silently discarded.
//
// The sole worker is pinned on the context Stop cancels, which puts the
// release exactly where it needs to be with nothing to time: the worker is
// held until Stop has signalled the workers to leave, and it is that same
// signal that frees it, so it goes out through its own done pre-check rather
// than draining the three jobs queued behind it. It also means every
// assertion here is free to fail without stranding a worker, since the
// deferred Stop is what unpins it.
func TestStopCountsAbandonedQueuedJobsAsDropped(t *testing.T) {
	started := make(chan struct{})

	p := NewPool(1, 4, nopLog{})
	p.Start()
	defer p.Stop()

	p.Submit(actionFunc(func(ctx context.Context, e eventbus.Event) error {
		close(started)
		<-ctx.Done()
		return nil
	}), eventbus.Event{Topic: "x.y"}, "r", nil)
	awaitSignal(t, started, "the pinning job to reach the worker")

	noop := actionFunc(func(ctx context.Context, e eventbus.Event) error { return nil })
	for i := 0; i < 3; i++ {
		if !p.Submit(noop, eventbus.Event{Topic: "x.y"}, "r", nil) {
			t.Fatalf("submit %d was refused with a free queue", i)
		}
	}

	stopReturned := make(chan struct{})
	go func() {
		p.Stop()
		close(stopReturned)
	}()
	awaitSignal(t, stopReturned, "Stop to return")

	if s := p.Stats(); s.Dropped != 3 {
		t.Errorf("Dropped = %d, want 3 abandoned queued jobs", s.Dropped)
	}
}

// Several goroutines hammer Submit while another goroutine calls Stop, all
// under -race and repeated across many pools so scheduling has room to hit
// the send-during-close window. Against the closed-channel design this
// panics with "send on closed channel" well before the loop count below is
// reached; against the done-channel design there is no send that can ever
// race a close, because jobs is never closed.
func TestConcurrentSubmitAndStopDoesNotPanic(t *testing.T) {
	const pools = 50
	const submitters = 4
	const submitsPerGoroutine = 200

	for i := 0; i < pools; i++ {
		p := NewPool(2, 8, nopLog{})
		p.Start()

		a := &fakeAction{}
		var submitWG sync.WaitGroup
		for g := 0; g < submitters; g++ {
			submitWG.Add(1)
			go func() {
				defer submitWG.Done()
				for j := 0; j < submitsPerGoroutine; j++ {
					p.Submit(a, eventbus.Event{Topic: "x.y"}, "r", nil)
				}
			}()
		}

		// Stop races the submitters directly: it is not gated on their
		// completion, so some Submit calls land before, during, and after
		// close(p.done).
		p.Stop()
		submitWG.Wait()

		if p.Submit(a, eventbus.Event{Topic: "x.y"}, "r", nil) {
			t.Fatalf("pool %d: Submit after Stop returned true, want false", i)
		}
	}
}

func TestSubmitCallsDoneWithNilOnSuccess(t *testing.T) {
	p := NewPool(1, 4, nopLog{})
	p.Start()
	defer p.Stop()

	a := &fakeAction{}
	doneCh := make(chan error, 1)
	if !p.Submit(a, eventbus.Event{Topic: "x.y"}, "r", func(err error) {
		doneCh <- err
	}) {
		t.Fatal("submit was rejected with a free queue")
	}

	select {
	case err := <-doneCh:
		if err != nil {
			t.Errorf("done called with %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("done was never called")
	}
}

func TestSubmitCallsDoneWithTheActionError(t *testing.T) {
	p := NewPool(1, 4, nopLog{})
	p.Start()
	defer p.Stop()

	wantErr := context.DeadlineExceeded
	a := &fakeAction{failWith: wantErr}
	doneCh := make(chan error, 1)
	if !p.Submit(a, eventbus.Event{Topic: "x.y"}, "r", func(err error) {
		doneCh <- err
	}) {
		t.Fatal("submit was rejected with a free queue")
	}

	select {
	case err := <-doneCh:
		if err != wantErr {
			t.Errorf("done called with %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("done was never called")
	}
}

// Refused jobs must not look like completed jobs: a caller that treats
// refusal as completion would advance a sequence past a step that never ran.
func TestDoneIsNotCalledWhenQueueIsFull(t *testing.T) {
	block := make(chan struct{})
	blocker := &fakeAction{block: block}

	p := NewPool(1, 1, nopLog{})
	p.Start()
	defer func() { close(block); p.Stop() }()

	if !p.Submit(blocker, eventbus.Event{Topic: "x.y"}, "r", nil) {
		t.Fatal("submit was rejected with a free queue")
	}

	deadline := time.Now().Add(2 * time.Second)
	for blocker.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if blocker.count() == 0 {
		t.Fatal("blocking job was never picked up by the worker")
	}

	filler := &fakeAction{}
	if !p.Submit(filler, eventbus.Event{Topic: "x.y"}, "r", nil) {
		t.Fatal("submit was rejected while the queue had room")
	}

	called := false
	if p.Submit(&fakeAction{}, eventbus.Event{Topic: "x.y"}, "r", func(err error) {
		called = true
	}) {
		t.Fatal("Submit returned true with a full queue and a busy worker")
	}
	if called {
		t.Error("done was called for a submission refused because the queue was full")
	}
}

// Refused jobs must not look like completed jobs, mirroring
// TestDoneIsNotCalledWhenQueueIsFull for the other refusal path.
func TestDoneIsNotCalledWhenPoolIsStopped(t *testing.T) {
	p := NewPool(1, 4, nopLog{})
	p.Start()
	p.Stop()

	called := false
	if p.Submit(&fakeAction{}, eventbus.Event{Topic: "x.y"}, "r", func(err error) {
		called = true
	}) {
		t.Fatal("Submit after Stop returned true, want false")
	}
	if called {
		t.Error("done was called for a submission refused because the pool was stopped")
	}
}

// TestDoneMaySubmitAnotherJob proves the sequence-advance path: a done
// callback running on the worker goroutine can call Submit again to queue
// the next step, and that submission is accepted and runs.
func TestDoneMaySubmitAnotherJob(t *testing.T) {
	p := NewPool(1, 4, nopLog{})
	p.Start()
	defer p.Stop()

	second := &fakeAction{}
	secondDone := make(chan error, 1)

	first := &fakeAction{}
	if !p.Submit(first, eventbus.Event{Topic: "x.y"}, "r", func(err error) {
		if !p.Submit(second, eventbus.Event{Topic: "x.y"}, "r", func(err error) {
			secondDone <- err
		}) {
			secondDone <- fmt.Errorf("submit from within done was refused")
		}
	}) {
		t.Fatal("submit was rejected with a free queue")
	}

	select {
	case err := <-secondDone:
		if err != nil {
			t.Errorf("second step: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second step never ran; done-triggered Submit did not advance the sequence")
	}
}
