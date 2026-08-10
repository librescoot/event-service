package stats

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ipc "github.com/librescoot/redis-ipc"
)

type nopLog struct{}

func (nopLog) Printf(string, ...any) {}

// capturingLog keeps every line, so a test can insist a failed write said so
// rather than vanishing quietly.
type capturingLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *capturingLog) Printf(format string, v ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
	l.mu.Unlock()
}

func (l *capturingLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

// call is one write the fake Hasher recorded, in order. Reading the sequence
// of calls, not just the hash's final contents, is what proves "wrote
// nothing" as distinct from "wrote and then wrote the same thing again."
type call struct {
	field, value string
}

// fakeHasher is an in-memory stand-in for the datastore hash. err, when set,
// makes every write fail without recording it, so a test can drive the
// retry path.
type fakeHasher struct {
	mu    sync.Mutex
	calls []call
	err   error
}

func (h *fakeHasher) HSet(key, field string, value any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return h.err
	}
	h.calls = append(h.calls, call{field: field, value: fmt.Sprint(value)})
	return nil
}

func (h *fakeHasher) list() []call {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]call(nil), h.calls...)
}

func (h *fakeHasher) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

func (h *fakeHasher) setErr(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.err = err
}

// snapper is a snapshot a test can change mid-run, standing in for the
// counters the real service would read from the pool, the runner and the
// scheduler.
type snapper struct {
	mu   sync.Mutex
	data map[string]string
}

func newSnapper(initial map[string]string) *snapper {
	m := make(map[string]string, len(initial))
	for k, v := range initial {
		m[k] = v
	}
	return &snapper{data: m}
}

func (s *snapper) snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

func (s *snapper) set(field, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[field] = value
}

// newGate returns a gate and the function that opens it. Opening twice is
// safe, so a test can defer the open and still call it where it means to: a
// test that fails an assertion before its own open would otherwise leave the
// publisher's goroutine parked forever, and Stop, which the deferred cleanup
// calls, waits for it.
func newGate() (chan struct{}, func()) {
	gate := make(chan struct{})
	var once sync.Once
	return gate, func() { once.Do(func() { close(gate) }) }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestFirstPublishWritesEverything(t *testing.T) {
	h := &fakeHasher{}
	initial := map[string]string{
		"rules": "2", "dispatched": "0", "dropped": "0", "failed": "0",
		"pending": "0", "runs-active": "0", "version": "v0.9.0",
	}
	s := newSnapper(initial)
	p := newPublisherIn(h, time.Hour, nopLog{}, "test:stats")
	p.Start(s.snapshot)
	defer p.Stop()

	waitFor(t, "the first publish", func() bool { return h.count() == len(initial) })

	got := map[string]string{}
	for _, c := range h.list() {
		got[c.field] = c.value
	}
	for field, want := range initial {
		if got[field] != want {
			t.Errorf("field %s = %q, want %q", field, got[field], want)
		}
	}
}

func TestPublishesChangedFieldsOnly(t *testing.T) {
	h := &fakeHasher{}
	s := newSnapper(map[string]string{"dispatched": "1", "dropped": "0", "version": "v1"})
	p := newPublisherIn(h, 15*time.Millisecond, nopLog{}, "test:stats")
	p.Start(s.snapshot)
	defer p.Stop()

	waitFor(t, "the first publish", func() bool { return h.count() == 3 })

	s.set("dispatched", "2")
	waitFor(t, "the changed field to be written", func() bool { return h.count() == 4 })

	last := h.list()[len(h.list())-1]
	if last.field != "dispatched" || last.value != "2" {
		t.Fatalf("the write after the change was %+v, want dispatched=2", last)
	}

	// A couple more ticks with nothing changed, to prove the two fields
	// that did not change do not get rewritten just because a third did.
	time.Sleep(60 * time.Millisecond)
	if got := h.count(); got != 4 {
		t.Errorf("%d write(s) after one field changed once, want 4 (3 first-publish + 1 change); dropped and version must not be rewritten", got)
	}
}

// TestIdleSnapshotWritesNothing is the load-bearing test. It has to prove
// two things at once: that the publisher's ticker is actually running, not
// sitting inert, and that a run of ticks over an unchanged snapshot writes
// nothing at all. Proving only the second half would also be true of a
// publisher that never started: the fix is to change the snapshot afterwards
// and require a write to appear, inside the same test, using the same
// publisher instance.
func TestIdleSnapshotWritesNothing(t *testing.T) {
	h := &fakeHasher{}
	s := newSnapper(map[string]string{"rules": "1", "dispatched": "0", "version": "v1"})
	p := newPublisherIn(h, 10*time.Millisecond, nopLog{}, "test:stats")
	p.Start(s.snapshot)
	defer p.Stop()

	waitFor(t, "the first publish", func() bool { return h.count() == 3 })
	idle := h.count()

	// About a dozen ticks over an unchanged snapshot: an idle scooter.
	time.Sleep(120 * time.Millisecond)
	if got := h.count(); got != idle {
		t.Fatalf("%d write(s) over ~12 idle ticks, want %d: an unchanged snapshot must not be rewritten", got, idle)
	}

	// Proof the ticker was awake and comparing rather than dead the whole
	// time: change one field and require a write to land within a couple
	// of ticks, well inside the time the idle check above just spent
	// producing nothing.
	s.set("dispatched", "5")
	waitFor(t, "the changed field to be written once the scooter stops being idle", func() bool {
		return h.count() == idle+1
	})
	last := h.list()[len(h.list())-1]
	if last.field != "dispatched" || last.value != "5" {
		t.Fatalf("the write after the idle window was %+v, want dispatched=5", last)
	}
}

func TestStopIsIdempotentAndStopsWriting(t *testing.T) {
	h := &fakeHasher{}
	s := newSnapper(map[string]string{"dispatched": "0"})
	p := newPublisherIn(h, 10*time.Millisecond, nopLog{}, "test:stats")
	p.Start(s.snapshot)

	waitFor(t, "the first publish", func() bool { return h.count() == 1 })

	done := make(chan struct{})
	go func() {
		p.Stop()
		p.Stop() // idempotent: a second call must not panic or block
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return; a second call must not block")
	}

	after := h.count()
	s.set("dispatched", "99")
	time.Sleep(80 * time.Millisecond)
	if got := h.count(); got != after {
		t.Errorf("%d write(s) after Stop, want %d: a stopped publisher must not write", got, after)
	}
}

// TestStopReturnsPromptlyWithoutWaitingOutTheInterval guards the shutdown
// invariant directly: Stop must not wait for the next tick, or a shutdown
// against the service's real interval would take as long as that interval.
func TestStopReturnsPromptlyWithoutWaitingOutTheInterval(t *testing.T) {
	h := &fakeHasher{}
	s := newSnapper(map[string]string{"dispatched": "0"})
	p := newPublisherIn(h, time.Hour, nopLog{}, "test:stats")
	p.Start(s.snapshot)
	waitFor(t, "the first publish", func() bool { return h.count() == 1 })

	start := time.Now()
	p.Stop()
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("Stop took %v against an hour-long interval, want well under a second", elapsed)
	}
}

// TestStopWaitsForAnInFlightPublishToFinish is what tells "Stop signals the
// goroutine" apart from "Stop waits for the goroutine to actually exit."
// Closing the stop channel and returning immediately would pass every other
// test here, since the goroutine happens to notice the close and exit within
// a tick or two of its own accord regardless of whether Stop waited for it.
// Parking the second snapshot call on a gate forces Stop to either block
// until it is released, which is the contract, or return early, which is the
// bug.
func TestStopWaitsForAnInFlightPublishToFinish(t *testing.T) {
	h := &fakeHasher{}
	gate, open := newGate()
	defer open()
	parked := make(chan struct{}, 1)

	s := newSnapper(map[string]string{"dispatched": "0"})
	var calls int32
	snapshot := func() map[string]string {
		if atomic.AddInt32(&calls, 1) == 2 {
			parked <- struct{}{}
			<-gate
		}
		return s.snapshot()
	}

	p := newPublisherIn(h, 10*time.Millisecond, nopLog{}, "test:stats")
	p.Start(snapshot)

	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		t.Fatal("the second snapshot call never parked; the test cannot drive the window Stop is supposed to wait through")
	}

	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Stop returned while the goroutine was still inside a snapshot call")
	case <-time.After(150 * time.Millisecond):
	}

	open()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the blocked snapshot call was released")
	}
}

// TestAFailedWriteIsRetriedOnTheNextTick pins the retry behaviour: a write
// that fails must not be treated as sent, or a datastore blip drops a
// counter change on the floor for good.
func TestAFailedWriteIsRetriedOnTheNextTick(t *testing.T) {
	h := &fakeHasher{}
	log := &capturingLog{}
	s := newSnapper(map[string]string{"dispatched": "0"})
	p := newPublisherIn(h, 15*time.Millisecond, log, "test:stats")
	p.Start(s.snapshot)
	defer p.Stop()
	waitFor(t, "the first publish", func() bool { return h.count() == 1 })

	h.setErr(fmt.Errorf("datastore unreachable"))
	s.set("dispatched", "1")
	waitFor(t, "the failed write to be logged", func() bool { return log.count() > 0 })

	if got := h.count(); got != 1 {
		t.Fatalf("%d write(s) recorded while HSet was failing, want 1: a failed write must not count as sent", got)
	}

	h.setErr(nil)
	waitFor(t, "the retried write to land once the datastore is back", func() bool { return h.count() == 2 })
	last := h.list()[len(h.list())-1]
	if last.field != "dispatched" || last.value != "1" {
		t.Fatalf("the retried write was %+v, want dispatched=1", last)
	}
}

// liveHasher returns the real datastore client at localhost:6379, over a
// hash unique to this test run, cleaned up when the test ends.
func liveHasher(t *testing.T) (*ipc.Client, string) {
	t.Helper()
	client, err := ipc.New(
		ipc.WithURL("localhost:6379"),
		ipc.WithDialTimeout(2*time.Second),
		ipc.WithCodec(ipc.StringCodec{}),
	)
	if err != nil {
		t.Skipf("datastore not reachable at localhost:6379: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	hash := fmt.Sprintf("test:event-service:stats:%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = client.Do("DEL", hash) })
	return client, hash
}

// TestLivePublishWritesToTheDatastore is the one test that goes through the
// real client rather than the fake, so a mismatch between *ipc.Client's HSet
// and what this package expects of Hasher shows up here rather than only in
// production.
func TestLivePublishWritesToTheDatastore(t *testing.T) {
	client, hash := liveHasher(t)

	initial := map[string]string{"rules": "1", "dispatched": "0", "version": "v1"}
	s := newSnapper(initial)
	p := newPublisherIn(client, 20*time.Millisecond, nopLog{}, hash)
	p.Start(s.snapshot)
	defer p.Stop()

	waitFor(t, "the first publish to land in the datastore", func() bool {
		got, err := client.HGetAll(hash)
		if err != nil {
			t.Fatalf("HGetAll: %v", err)
		}
		return len(got) == len(initial)
	})

	got, err := client.HGetAll(hash)
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	for field, want := range initial {
		if got[field] != want {
			t.Errorf("field %s = %q, want %q", field, got[field], want)
		}
	}

	s.set("dispatched", "7")
	waitFor(t, "the changed field to land", func() bool {
		got, err := client.HGetAll(hash)
		if err != nil {
			t.Fatalf("HGetAll: %v", err)
		}
		return got["dispatched"] == "7"
	})
}
