package seq

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/eventbus"
	ipc "github.com/librescoot/redis-ipc"
)

// memHash is an in-memory stand-in for the datastore hash. The runner and
// replay tests use it rather than a live client so that "the record is gone"
// is a fact the test reads directly, with no round trip to time out on.
//
// entered and release, when set, hold HSet open after it has written: the
// value is in the hash and the caller has not been told so yet, which is the
// window between a record being written and the run being marked as holding
// it.
type memHash struct {
	mu sync.Mutex
	m  map[string]map[string]string

	entered chan string
	release chan struct{}
}

func newMemHash() *memHash { return &memHash{m: make(map[string]map[string]string)} }

func (h *memHash) HSet(key, field string, value any) error {
	h.mu.Lock()
	if h.m[key] == nil {
		h.m[key] = make(map[string]string)
	}
	h.m[key][field] = fmt.Sprint(value)
	entered, release := h.entered, h.release
	h.mu.Unlock()

	// Outside the lock on purpose: a test holding this open still has to be
	// able to read the hash it is holding open.
	//
	// The handshake is selected against release rather than sent
	// unconditionally, so a write arriving after the hold has been opened
	// passes straight through. An unconditional send would block forever on
	// an unbuffered channel nobody is receiving from any more, and a worker
	// stuck there takes Pool.Stop, and the whole package, down with it.
	if entered != nil {
		select {
		case entered <- field:
		case <-release:
		}
		<-release
	}
	return nil
}

// hold makes every later HSet write its value, hand the field name to the
// returned channel and wait there until open is called. That parks a caller
// inside putRecord with the record already in the hash and the run not yet
// marked as holding it, which is the window a cancel or a shutdown has to be
// driven into.
func (h *memHash) hold() (entered <-chan string, open func()) {
	release, open := newGate()
	e := make(chan string)
	h.mu.Lock()
	h.entered, h.release = e, release
	h.mu.Unlock()
	return e, open
}

func (h *memHash) HGetAll(key string) (map[string]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]string, len(h.m[key]))
	for k, v := range h.m[key] {
		out[k] = v
	}
	return out, nil
}

func (h *memHash) HDel(key string, fields ...string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range fields {
		delete(h.m[key], f)
	}
	if len(h.m[key]) == 0 {
		delete(h.m, key)
	}
	return nil
}

// testHashName is unique per store, so two tests running at once, in the same
// package or in two copies of it, cannot see each other's records.
var hashSeq struct {
	mu sync.Mutex
	n  int
}

func testHashName(t *testing.T) string {
	t.Helper()
	hashSeq.mu.Lock()
	hashSeq.n++
	n := hashSeq.n
	hashSeq.mu.Unlock()
	return fmt.Sprintf("test:event-service:pending:%d:%d", time.Now().UnixNano(), n)
}

// liveStore returns a store over the datastore at localhost:6379, with the
// hash removed again when the test ends.
func liveStore(t *testing.T, log Logger) *PendingStore {
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

	hash := testHashName(t)
	t.Cleanup(func() { _, _ = client.Do("DEL", hash) })
	// The same adapter main uses, so a fix to one is a fix to both.
	return newPendingStoreIn(NewClientHasher(client), log, hash)
}

func memStore(t *testing.T, log Logger) (*PendingStore, *memHash) {
	t.Helper()
	h := newMemHash()
	return newPendingStoreIn(h, log, testHashName(t)), h
}

// fingerprintOf is what a record written for step idx of s carries. Seeding a
// record by hand means seeding this too, or replay drops it as a record for a
// step that has since been edited.
func fingerprintOf(s *Sequence, idx int) string { return s.Steps[idx].Fingerprint }

// capturingLog keeps every line, so a test can insist a dropped record said
// so rather than vanishing quietly.
type capturingLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *capturingLog) Printf(format string, v ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
	l.mu.Unlock()
}

func (l *capturingLog) contains(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, sub) {
			return true
		}
	}
	return false
}

func (l *capturingLog) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func loadAll(t *testing.T, st *PendingStore) []Pending {
	t.Helper()
	got, err := st.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return got
}

func countPending(t *testing.T, st *PendingStore) int {
	t.Helper()
	return len(loadAll(t, st))
}

func boolPtr(b bool) *bool { return &b }

// eventually reports whether cond became true inside d. Unlike waitFor it
// does not fail the test, so a caller can say what a true and a false mean in
// its own words, which matters where the interesting outcome is the negative
// one.
func eventually(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// waitEntered takes one field name from a held HSet, or fails the test rather
// than blocking to the package timeout.
func waitEntered(t *testing.T, entered <-chan string) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no record was written; the step under test is not durable")
	}
}

// TestAHeldWriteStopsHoldingOnceItIsOpened guards the helper rather than the
// code: a second write arriving after the hold is open must not park on the
// handshake. A worker stuck there is waited on by Pool.Stop, which turns any
// later failure in this package into a hang instead of a red line.
func TestAHeldWriteStopsHoldingOnceItIsOpened(t *testing.T) {
	h := newMemHash()
	entered, open := h.hold()
	defer open()

	go func() { _ = h.HSet("k", "first", "1") }()
	waitEntered(t, entered)
	open()

	done := make(chan struct{})
	go func() {
		_ = h.HSet("k", "second", "2")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a write after the hold was opened is still blocked on the handshake")
	}
}

func TestPutThenLoadRoundTripsAPending(t *testing.T) {
	st := liveStore(t, nopLog{})

	want := Pending{
		ID:     "hazards#1",
		Rule:   "hazards",
		Source: "hazards.toml",
		Step:   2,
		Iter:   1,
		FireAt: time.Now().Add(30 * time.Second).UnixMilli(),
		Event: eventbus.Event{
			Topic: "alarm.triggered", Src: "alarm",
			From: "armed", To: "level-2-triggered",
			Data: map[string]any{"slot": "1"},
		},
	}
	if err := st.Put(want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got := loadAll(t, st)
	if len(got) != 1 {
		t.Fatalf("Load returned %d record(s), want 1", len(got))
	}
	p := got[0]
	if p.ID != want.ID || p.Rule != want.Rule || p.Source != want.Source {
		t.Errorf("Load returned %+v, want id/rule/source of %+v", p, want)
	}
	if p.Step != want.Step || p.Iter != want.Iter || p.FireAt != want.FireAt {
		t.Errorf("Load returned step %d iter %d fire-at %d, want %d/%d/%d",
			p.Step, p.Iter, p.FireAt, want.Step, want.Iter, want.FireAt)
	}
	if p.Event.Topic != want.Event.Topic || p.Event.To != want.Event.To {
		t.Errorf("Load returned event %+v, want %+v", p.Event, want.Event)
	}
	if p.Event.Data["slot"] != "1" {
		t.Errorf("event data came back as %v, want slot 1; a step when reads the triggering event", p.Event.Data)
	}
}

func TestDropRemovesAPending(t *testing.T) {
	st := liveStore(t, nopLog{})

	for _, id := range []string{"a", "b"} {
		if err := st.Put(Pending{ID: id, Rule: "r", FireAt: time.Now().UnixMilli()}); err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
	}
	if n := countPending(t, st); n != 2 {
		t.Fatalf("%d record(s) before the drop, want 2", n)
	}

	if err := st.Drop("a"); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	got := loadAll(t, st)
	if len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("records after dropping a are %+v, want only b", got)
	}
}

func TestAfterStepDefaultsToDurable(t *testing.T) {
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", After: "30s"},
		},
	}, nil)

	if r.Steps[0].Durable {
		t.Error("a step with no after must not be durable: it never waits, so there is nothing to replay")
	}
	if !r.Steps[1].Durable {
		t.Error("a step with after and no durable key must default to durable")
	}

	s, err := Build(r, nopPusher{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if s.Steps[0].Durable || !s.Steps[1].Durable {
		t.Errorf("compiled steps are durable=%v,%v, want false,true", s.Steps[0].Durable, s.Steps[1].Durable)
	}
}

func TestDurableFalseOptsOut(t *testing.T) {
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{
			{Do: "redis", List: "a", Push: "1", After: "30s", Durable: boolPtr(false)},
			{Do: "redis", List: "b", Push: "2", After: "30s", Durable: boolPtr(true)},
		},
	}, nil)

	if r.Steps[0].Durable {
		t.Error("durable = false on a step with after must opt out")
	}
	if !r.Steps[1].Durable {
		t.Error("durable = true on a step with after must stay durable")
	}
}

func TestDurableOnAStepWithoutAfterIsAConfigError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value *bool
	}{
		{"true", boolPtr(true)},
		{"false", boolPtr(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, errs := rules.Compile([]rules.RuleConfig{{
				Name: "r", On: []string{"x.y"},
				Steps: []rules.StepConfig{
					push("a", "1"),
					{Do: "redis", List: "b", Push: "2", Durable: tc.value},
				},
			}}, func(string, string) string { return "" })

			if len(errs) != 1 {
				t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
			}
			msg := errs[0].Error()
			for _, want := range []string{`"r"`, "step 1", "durable", "after"} {
				if !strings.Contains(msg, want) {
					t.Errorf("error should mention %s, got %q", want, msg)
				}
			}
		})
	}
}

// TestSchedulingADurableStepWritesItsRecord is the write half of the pair:
// nothing can be replayed that was never recorded. The hour-long after keeps
// the run parked while the record is read back.
func TestSchedulingADurableStepWritesItsRecord(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "hazards", On: []string{"alarm.triggered"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("on"), rec.step("off"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, nopLog{})
	before := time.Now()
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered", To: "level-2-triggered"})

	waitFor(t, "the run to park on its durable tail", func() bool { return sch.Pending() == 1 })
	waitFor(t, "the record to be written", func() bool { return countPending(t, st) == 1 })

	p := loadAll(t, st)[0]
	if p.Rule != "hazards" {
		t.Errorf("record names rule %q, want hazards", p.Rule)
	}
	if p.Step != 1 {
		t.Errorf("record names step %d, want 1", p.Step)
	}
	if p.Iter != 0 {
		t.Errorf("record names iter %d, want 0", p.Iter)
	}
	if p.Event.Topic != "alarm.triggered" || p.Event.To != "level-2-triggered" {
		t.Errorf("record carries event %+v, want the triggering one", p.Event)
	}
	wantAt := before.Add(time.Hour).UnixMilli()
	if p.FireAt < wantAt-5000 || p.FireAt > wantAt+5000 {
		t.Errorf("record fires at %d, want about %d", p.FireAt, wantAt)
	}
}

func TestNonDurableStepWritesNoRecord(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", After: "1h", Durable: boolPtr(false)},
		},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the run to park on its tail", func() bool { return sch.Pending() == 1 })
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) written for a step that opted out of durability, want 0", n)
	}
}

// TestFiringADurableStepRemovesItsRecord holds the run inside the step after
// the durable one, so the record has to be gone while the run is still live.
// Checking after the run has ended would prove nothing: the end of a run
// removes whatever record it still holds, so the fire path could do nothing
// at all and the hash would still come out empty.
func TestFiringADurableStepRemovesItsRecord(t *testing.T) {
	gate, open := newGate()
	defer open()
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{
			{Do: "redis", List: "a", Push: "1", After: "20ms"},
			push("b", "2"),
		},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.gated("two", gate))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the record to be written", func() bool { return countPending(t, st) == 1 })
	waitFor(t, "the durable step to fire", func() bool { return len(rec.list()) == 2 })

	if got := rn.Active(); got != 1 {
		t.Fatalf("Active() = %d while the run is held in its last step, want 1", got)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left once the step fired, want 0; a record that outlives its step replays on the next boot", n)
	}

	open()
	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one", "two"}) {
		t.Fatalf("steps ran as %v, want one, two", got)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left once the run ended, want 0", n)
	}
}

// TestCancellingADurableStepRemovesItsRecord is the worst failure this whole
// feature can have: a record left behind by a cancel is replayed on the next
// boot and re-fires hardware the rider explicitly stopped.
func TestCancellingADurableStepRemovesItsRecord(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.disarmed"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})

	waitFor(t, "the record to be written", func() bool { return countPending(t, st) == 1 })

	if got := rn.CancelMatching("alarm.disarmed"); got != 1 {
		t.Fatalf("CancelMatching returned %d, want 1", got)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s) after the cancel, want 0", got)
	}
	if !eventually(2*time.Second, func() bool { return countPending(t, st) == 0 }) {
		t.Error("the record survived the cancel; it replays at the next start and re-fires a step the rider stopped on purpose")
	}
}

// TestCancelWhileTheRecordIsBeingWrittenRemovesIt drives the window between
// the record landing in the datastore and the run being marked as holding it.
// A cancel arriving in there ends the run without finding a record to claim,
// so whatever comes back to park the step is the last thing that can remove
// it. Miss that and the rider's disarm leaves a record which replays the
// deferred action at the next start.
func TestCancelWhileTheRecordIsBeingWrittenRemovesIt(t *testing.T) {
	st, h := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "hazards", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.disarmed"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("on"), rec.step("off"))

	entered, open := h.hold()
	defer open()

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})

	waitEntered(t, entered)
	if n := countPending(t, st); n != 1 {
		t.Fatalf("%d record(s) while the write is held open, want 1", n)
	}
	if got := rn.CancelMatching("alarm.disarmed"); got != 1 {
		t.Fatalf("CancelMatching returned %d, want 1", got)
	}
	open()

	if !eventually(2*time.Second, func() bool { return countPending(t, st) == 0 }) {
		t.Error("the record survived a cancel that landed while it was being written; nothing else will remove it, so it replays and re-fires the step at the next start")
	}
}

// TestShutdownWhileTheRecordIsBeingWrittenKeepsIt is the same window from the
// other side. A run ended by Stop has not run its step, so the record is the
// only thing that will ever get it run.
func TestShutdownWhileTheRecordIsBeingWrittenKeepsIt(t *testing.T) {
	st, h := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "hazards", On: []string{"alarm.triggered"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "30s"}},
	}, nil)
	s := seqWith(t, r, rec.step("on"), rec.step("off"))

	entered, open := h.hold()
	defer open()

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})

	waitEntered(t, entered)
	rn.Stop()
	open()

	if eventually(300*time.Millisecond, func() bool { return countPending(t, st) == 0 }) {
		t.Error("a shutdown landing while the record was being written removed it; the step never ran, so this is the hazards left on with nothing able to turn them off")
	}
}

// TestARepeatPassRecordsTheIterationItIsOn: the pass number in the record is
// what a replayed run resumes on. Record the wrong one and a rule that was
// three chirps into five comes back up and does five more.
func TestARepeatPassRecordsTheIterationItIsOn(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "chirp", On: []string{"x.y"},
		Repeat: &rules.RepeatConfig{Count: 3, Every: "1ms"},
		Steps:  []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "60ms"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	// one, two of the first pass, then one of the second, which leaves the
	// second pass parked on the durable step with its record written.
	waitFor(t, "the second pass to park on its durable step", func() bool {
		return len(rec.list()) == 3 && countPending(t, st) == 1
	})

	if got := loadAll(t, st)[0].Iter; got != 1 {
		t.Errorf("the record written on the second pass says iter %d, want 1; a replay would restart the repeat and run the whole count over", got)
	}
}

// TestReplayFinishesTheRepeatPassesTheRunHadLeft is the reading half. The
// record was written on pass 1 of 3, so what is left is the rest of that pass
// and one more, not three passes from the top.
func TestReplayFinishesTheRepeatPassesTheRunHadLeft(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "chirp", On: []string{"x.y"},
		Repeat: &rules.RepeatConfig{Count: 3, Every: "5ms"},
		Steps:  []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "20ms"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	if err := st.Put(Pending{
		ID: "mid", Rule: "chirp", Step: 1, Iter: 1,
		FireAt:      time.Now().Add(-time.Second).UnixMilli(),
		Fingerprint: fingerprintOf(s, 1),
		Event:       eventbus.Event{Topic: "x.y"},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
		t.Fatalf("Replay resumed %d step(s), want 1", n)
	}

	waitFor(t, "the replayed run to end", func() bool { return rn.Active() == 0 })
	// Long enough for a third and fourth pass to show up if the run came back
	// believing it was on its first.
	time.Sleep(60 * time.Millisecond)
	want := []string{"two", "one", "two"}
	if got := rec.list(); !equal(got, want) {
		t.Errorf("steps ran as %v, want %v; a replayed run finishes the passes it had left rather than the whole count again", got, want)
	}
}

// TestRestartDropsTheOldRunsRecord covers the fourth sink: a restart abandons
// the live run's tail, so its record must go with it rather than being
// replayed alongside the replacement's.
func TestRestartDropsTheOldRunsRecord(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "restart",
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the first run's record", func() bool { return countPending(t, st) == 1 })
	first := loadAll(t, st)[0].ID

	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the restarted run to park on its tail", func() bool { return sch.Pending() == 1 })
	if !eventually(2*time.Second, func() bool {
		got := loadAll(t, st)
		return len(got) == 1 && got[0].ID != first
	}) {
		t.Errorf("records after a restart are %+v, want exactly one and not the abandoned run's %s", loadAll(t, st), first)
	}
}

// TestStopLeavesTheRecordForReplay is the hazard this task exists for. A
// service going down between "hazards on" and "hazards off in 30 s" must
// leave the off step recorded, or nothing in the system will ever run it.
func TestStopLeavesTheRecordForReplay(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "hazards", On: []string{"alarm.triggered"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "30s"}},
	}, nil)
	s := seqWith(t, r, rec.step("on"), rec.step("off"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, "the record to be written", func() bool { return countPending(t, st) == 1 })

	rn.Stop()

	if n := countPending(t, st); n != 1 {
		t.Fatalf("%d record(s) after shutdown, want 1; a shutdown must leave the pending step to be replayed", n)
	}
}

// TestARecordWrittenByARunReplaysIntoANewRunner is the whole feature end to
// end, and the only test where the record is written by a run rather than by
// the test. One runner parks a durable step and goes away with the record
// still in the hash, the way a service stopped mid-wait does; a second runner
// picks it up and finishes the sequence.
//
// The second runner gets a sequence compiled fresh from the same config, not
// the one the first runner used. That is what a start does: it reads the TOML
// again rather than inheriting the objects the previous process built, so the
// record only survives if a fingerprint the compiler produces a second time
// equals the one that was written down. Handing over the same *Sequence would
// prove no more than that the fingerprint is not empty, and anything the
// fingerprint picked up from its own compile, a pointer, a timestamp, a map
// walk, would strand every real record at the next start with nothing logged
// but a routine drop.
func TestARecordWrittenByARunReplaysIntoANewRunner(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	cfg := rules.RuleConfig{
		Name: "hazards", On: []string{"alarm.triggered"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", After: "80ms"},
			push("c", "3"),
		},
	}
	s := seqWith(t, compileRule(t, cfg, nil), rec.step("on"), rec.step("off"), rec.step("after"))

	first := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	first.Fire(s, eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, "the first runner to park on its durable step", func() bool { return countPending(t, st) == 1 })
	first.Stop()

	if got := rec.list(); !equal(got, []string{"on"}) {
		t.Fatalf("steps ran as %v before the restart, want only on", got)
	}

	// The next start, compiling the same file again.
	restarted := seqWith(t, compileRule(t, cfg, nil), rec.step("on"), rec.step("off"), rec.step("after"))

	second := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	if n := second.Replay([]*Sequence{restarted}, 5*time.Minute); n != 1 {
		t.Fatalf("Replay resumed %d step(s), want 1; a record a run wrote must be one a fresh compile can still read", n)
	}

	waitFor(t, "the resumed run to end", func() bool { return second.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"on", "off", "after"}) {
		t.Errorf("steps ran as %v, want on, off, after", got)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) once the resumed run ended, want 0", n)
	}
}

// TestQueuedTriggersAreNotPersisted pins the ruling: a queue-policy backlog is
// memory only. A trigger that never started a run has latched nothing, and
// replaying a burst of them on boot would fire hardware against a vehicle
// state that has moved on.
// The rule's second step is durable on purpose, so the live run does hold a
// record and "one record, whatever the backlog" is a real distinction rather
// than an empty hash that would hold however the runner behaved.
func TestQueuedTriggersAreNotPersisted(t *testing.T) {
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "queue",
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the live run to park on its durable step", func() bool { return countPending(t, st) == 1 })

	for i := 0; i < 3; i++ {
		rn.Fire(s, eventbus.Event{Topic: "x.y"})
	}

	// Long enough for a runner that recorded its backlog to have done so.
	time.Sleep(30 * time.Millisecond)
	if n := countPending(t, st); n != 1 {
		t.Errorf("%d record(s) with three triggers queued behind a parked run, want 1: the live run's own", n)
	}
	if got := rn.Active(); got != 1 {
		t.Errorf("Active() = %d, want 1; the other three are queued, not running", got)
	}
	if got := loadAll(t, st)[0].Step; got != 1 {
		t.Errorf("the one record names step %d, want 1: the step the live run is parked on", got)
	}
}

// replaySeq builds the two-rule set the replay tests reschedule against.
func replaySeq(t *testing.T, rec *recorder) *Sequence {
	t.Helper()
	r := compileRule(t, rules.RuleConfig{
		Name: "hazards", On: []string{"alarm.triggered"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", After: "1h"},
			push("c", "3"),
		},
	}, nil)
	return seqWith(t, r, rec.step("one"), rec.step("two"), rec.step("three"))
}

func TestReplayDropsEntriesOlderThanTheWindow(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "old", Rule: "hazards", Step: 1,
		FireAt:      time.Now().Add(-10 * time.Minute).UnixMilli(),
		Fingerprint: fingerprintOf(s, 1),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 0 {
		t.Errorf("Replay resumed %d step(s), want 0; a record older than the window is stale", n)
	}

	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left after a stale replay, want 0", n)
	}
	if got := rn.Active(); got != 0 {
		t.Errorf("Active() = %d, want 0", got)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s), want 0", got)
	}
	time.Sleep(30 * time.Millisecond)
	if got := rec.list(); len(got) != 0 {
		t.Errorf("steps ran as %v, want none; a stale step must not fire", got)
	}
	if !log.contains("old") {
		t.Errorf("a dropped record must be logged, got:\n%s", log.all())
	}
}

func TestReplayDropsEntriesForARuleThatNoLongerExists(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "gone", Rule: "deleted-rule", Step: 0,
		FireAt: time.Now().Add(time.Minute).UnixMilli(),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 0 {
		t.Errorf("Replay resumed %d step(s), want 0", n)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left for a rule that no longer exists, want 0", n)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s), want 0", got)
	}
	if !log.contains("deleted-rule") {
		t.Errorf("a dropped record must name the rule, got:\n%s", log.all())
	}
}

func TestReplayDropsEntriesWhoseStepIndexNoLongerExists(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "edited", Rule: "hazards", Step: 7,
		FireAt: time.Now().Add(time.Minute).UnixMilli(),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 0 {
		t.Errorf("Replay resumed %d step(s), want 0", n)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left for a step index that no longer exists, want 0", n)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s), want 0", got)
	}
	if !log.contains("hazards") {
		t.Errorf("a dropped record must name the rule, got:\n%s", log.all())
	}
}

// TestReplayDropsAnEntryWhoseStepHasChanged: the record names a step by
// index, and a user is free to reorder or rewrite their steps while the
// service is down. The seeded fingerprint is a real one, taken from a rule
// whose step 1 pushes something else, so the check has to compare what the
// step does rather than just notice a missing value.
func TestReplayDropsAnEntryWhoseStepHasChanged(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	edited := compileRule(t, rules.RuleConfig{
		Name: "hazards", On: []string{"alarm.triggered"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "something-else", After: "1h"},
			push("c", "3"),
		},
	}, nil)
	before, err := Build(edited, nopPusher{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if err := st.Put(Pending{
		ID: "stale", Rule: "hazards", Step: 1, Source: "hazards.toml",
		FireAt:      time.Now().Add(-time.Minute).UnixMilli(),
		Fingerprint: fingerprintOf(before, 1),
		Event:       eventbus.Event{Topic: "alarm.triggered"},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 0 {
		t.Errorf("Replay resumed %d step(s), want 0; the step at that index is not the one the record was written for", n)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left for an edited step, want 0", n)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s), want 0", got)
	}
	time.Sleep(30 * time.Millisecond)
	if got := rec.list(); len(got) != 0 {
		t.Errorf("steps ran as %v, want none; an edited step must not be fired by an old record", got)
	}
	if !log.contains("hazards.toml") {
		t.Errorf("the drop should point at the file to look in, got:\n%s", log.all())
	}
}

// TestAReplayWindowOfZeroReplaysOnlyWhatIsStillInTheFuture pins the flag's
// edge: at or below zero, nothing past due is run and a step still waiting
// out its delay is kept.
func TestAReplayWindowOfZeroReplaysOnlyWhatIsStillInTheFuture(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	now := time.Now()
	for id, at := range map[string]time.Time{
		"past":   now.Add(-50 * time.Millisecond),
		"future": now.Add(80 * time.Millisecond),
	} {
		if err := st.Put(Pending{
			ID: id, Rule: "hazards", Step: 1,
			FireAt:      at.UnixMilli(),
			Fingerprint: fingerprintOf(s, 1),
			Event:       eventbus.Event{Topic: "alarm.triggered"},
		}); err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 0); n != 1 {
		t.Errorf("Replay resumed %d step(s) with a zero window, want 1: the one still in the future", n)
	}
	if got := sch.Pending(); got != 1 {
		t.Errorf("scheduler has %d pending fire(s), want 1", got)
	}
	got := loadAll(t, st)
	if len(got) != 1 || got[0].ID != "future" {
		t.Fatalf("records after a zero-window replay are %+v, want only future", got)
	}
	if !log.contains("past") {
		t.Errorf("the dropped past-due record must be logged, got:\n%s", log.all())
	}

	waitFor(t, "the future record to run", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"two", "three"}) {
		t.Errorf("steps ran as %v, want two, three; only the future record replays", got)
	}
}

// TestReplayFiresAPastDueEntryWithinTheWindowImmediately gives the recorded
// step an hour-long after, so a replay that re-parked for the step's own delay
// instead of firing what is already due would run nothing at all.
func TestReplayFiresAPastDueEntryWithinTheWindowImmediately(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "due", Rule: "hazards", Step: 1,
		FireAt:      time.Now().Add(-time.Minute).UnixMilli(),
		Fingerprint: fingerprintOf(s, 1),
		Event:       eventbus.Event{Topic: "alarm.triggered"},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
		t.Errorf("Replay resumed %d step(s), want 1", n)
	}
	if got := sch.Pending(); got != 0 {
		t.Fatalf("scheduler has %d pending fire(s) after replaying a past-due step, want 0; what is already due is fired, not parked again", got)
	}

	waitFor(t, "the replayed run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"two", "three"}) {
		t.Errorf("steps ran as %v, want two, three; replay picks the sequence up at the recorded step", got)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left after the replayed step fired, want 0", n)
	}
}

// TestReplayReschedulesAFutureEntryWithTheRemainingDelay uses a step whose own
// after is an hour and a record due in 80 ms: only a replay that honours the
// recorded fire time gets there inside the test's patience.
func TestReplayReschedulesAFutureEntryWithTheRemainingDelay(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "later", Rule: "hazards", Step: 1,
		FireAt:      time.Now().Add(80 * time.Millisecond).UnixMilli(),
		Fingerprint: fingerprintOf(s, 1),
		Event:       eventbus.Event{Topic: "alarm.triggered"},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
		t.Errorf("Replay resumed %d step(s), want 1", n)
	}
	if got := sch.Pending(); got != 1 {
		t.Errorf("scheduler has %d pending fire(s) after replay, want 1; a future record is parked, not fired", got)
	}
	if got := rn.Active(); got != 1 {
		t.Errorf("Active() = %d after replay, want 1", got)
	}
	if got := rec.list(); len(got) != 0 {
		t.Fatalf("steps ran as %v straight after replay, want none; the remaining delay must still be waited out", got)
	}
	if n := countPending(t, st); n != 1 {
		t.Errorf("%d record(s) after replay, want 1; a rescheduled step stays recorded until it fires", n)
	}

	// 80 ms of remaining delay, against the step's own hour: anything that
	// took the step's after instead is still parked when this gives up.
	deadline := time.Now().Add(time.Second)
	for rn.Active() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := rn.Active(); got != 0 {
		t.Fatalf("Active() = %d a second after replaying a step due in 80ms, want 0; the delay must come from the record, not from the step's own after", got)
	}
	if got := rec.list(); !equal(got, []string{"two", "three"}) {
		t.Errorf("steps ran as %v, want two, three", got)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left once the replayed step fired, want 0", n)
	}
}

func TestCorruptPendingJsonIsDroppedAndLoggedNotFatal(t *testing.T) {
	log := &capturingLog{}
	st := liveStore(t, log)

	if err := st.Put(Pending{
		ID: "good", Rule: "hazards", Step: 1, FireAt: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.c.HSet(st.hash, "broken", "{not json"); err != nil {
		t.Fatalf("seed corrupt record: %v", err)
	}

	got, err := st.Load()
	if err != nil {
		t.Fatalf("Load returned %v; one unreadable record must not fail the load", err)
	}
	if len(got) != 1 || got[0].ID != "good" {
		t.Fatalf("Load returned %+v, want only the readable record", got)
	}
	if !log.contains("broken") {
		t.Errorf("an unreadable record must be logged, got:\n%s", log.all())
	}
	if raw, err := st.c.HGetAll(st.hash); err != nil {
		t.Fatalf("HGetAll: %v", err)
	} else if _, still := raw["broken"]; still {
		t.Error("an unreadable record must be removed, or it is logged again on every boot")
	}
}

// TestARefusedStepKeepsItsRecord is the failure durability exists to prevent,
// arriving through the pool rather than through a shutdown. The step that
// turns the hazards off again comes due, the pool has nowhere to put it, and
// the run ends. Nothing ran, so the record is the only thing left in the
// system that can still turn them off, and dropping it leaves the vehicle
// latched with no restart able to recover it.
//
// The record is seeded past due and the pool is filled before the replay, so
// the refusal happens inside Replay itself and the test waits on no timer.
func TestARefusedStepKeepsItsRecord(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "off", Rule: "hazards", Step: 1,
		FireAt:      time.Now().Add(-time.Second).UnixMilli(),
		Fingerprint: fingerprintOf(s, 1),
		Event:       eventbus.Event{Topic: "alarm.triggered"},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// One worker, one queue slot, and both taken: the pinned job holds the
	// worker until the pool's own context is cancelled, and the second sits in
	// the queue behind it.
	pool := startedPool(t, 1, 1)
	if !pool.Submit(ctxAction{}, eventbus.Event{}, "pin", nil) {
		t.Fatal("the pinning job was refused by an empty pool")
	}
	waitFor(t, "the pinning job to reach the worker", func() bool { return pool.Stats().Dispatched == 1 })
	if !pool.Submit(ctxAction{}, eventbus.Event{}, "fill", nil) {
		t.Fatal("the filling job was refused by an empty queue")
	}

	rn := NewRunner(pool, testSched(t), st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
		t.Fatalf("Replay resumed %d step(s), want 1", n)
	}

	if got := rn.Active(); got != 0 {
		t.Errorf("Active() = %d after the refusal, want 0; a refused step ends its run", got)
	}
	if got := rec.list(); len(got) != 0 {
		t.Fatalf("steps ran as %v, want none; the pool had no room for the step", got)
	}
	if n := countPending(t, st); n != 1 {
		t.Fatalf("%d record(s) after a refused step, want 1; the step never ran, so its record is the only thing that can still run it", n)
	}

	// And the record is worth keeping: a start with room in the pool finishes
	// the sequence off it.
	next := NewRunner(startedPool(t, 1, 8), testSched(t), st, log)
	if n := next.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
		t.Fatalf("the next start resumed %d step(s), want 1", n)
	}
	waitFor(t, "the resumed run to end", func() bool { return next.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"two", "three"}) {
		t.Errorf("steps ran as %v at the next start, want two, three", got)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) once the step finally ran, want 0", n)
	}
}

// TestAStepAbandonedInThePoolQueueKeepsItsRecord is the same hazard one step
// further along. The step was accepted, so it is queued rather than refused,
// and then every worker leaves before one reaches it: shutting down while a
// worker is busy abandons whatever is behind it. That step has not run either,
// so its record has to outlive the process the same way.
//
// The pinned job waits on the pool's own context rather than on a gate, so the
// worker lets go exactly when Stop reaches it and cannot drain the queue on
// the way out.
func TestAStepAbandonedInThePoolQueueKeepsItsRecord(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "off", Rule: "hazards", Step: 1,
		FireAt:      time.Now().Add(-time.Second).UnixMilli(),
		Fingerprint: fingerprintOf(s, 1),
		Event:       eventbus.Event{Topic: "alarm.triggered"},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	pool := startedPool(t, 1, 4)
	if !pool.Submit(ctxAction{}, eventbus.Event{}, "pin", nil) {
		t.Fatal("the pinning job was refused by an empty pool")
	}
	waitFor(t, "the pinning job to reach the worker", func() bool { return pool.Stats().Dispatched == 1 })

	rn := NewRunner(pool, testSched(t), st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
		t.Fatalf("Replay resumed %d step(s), want 1", n)
	}
	if got := rec.list(); len(got) != 0 {
		t.Fatalf("steps ran as %v while the only worker was pinned, want none", got)
	}

	// Shutdown order, as main has it: the runner first, then the pool.
	rn.Stop()
	pool.Stop()

	if got := rec.list(); len(got) != 0 {
		t.Fatalf("steps ran as %v across the shutdown, want none; the queued step was abandoned", got)
	}
	if n := countPending(t, st); n != 1 {
		t.Fatalf("%d record(s) after a step was abandoned in the pool queue, want 1", n)
	}
}

// TestReplayDropsAnEntryDatedFurtherAheadThanItsStepCouldWait pins the far
// side of the replay window. A deadline is the moment of the write plus the
// step's own after, so nothing legitimate can sit further ahead than that
// after; a record that does means the clock ran backwards over the restart.
// Resumed verbatim it parks for the length of the jump, holding the rule's
// concurrency slot and its own record, and every reboot arms it again.
func TestReplayDropsAnEntryDatedFurtherAheadThanItsStepCouldWait(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "next-year", Rule: "hazards", Step: 1,
		FireAt:      time.Now().Add(365 * 24 * time.Hour).UnixMilli(),
		Fingerprint: fingerprintOf(s, 1),
		Event:       eventbus.Event{Topic: "alarm.triggered"},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 0 {
		t.Errorf("Replay resumed %d step(s), want 0; the step waits an hour and this record is a year out", n)
	}
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) left for a future-dated entry, want 0; it survives every reboot otherwise", n)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s), want 0", got)
	}
	if got := rn.Active(); got != 0 {
		t.Errorf("Active() = %d, want 0; a phantom run blocks a drop-policy rule for good", got)
	}
	if !log.contains("next-year") {
		t.Errorf("the drop must name the record, got:\n%s", log.all())
	}
}

// TestANegativeReplayWindowMeansTheSameAsZero pins the flag against reading
// itself backwards. Zero replays only what is still in the future; anything
// below zero says the same thing, and must not start dropping future steps for
// being past a limit that is itself in the past.
func TestANegativeReplayWindowMeansTheSameAsZero(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	now := time.Now()
	for id, at := range map[string]time.Time{
		"past":   now.Add(-50 * time.Millisecond),
		"future": now.Add(80 * time.Millisecond),
	} {
		if err := st.Put(Pending{
			ID: id, Rule: "hazards", Step: 1,
			FireAt:      at.UnixMilli(),
			Fingerprint: fingerprintOf(s, 1),
			Event:       eventbus.Event{Topic: "alarm.triggered"},
		}); err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, -time.Minute); n != 1 {
		t.Errorf("Replay resumed %d step(s) with a negative window, want 1: the one still in the future", n)
	}
	if got := sch.Pending(); got != 1 {
		t.Errorf("scheduler has %d pending fire(s), want 1", got)
	}
	got := loadAll(t, st)
	if len(got) != 1 || got[0].ID != "future" {
		t.Fatalf("records after a negative-window replay are %+v, want only future", got)
	}

	waitFor(t, "the future record to run", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"two", "three"}) {
		t.Errorf("steps ran as %v, want two, three", got)
	}
}

// TestReplayAppliesTheRestartPolicyAcrossTwoRecords: one rule can end up with
// two records, because a removal that failed at the last shutdown leaves one
// behind that its run had already finished with. A live trigger meeting a run
// in flight goes through the rule's concurrency policy, and a resumed record
// has to go through the same gate, or a restart-policy rule comes back up
// running its tail twice.
func TestReplayAppliesTheRestartPolicyAcrossTwoRecords(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	now := time.Now()
	for id, at := range map[string]time.Time{
		"first":  now.Add(60 * time.Millisecond),
		"second": now.Add(120 * time.Millisecond),
	} {
		if err := st.Put(Pending{
			ID: id, Rule: "hazards", Step: 1,
			FireAt:      at.UnixMilli(),
			Fingerprint: fingerprintOf(s, 1),
			Event:       eventbus.Event{Topic: "alarm.triggered"},
		}); err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
	}

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
		t.Errorf("Replay reported %d resumed step(s), want 1; restart leaves one run, not two", n)
	}
	if got := rn.Active(); got != 1 {
		t.Errorf("Active() = %d after replaying two records for one restart rule, want 1", got)
	}
	if got := sch.Pending(); got != 1 {
		t.Errorf("scheduler has %d pending fire(s), want 1; the replaced run's tail must be cancelled", got)
	}
	// Records are replayed oldest deadline first, so the newest is the one
	// left standing, exactly as a live restart keeps the newest trigger.
	got := loadAll(t, st)
	if len(got) != 1 || got[0].ID != "second" {
		t.Fatalf("records after the replay are %+v, want only second", got)
	}

	waitFor(t, "the surviving run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"two", "three"}) {
		t.Errorf("steps ran as %v, want two, three once; a replay must not fire the tail twice", got)
	}
}

// TestReplayKeepsOneRecordUnderDropAndQueue is the same gate under the other
// two policies, both of which keep the earliest record and throw the rest
// away. Throwing one away means removing it, or it comes back at every boot to
// be thrown away again.
//
// Queue has nothing to queue behind here. A backlog is memory only and never
// crosses a restart, so the only thing that puts two records on one rule is a
// removal that failed at the last shutdown, and both then name the same step.
// Resuming them side by side would run one tail twice and hand a queue rule
// the overlap it exists to forbid.
func TestReplayKeepsOneRecordUnderDropAndQueue(t *testing.T) {
	for _, policy := range []string{"drop", "queue"} {
		t.Run(policy, func(t *testing.T) {
			log := &capturingLog{}
			st, _ := memStore(t, log)
			rec := &recorder{}
			r := compileRule(t, rules.RuleConfig{
				Name: "hazards", On: []string{"alarm.triggered"}, Concurrency: policy,
				Steps: []rules.StepConfig{
					push("a", "1"),
					{Do: "redis", List: "b", Push: "2", After: "1h"},
					push("c", "3"),
				},
			}, nil)
			s := seqWith(t, r, rec.step("one"), rec.step("two"), rec.step("three"))

			now := time.Now()
			for id, at := range map[string]time.Time{
				"first":  now.Add(60 * time.Millisecond),
				"second": now.Add(120 * time.Millisecond),
			} {
				if err := st.Put(Pending{
					ID: id, Rule: "hazards", Step: 1,
					FireAt:      at.UnixMilli(),
					Fingerprint: fingerprintOf(s, 1),
					Event:       eventbus.Event{Topic: "alarm.triggered"},
				}); err != nil {
					t.Fatalf("Put %s: %v", id, err)
				}
			}

			sch := testSched(t)
			rn := NewRunner(startedPool(t, 1, 8), sch, st, log)
			if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
				t.Errorf("Replay reported %d resumed step(s), want 1; %s keeps the first and ignores the rest", n, policy)
			}
			if got := rn.Active(); got != 1 {
				t.Errorf("Active() = %d after replaying two records, want 1", got)
			}
			if got := sch.Pending(); got != 1 {
				t.Errorf("scheduler has %d pending fire(s), want 1", got)
			}
			got := loadAll(t, st)
			if len(got) != 1 || got[0].ID != "first" {
				t.Fatalf("records after the replay are %+v, want only first", got)
			}
			if !log.contains("second") {
				t.Errorf("the dropped record must be logged, got:\n%s", log.all())
			}

			waitFor(t, "the surviving run to end", func() bool { return rn.Active() == 0 })
			if got := rec.list(); !equal(got, []string{"two", "three"}) {
				t.Errorf("steps ran as %v, want two, three once", got)
			}
		})
	}
}

// TestAStepDrainedFromThePoolDuringShutdownRemovesItsRecord covers the gap
// between the runner stopping and the pool stopping. Shutdown ends the runs
// and leaves their records for the next start, but the workers are still
// draining the queue while that happens, so a step handed over before the
// signal can still run. Whichever way that lands, the record and the action
// must agree: if the action runs, the record has to go, or the next start
// fires the same hardware a second time with nothing having asked it to.
func TestAStepDrainedFromThePoolDuringShutdownRemovesItsRecord(t *testing.T) {
	log := &capturingLog{}
	st, _ := memStore(t, log)
	rec := &recorder{}
	s := replaySeq(t, rec)

	if err := st.Put(Pending{
		ID: "off", Rule: "hazards", Step: 1,
		FireAt:      time.Now().Add(-time.Second).UnixMilli(),
		Fingerprint: fingerprintOf(s, 1),
		Event:       eventbus.Event{Topic: "alarm.triggered"},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	gate, open := newGate()
	defer open()
	pool := startedPool(t, 1, 4)
	if !pool.Submit(gatedAction{gate: gate}, eventbus.Event{}, "pin", nil) {
		t.Fatal("the pinning job was refused by an empty pool")
	}
	waitFor(t, "the pinning job to reach the worker", func() bool { return pool.Stats().Dispatched == 1 })

	rn := NewRunner(pool, testSched(t), st, log)
	if n := rn.Replay([]*Sequence{s}, 5*time.Minute); n != 1 {
		t.Fatalf("Replay resumed %d step(s), want 1", n)
	}
	if got := rec.list(); len(got) != 0 {
		t.Fatalf("steps ran as %v while the only worker was pinned, want none", got)
	}

	// The runner stops, and the pool has not been told yet: this is the window
	// main leaves open between en.Stop() and pool.Stop().
	rn.Stop()
	open()

	waitFor(t, "the queued step to be drained by the worker", func() bool { return len(rec.list()) == 1 })
	if n := countPending(t, st); n != 0 {
		t.Fatalf("%d record(s) left for a step that ran during the shutdown drain, want 0; the next start would run it again", n)
	}
}
