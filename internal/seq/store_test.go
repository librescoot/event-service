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
type memHash struct {
	mu sync.Mutex
	m  map[string]map[string]string
}

func newMemHash() *memHash { return &memHash{m: make(map[string]map[string]string)} }

func (h *memHash) HSet(key, field string, value any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.m[key] == nil {
		h.m[key] = make(map[string]string)
	}
	h.m[key][field] = fmt.Sprint(value)
	return nil
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

// liveHasher is the same surface over a real datastore client, which has no
// HDel of its own.
type liveHasher struct{ c *ipc.Client }

func (h liveHasher) HSet(key, field string, value any) error {
	return h.c.HSet(key, field, value)
}

func (h liveHasher) HGetAll(key string) (map[string]string, error) {
	return h.c.HGetAll(key)
}

func (h liveHasher) HDel(key string, fields ...string) error {
	args := make([]any, 0, len(fields)+1)
	args = append(args, key)
	for _, f := range fields {
		args = append(args, f)
	}
	_, err := h.c.Do("HDEL", args...)
	return err
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
	return newPendingStoreIn(liveHasher{c: client}, log, hash)
}

func memStore(t *testing.T, log Logger) (*PendingStore, *memHash) {
	t.Helper()
	h := newMemHash()
	return newPendingStoreIn(h, log, testHashName(t)), h
}

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
	waitFor(t, "the record to be removed", func() bool { return countPending(t, st) == 0 })
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
	waitFor(t, "the old record to be replaced", func() bool {
		got := loadAll(t, st)
		return len(got) == 1 && got[0].ID != first
	})
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

// TestQueuedTriggersAreNotPersisted pins the ruling: a queue-policy backlog is
// memory only. A trigger that never started a run has latched nothing, and
// replaying a burst of them on boot would fire hardware against a vehicle
// state that has moved on.
func TestQueuedTriggersAreNotPersisted(t *testing.T) {
	gate, open := newGate()
	defer open()
	st, _ := memStore(t, nopLog{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "queue",
		Steps: []rules.StepConfig{push("a", "1")},
	}, nil)
	s := seqWith(t, r, rec.gated("run", gate))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), st, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the first run to start", func() bool { return len(rec.list()) == 1 })
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) written for a queued backlog, want 0", n)
	}
	open()
	waitFor(t, "the backlog to drain", func() bool { return len(rec.list()) == 3 })
	if n := countPending(t, st); n != 0 {
		t.Errorf("%d record(s) once the backlog drained, want 0", n)
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
		FireAt: time.Now().Add(-10 * time.Minute).UnixMilli(),
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
		FireAt: time.Now().Add(-time.Minute).UnixMilli(),
		Event:  eventbus.Event{Topic: "alarm.triggered"},
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
		FireAt: time.Now().Add(80 * time.Millisecond).UnixMilli(),
		Event:  eventbus.Event{Topic: "alarm.triggered"},
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
