package main

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/engine"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/event-service/internal/seq"
	"github.com/librescoot/eventbus"
)

type nopLog struct{}

func (nopLog) Printf(string, ...any) {}

type nopPusher struct{}

func (nopPusher) LPush(string, ...any) (int64, error) { return 1, nil }

// memPending is the pending hash in memory. seq.NewPendingStore writes to one
// fixed hash name, so a live datastore would have these tests reading records
// belonging to another package's tests, or to a service running on the same
// box. It counts writes, because the zero-rules path has to add no work at
// all rather than merely end up with an empty hash.
type memPending struct {
	mu     sync.Mutex
	fields map[string]string
	writes int
}

func newMemPending() *memPending { return &memPending{fields: make(map[string]string)} }

func (h *memPending) HSet(_, field string, value any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fields[field] = fmt.Sprint(value)
	h.writes++
	return nil
}

func (h *memPending) HGetAll(string) (map[string]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]string, len(h.fields))
	for k, v := range h.fields {
		out[k] = v
	}
	return out, nil
}

func (h *memPending) HDel(_ string, fields ...string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range fields {
		delete(h.fields, f)
	}
	return nil
}

func (h *memPending) written() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.writes
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within 2s")
	}
}

// TestBuildSnapshotKeepsDroppedAndRefusedApart is the field-mapping test
// dropped and refused need: dropped must come from the pool alone and
// refused from the runner alone, with neither leaking into the other. The
// two are driven to different, nonzero, mutually distinguishable counts so
// a swap between them is not accidentally invisible.
func TestBuildSnapshotKeepsDroppedAndRefusedApart(t *testing.T) {
	// A one-slot, unstarted pool: the first Submit fills the queue, the
	// second finds it full and is dropped. Unstarted, so there is nothing
	// to Stop.
	pool := action.NewPool(1, 1, nopLog{})
	act, err := action.Build(action.Spec{Do: "redis", List: "x", Push: "1"}, nopPusher{})
	if err != nil {
		t.Fatalf("action.Build: %v", err)
	}
	pool.Submit(act, eventbus.Event{}, "filler", nil)
	pool.Submit(act, eventbus.Event{}, "filler", nil) // dropped: queue full

	if got := pool.Stats().Dropped; got != 1 {
		t.Fatalf("pool dropped %d job(s) setting up the test, want 1", got)
	}

	// A queue-policy rule whose only step parks for an hour, so the live
	// run never ends for the length of the test. maxQueued is 8
	// (internal/seq): the first Handle starts the run, the next 8 fill the
	// backlog, and the remaining 5 are refused.
	sch := sched.New()
	defer sch.Stop()
	rs, errs := rules.Compile([]rules.RuleConfig{{
		Name: "r", On: []string{"x.y"}, Concurrency: "queue",
		Steps: []rules.StepConfig{{Do: "redis", List: "a", Push: "1", After: "1h"}},
	}}, func(string, string) string { return "" })
	if len(errs) != 0 {
		t.Fatalf("compile: %v", errs)
	}
	runnerPool := action.NewPool(1, 8, nopLog{})
	runnerPool.Start()
	defer runnerPool.Stop()
	en, buildErrs := engine.New(rs, runnerPool, sch, nil, nopPusher{}, nopLog{})
	if len(buildErrs) != 0 {
		t.Fatalf("engine.New: %v", buildErrs)
	}
	defer en.Stop()

	for i := 0; i < 1+8+5; i++ {
		en.Handle(eventbus.Event{Topic: "x.y"})
	}
	waitFor(t, func() bool { return en.Refused() == 5 })

	got := buildSnapshot(pool, sch, en, "v1.2.3")

	if got["dropped"] != strconv.FormatUint(pool.Stats().Dropped, 10) {
		t.Errorf(`snapshot["dropped"] = %q, want the pool's own Dropped (%d)`, got["dropped"], pool.Stats().Dropped)
	}
	if got["refused"] != strconv.FormatUint(en.Refused(), 10) {
		t.Errorf(`snapshot["refused"] = %q, want the runner's own Refused (%d)`, got["refused"], en.Refused())
	}
	if got["dropped"] == got["refused"] {
		t.Fatalf("dropped and refused came out equal (%q); the test cannot tell a swap from a correct mapping this way", got["dropped"])
	}
	if got["dropped"] != "1" {
		t.Errorf(`snapshot["dropped"] = %q, want "1"; it must not include the runner's refusals`, got["dropped"])
	}
	if got["refused"] != "5" {
		t.Errorf(`snapshot["refused"] = %q, want "5"; it must not include the pool's drops`, got["refused"])
	}
}

// TestStartRulesReplaysBeforeItSubscribes is the only mechanical check on an
// ordering that is otherwise a matter of reading main from top to bottom. A
// step recorded before the restart has to be back in the runner before the
// bus can deliver anything, or a live event re-firing the same rule meets a
// concurrency policy applied against a run that has not been resumed yet: the
// restart drops nothing, and the resumed tail and the fresh run both push.
//
// The seeded record is an hour from due, so the run it resumes is parked and
// countable for the whole test rather than racing the assertion.
func TestStartRulesReplaysBeforeItSubscribes(t *testing.T) {
	h := newMemPending()
	store := seq.NewPendingStore(h, nopLog{})

	rs, errs := rules.Compile([]rules.RuleConfig{{
		Name: "hazards-on-alarm", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.disarmed"},
		Steps: []rules.StepConfig{
			{Do: "redis", List: "scooter:blinker", Push: "both"},
			{Do: "redis", List: "scooter:blinker", Push: "off", After: "1h"},
		},
	}}, func(string, string) string { return "" })
	if len(errs) != 0 {
		t.Fatalf("compile: %v", errs)
	}

	if err := store.Put(seq.Pending{
		ID: "hazards-on-alarm#1-1", Rule: "hazards-on-alarm", Step: 1,
		FireAt:      time.Now().Add(time.Hour).UnixMilli(),
		Fingerprint: rs[0].Steps[1].Fingerprint,
		Event:       eventbus.Event{Topic: "alarm.triggered"},
	}); err != nil {
		t.Fatalf("seed a pending record: %v", err)
	}

	sch := sched.New()
	defer sch.Stop()
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()
	en, buildErrs := engine.New(rs, pool, sch, store, nopPusher{}, nopLog{})
	if len(buildErrs) != 0 {
		t.Fatalf("engine.New: %v", buildErrs)
	}
	defer en.Stop()

	var (
		subscribes  int
		patterns    []string
		activeAtSub int
		closed      bool
	)
	stop := startRules(en, 5*time.Minute, func(p []string) func() {
		subscribes++
		patterns = p
		activeAtSub = en.Active()
		return func() { closed = true }
	}, nopLog{})

	if subscribes != 1 {
		t.Fatalf("subscribed %d time(s), want 1", subscribes)
	}
	if activeAtSub != 1 {
		t.Errorf("%d run(s) were live when the subscription opened, want 1; the recorded step must be resumed before the bus can deliver an event that fires the same rule", activeAtSub)
	}
	want := []string{eventbus.ChannelPrefix + "alarm.triggered", eventbus.ChannelPrefix + "alarm.disarmed"}
	if !equalStrings(patterns, want) {
		t.Errorf("subscribed to %v, want %v", patterns, want)
	}

	stop()
	if !closed {
		t.Error("the function startRules returns must close the subscription it opened, or shutdown leaves the bus reader running")
	}
}

// TestStartRulesDoesNotSubscribeWithNoRules is what keeps an idle scooter at
// zero events and zero datastore writes per second. With nothing installed in
// the extensions directory there is nothing to match, so the process must not
// be woken by bus traffic at all, and nothing may be scheduled or recorded.
func TestStartRulesDoesNotSubscribeWithNoRules(t *testing.T) {
	h := newMemPending()
	store := seq.NewPendingStore(h, nopLog{})

	sch := sched.New()
	defer sch.Stop()
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()
	en, buildErrs := engine.New(nil, pool, sch, store, nopPusher{}, nopLog{})
	if len(buildErrs) != 0 {
		t.Fatalf("engine.New: %v", buildErrs)
	}
	defer en.Stop()

	stop := startRules(en, 5*time.Minute, func(patterns []string) func() {
		t.Errorf("subscribed to %v with no rules live; an idle scooter must not be woken by bus traffic nothing can match", patterns)
		return func() {}
	}, nopLog{})
	stop()

	if got := sch.Pending(); got != 0 {
		t.Errorf("the scheduler holds %d pending fire(s) with no rules live, want 0", got)
	}
	if got := en.Active(); got != 0 {
		t.Errorf("%d run(s) live with no rules live, want 0", got)
	}
	if got := h.written(); got != 0 {
		t.Errorf("the pending store was written %d time(s) with no rules live, want 0", got)
	}
}
