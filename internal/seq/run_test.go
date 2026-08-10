package seq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/event-service/internal/shadow"
	"github.com/librescoot/eventbus"
)

type nopLog struct{}

func (nopLog) Printf(string, ...any) {}

// fnAction is an Action with a body the test controls, so a step can fail,
// block, or record itself without a datastore.
type fnAction struct {
	fn func() error
}

func (a fnAction) Do(context.Context, eventbus.Event) error { return a.fn() }
func (fnAction) Kind() string                               { return "test" }

type recorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *recorder) step(name string) action.Action {
	return fnAction{fn: func() error {
		r.mu.Lock()
		r.seen = append(r.seen, name)
		r.mu.Unlock()
		return nil
	}}
}

func (r *recorder) failing(name string, err error) action.Action {
	return fnAction{fn: func() error {
		r.mu.Lock()
		r.seen = append(r.seen, name)
		r.mu.Unlock()
		return err
	}}
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

func seqWith(t *testing.T, r *rules.Rule, acts ...action.Action) *Sequence {
	t.Helper()
	if len(acts) != len(r.Steps) {
		t.Fatalf("test bug: %d actions for %d steps", len(acts), len(r.Steps))
	}
	s := &Sequence{Rule: r, Steps: make([]CompiledStep, len(acts))}
	for i, a := range acts {
		s.Steps[i] = CompiledStep{Action: a, When: r.Steps[i].When, After: r.Steps[i].After}
	}
	return s
}

func startedPool(t *testing.T, workers, queue int) *action.Pool {
	t.Helper()
	p := action.NewPool(workers, queue, nopLog{})
	p.Start()
	t.Cleanup(p.Stop)
	return p
}

func testSched(t *testing.T) *sched.Scheduler {
	t.Helper()
	s := sched.New()
	t.Cleanup(s.Stop)
	return s
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

func equal(a, b []string) bool {
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

func TestTwoStepsRunInOrder(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2"), push("c", "3")},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"), rec.step("three"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one", "two", "three"}) {
		t.Errorf("steps ran as %v, want one, two, three", got)
	}
}

func TestSecondStepDoesNotRunWhenFirstFails(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2")},
	}, nil)
	s := seqWith(t, r, rec.failing("one", errors.New("boom")), rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one; a failed step ends the run", got)
	}
}

func TestStepWhenFalseStopsTheRunWithoutError(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", When: `to == "moving"`},
			push("c", "3"),
		},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"), rec.step("three"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y", To: "parked"})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one; a false step when ends the run", got)
	}
}

func TestStepWhenSeesTheTriggeringEvent(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"battery.inserted"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{
				Do: "redis", List: "b", Push: "2",
				When: `topic == "battery.inserted" && from == "absent" && to == "present" && data.slot == 1`,
			},
		},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{
		Topic: "battery.inserted", From: "absent", To: "present",
		Data: map[string]any{"slot": 1},
	})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one", "two"}) {
		t.Errorf("steps ran as %v, want one, two; a step when sees the triggering event", got)
	}
}

func TestStepWhenCanReadStateFromTheShadowStore(t *testing.T) {
	sh := shadow.NewStore()
	sh.Observe("alarm", "status", "armed")

	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"motion.detected"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", When: `state("alarm", "status") == "armed"`},
		},
	}, sh.Get)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "motion.detected"})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one", "two"}) {
		t.Errorf("steps ran as %v, want one, two; state() must be readable from a step when", got)
	}
}

// TestRunEndsWhenPoolRefusesAStep fills the queue from inside the first step,
// so the pool's only worker is still executing the completion callback when
// the second step is submitted and the refusal is deterministic rather than
// timing-dependent.
func TestRunEndsWhenPoolRefusesAStep(t *testing.T) {
	pool := startedPool(t, 1, 2)
	filler := fnAction{fn: func() error { return nil }}

	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2")},
	}, nil)
	first := fnAction{fn: func() error {
		rec.mu.Lock()
		rec.seen = append(rec.seen, "one")
		rec.mu.Unlock()
		for pool.Submit(filler, eventbus.Event{}, "filler", nil) {
		}
		return nil
	}}
	s := seqWith(t, r, first, rec.step("two"))

	rn := NewRunner(pool, testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one; a refused step ends the run", got)
	}
	if pool.Stats().Dropped == 0 {
		t.Error("a refused step should be counted as dropped")
	}
}

func TestActiveCountsARunUntilItEnds(t *testing.T) {
	release := make(chan struct{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2")},
	}, nil)
	blocking := fnAction{fn: func() error {
		<-release
		return nil
	}}
	s := seqWith(t, r, blocking, rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	if n := rn.Active(); n != 1 {
		t.Fatalf("Active() = %d while a step is running, want 1", n)
	}
	close(release)
	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"two"}) {
		t.Errorf("steps ran as %v, want two", got)
	}
}

func TestStopAbandonsRunsAndRefusesNewOnes(t *testing.T) {
	release := make(chan struct{})
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2")},
	}, nil)
	blocking := fnAction{fn: func() error {
		<-release
		return nil
	}}
	s := seqWith(t, r, blocking, rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	rn.Stop()
	if n := rn.Active(); n != 0 {
		t.Errorf("Active() = %d after Stop, want 0", n)
	}
	close(release)

	rn.Fire(seqWith(t, r, rec.step("three"), rec.step("four")), eventbus.Event{Topic: "x.y"})

	time.Sleep(50 * time.Millisecond)
	if got := rec.list(); len(got) != 0 {
		t.Errorf("steps ran as %v after Stop, want none", got)
	}
}

func TestStepWithAfterDoesNotRunImmediately(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "100ms"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "step one to run", func() bool { return len(rec.list()) >= 1 })
	// A wide margin short of the 100ms delay: a step with after must not be
	// submitted the moment the step before it finishes.
	time.Sleep(20 * time.Millisecond)
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v before the delay elapsed, want only one", got)
	}
}

func TestStepWithAfterEventuallyRuns(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "20ms"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one", "two"}) {
		t.Errorf("steps ran as %v, want one, two; a delayed step must still run", got)
	}
}

// TestStepWhenIsReEvaluatedAtFireTimeNotAtScheduleTime is the point of a
// step-level condition on a deferred step. Step one flips alarm status the
// moment it runs, well before the second step's 30ms delay is over, so a
// when checked at schedule time would still see "armed" and a when checked
// at fire time would see "disarmed". Only the latter reading agrees with
// this test's expectation that step two does not run.
func TestStepWhenIsReEvaluatedAtFireTimeNotAtScheduleTime(t *testing.T) {
	sh := shadow.NewStore()
	sh.Observe("alarm", "status", "armed")

	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"motion.detected"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", After: "30ms", When: `state("alarm", "status") == "armed"`},
		},
	}, sh.Get)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "motion.detected"})

	waitFor(t, "step one to run", func() bool { return len(rec.list()) >= 1 })
	sh.Observe("alarm", "status", "disarmed")

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one; when must be re-evaluated at fire time", got)
	}
}

// TestPendingStepOccupiesNoWorker is the whole reason after exists: a step
// waiting out its delay must hold no worker, so the pool's one worker stays
// free for anything else.
func TestPendingStepOccupiesNoWorker(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "200ms"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	pool := startedPool(t, 1, 8)
	rn := NewRunner(pool, testSched(t), nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "step one to run", func() bool { return len(rec.list()) >= 1 })

	unrelated := make(chan struct{})
	start := time.Now()
	if !pool.Submit(fnAction{fn: func() error { close(unrelated); return nil }}, eventbus.Event{}, "unrelated", nil) {
		t.Fatal("unrelated action was refused")
	}
	select {
	case <-unrelated:
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated action never ran")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("unrelated action took %v to run; a pending step must not occupy the pool's only worker", elapsed)
	}

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
}

// TestRunnerStopCancelsAPendingTail uses an hour-long after so the timer
// cannot possibly have fired by the time Stop returns. Pending is checked
// immediately, with no sleep in between: any margin here would let the
// timer's own self-removal on fire, rather than Stop's cancel, account for
// an empty registry, which proves nothing about Stop.
func TestRunnerStopCancelsAPendingTail(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "step one to run", func() bool { return len(rec.list()) >= 1 })
	rn.Stop()

	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s) right after Stop, want 0; Stop must cancel a pending tail", got)
	}
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v after Stop, want only one", got)
	}
}

func TestNegativeAfterIsRejectedAtCompileTimeNamingTheStep(t *testing.T) {
	_, errs := rules.Compile([]rules.RuleConfig{{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "-5s"}},
	}}, func(string, string) string { return "" })
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{`"r"`, "step 1"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %s, got %q", want, msg)
		}
	}
}
