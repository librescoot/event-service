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

// ctxAction holds its worker until the pool cancels the context every action
// runs under, which is what Pool.Stop does before it waits. A test that pins a
// worker with this one has it let go at exactly the point Stop reaches it,
// with no sleep to tune and no gate to open at the right moment.
type ctxAction struct{}

func (ctxAction) Do(ctx context.Context, _ eventbus.Event) error {
	<-ctx.Done()
	return nil
}
func (ctxAction) Kind() string { return "test" }

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

// newGate returns a gate for a gated step and the function that opens it.
// Opening twice is safe, so a test can defer the open and still call it where
// it means to. Deferring matters: a test that fails an assertion before its
// own open would otherwise leave a worker parked on the gate forever, and
// Pool.Stop, which the pool's own cleanup calls afterwards, waits for that
// worker. A clean failure would turn into a hung package.
func newGate() (chan struct{}, func()) {
	gate := make(chan struct{})
	var once sync.Once
	return gate, func() { once.Do(func() { close(gate) }) }
}

// gated is a step that records itself and then waits for the test to open
// gate, so a run can be held mid-flight without a timer deciding when.
func (r *recorder) gated(name string, gate <-chan struct{}) action.Action {
	return fnAction{fn: func() error {
		r.add(name)
		<-gate
		return nil
	}}
}

func (r *recorder) add(name string) {
	r.mu.Lock()
	r.seen = append(r.seen, name)
	r.mu.Unlock()
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
		s.Steps[i] = CompiledStep{
			Action:      a,
			When:        r.Steps[i].When,
			After:       r.Steps[i].After,
			Durable:     r.Steps[i].Durable,
			Fingerprint: r.Steps[i].Fingerprint,
		}
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

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
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

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
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

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
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

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
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

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
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

	rn := NewRunner(pool, testSched(t), nil, nopLog{})
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
	gate, open := newGate()
	defer open()
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2")},
	}, nil)
	blocking := fnAction{fn: func() error {
		<-gate
		return nil
	}}
	s := seqWith(t, r, blocking, rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	if n := rn.Active(); n != 1 {
		t.Fatalf("Active() = %d while a step is running, want 1", n)
	}
	open()
	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"two"}) {
		t.Errorf("steps ran as %v, want two", got)
	}
}

func TestStopAbandonsRunsAndRefusesNewOnes(t *testing.T) {
	gate, open := newGate()
	defer open()
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2")},
	}, nil)
	blocking := fnAction{fn: func() error {
		<-gate
		return nil
	}}
	s := seqWith(t, r, blocking, rec.step("two"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	rn.Stop()
	if n := rn.Active(); n != 0 {
		t.Errorf("Active() = %d after Stop, want 0", n)
	}
	open()

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

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
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

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
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

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
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
	rn := NewRunner(pool, testSched(t), nil, nopLog{})
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
	rn := NewRunner(startedPool(t, 1, 8), sch, nil, nopLog{})
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

// TestRestartCancelsThePendingTailAndStartsOver parks the first run on an
// hour-long tail, so the only way the scheduler can be down to one pending
// fire after the second trigger is that the first run's tail was dropped.
// Counting pending fires is the check that separates restart from a runner
// that simply lets both runs live.
func TestRestartCancelsThePendingTailAndStartsOver(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "restart",
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the first run to park on its tail", func() bool { return sch.Pending() == 1 })

	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the restarted run to park on its tail", func() bool {
		return len(rec.list()) == 2 && sch.Pending() >= 1
	})

	if got := sch.Pending(); got != 1 {
		t.Errorf("scheduler has %d pending fire(s) after a restart, want 1; the old tail must be dropped", got)
	}
	if got := rn.Active(); got != 1 {
		t.Errorf("Active() = %d after a restart, want 1; a restart replaces the run rather than adding one", got)
	}
	if got := rec.list(); !equal(got, []string{"one", "one"}) {
		t.Errorf("steps ran as %v, want one, one; a restart starts the sequence over", got)
	}
}

// TestRestartIsTheDefaultWhenConcurrencyIsOmitted repeats the restart check
// with the key left out and a tail short enough to land, so the restarted run
// is seen through to its end.
func TestRestartIsTheDefaultWhenConcurrencyIsOmitted(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "60ms"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the first run to park on its tail", func() bool { return sch.Pending() == 1 })
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the restarted run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one", "one", "two"}) {
		t.Errorf("steps ran as %v, want one, one, two; an omitted concurrency means restart", got)
	}
	// The dropped tail would land around here if it had not been cancelled.
	time.Sleep(80 * time.Millisecond)
	if got := rec.list(); !equal(got, []string{"one", "one", "two"}) {
		t.Errorf("steps ran as %v once the first tail's delay had passed, want one, one, two", got)
	}
}

// TestDropIgnoresAReFireWhileARunIsLive holds the first run inside a step
// rather than on a timer, so "a run is live" is a fact rather than a race,
// and then fires a third time after that run has ended: drop must mean
// "ignored while busy", not "fires once and never again".
//
// The pool has two workers on purpose. With one, a restart would look exactly
// like a drop from the outside: the replacement run's opening step would sit
// in the pool queue behind the blocked worker, recording nothing, and the run
// count would be 1 either way. The spare worker gives a replacement run
// somewhere to run, so the middle assertion tells the two apart.
func TestDropIgnoresAReFireWhileARunIsLive(t *testing.T) {
	gate, open := newGate()
	defer open()
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "drop",
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2")},
	}, nil)
	s := seqWith(t, r, rec.gated("one", gate), rec.step("two"))

	rn := NewRunner(startedPool(t, 2, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the first step to start", func() bool { return len(rec.list()) == 1 })

	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	// Long enough for a replacement run to reach the free worker and record
	// its opening step, which is what a restart would do here.
	time.Sleep(30 * time.Millisecond)
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one; the re-fire started a run instead of being ignored", got)
	}
	if got := rn.Active(); got != 1 {
		t.Errorf("Active() = %d after a dropped re-fire, want 1", got)
	}

	open()
	waitFor(t, "the first run to end", func() bool { return rn.Active() == 0 })

	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the third fire to run", func() bool { return len(rec.list()) == 4 })
	if got := rec.list(); !equal(got, []string{"one", "two", "one", "two"}) {
		t.Errorf("steps ran as %v, want one, two, one, two; drop must not latch the rule off", got)
	}
}

// TestQueueRunsSequencesBackToBack gives the pool two workers, so a runner
// that started the queued fires instead of holding them would have somewhere
// to run them and the interleaving would show.
func TestQueueRunsSequencesBackToBack(t *testing.T) {
	gate, open := newGate()
	defer open()
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "queue",
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2")},
	}, nil)
	s := seqWith(t, r, rec.gated("start", gate), rec.step("end"))

	rn := NewRunner(startedPool(t, 2, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the first run to start", func() bool { return len(rec.list()) == 1 })

	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	// A queued fire holds no worker and starts nothing: with the first run
	// stuck in its opening step, the other two must still be waiting.
	time.Sleep(30 * time.Millisecond)
	if got := rec.list(); !equal(got, []string{"start"}) {
		t.Fatalf("steps ran as %v while the first run was still in its first step, want only start", got)
	}

	open()
	waitFor(t, "all three runs to end", func() bool { return len(rec.list()) == 6 })
	want := []string{"start", "end", "start", "end", "start", "end"}
	if got := rec.list(); !equal(got, want) {
		t.Errorf("steps ran as %v, want %v; queued runs go back to back, not side by side", got, want)
	}
}

// TestQueueIsBoundedAndCountsRefusals fires far past the bound while the
// first run is held, so the backlog cannot drain underneath the test. A
// flapping trigger must cost a fixed amount of memory, not a growing one.
func TestQueueIsBoundedAndCountsRefusals(t *testing.T) {
	gate, open := newGate()
	defer open()
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "queue",
		Steps: []rules.StepConfig{push("a", "1")},
	}, nil)
	s := seqWith(t, r, rec.gated("run", gate))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the first run to start", func() bool { return len(rec.list()) == 1 })

	for i := 0; i < 12; i++ {
		rn.Fire(s, eventbus.Event{Topic: "x.y"})
	}
	if got := rn.Refused(); got != 4 {
		t.Errorf("Refused() = %d after 12 fires behind a live run, want 4; the queue holds 8", got)
	}

	open()
	waitFor(t, "the queue to drain", func() bool { return len(rec.list()) == 9 })
	time.Sleep(30 * time.Millisecond)
	if got := len(rec.list()); got != 9 {
		t.Errorf("%d runs executed, want 9: the live one plus a queue of 8", got)
	}
	if got := rn.Refused(); got != 4 {
		t.Errorf("Refused() = %d once the queue drained, want 4", got)
	}
}

// TestCancelOnDropsThePendingTail checks the scheduler, not just the
// recorder: a cancel that only marked the run ended and left the timer
// running would still keep step two from firing, and would still leak a
// timer per cancel.
func TestCancelOnDropsThePendingTail(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.disarmed"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})

	waitFor(t, "the run to park on its tail", func() bool { return sch.Pending() == 1 })

	if got := rn.CancelMatching("alarm.disarmed"); got != 1 {
		t.Errorf("CancelMatching returned %d, want 1", got)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s) right after the cancel, want 0", got)
	}
	if got := rn.Active(); got != 0 {
		t.Errorf("Active() = %d after the cancel, want 0", got)
	}
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one", got)
	}
}

func TestCancelOnMatchesGlobs(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.*"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "redis", List: "b", Push: "2", After: "1h"}},
	}, nil)
	s := seqWith(t, r, rec.step("one"), rec.step("two"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, "the run to park on its tail", func() bool { return sch.Pending() == 1 })

	if got := rn.CancelMatching("battery.inserted"); got != 0 {
		t.Errorf("CancelMatching on an unrelated topic returned %d, want 0", got)
	}
	if got := rn.Active(); got != 1 {
		t.Fatalf("Active() = %d after an unrelated topic, want 1", got)
	}
	if got := rn.CancelMatching("alarm.disarmed"); got != 1 {
		t.Errorf("CancelMatching on a topic under the glob returned %d, want 1", got)
	}
	if got := rn.Active(); got != 0 {
		t.Errorf("Active() = %d after the cancel, want 0", got)
	}
}

// TestCancelOnAlsoDropsQueuedFires: a queued trigger that started the moment
// its rule was cancelled would defeat the point of cancelling, since the
// backlog is exactly the runs the same trigger built up.
//
// The second half is the part with teeth. A backlog that survives a cancel
// does not run straight away, because the cancelled run never reaches the
// promotion path; it lies there until the next trigger's run ends and then
// comes out behind it. So the test fires once more at the end and insists
// that trigger is the only thing left to run.
func TestCancelOnAlsoDropsQueuedFires(t *testing.T) {
	gate, open := newGate()
	defer open()
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"alarm.triggered"}, Concurrency: "queue",
		CancelOn: []string{"alarm.disarmed"},
		Steps:    []rules.StepConfig{push("a", "1")},
	}, nil)
	s := seqWith(t, r, rec.gated("run", gate))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, "the first run to start", func() bool { return len(rec.list()) == 1 })
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})

	if got := rn.CancelMatching("alarm.disarmed"); got != 1 {
		t.Errorf("CancelMatching returned %d, want 1; queued triggers are not runs", got)
	}
	open()

	waitFor(t, "the cancelled run to leave the registry", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"run"}) {
		t.Fatalf("steps ran as %v, want only run; the backlog must not start on a cancel", got)
	}

	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, "the trigger after the cancel to run", func() bool { return len(rec.list()) >= 2 })
	// Long enough for two more promoted runs to appear, since neither has
	// anything left to wait for by now.
	time.Sleep(50 * time.Millisecond)
	if got := rec.list(); !equal(got, []string{"run", "run"}) {
		t.Errorf("steps ran as %v, want run, run; a cancel throws the backlog away rather than deferring it", got)
	}
}

// TestStopDropsQueuedFires: shutdown is the one moment where running the
// backlog would be worst, since the pool is about to go away underneath it. A
// stopped runner throws away whatever is queued behind the run it abandons,
// and starts nothing out of a backlog afterwards whatever put it there.
//
// Both halves are checked against the runner's own state, and the second one
// puts a trigger back by hand. That is deliberate. Stop also ends every live
// run, so no run is left that could reach the promotion path and show either
// guarantee from the outside: a test written purely through Fire and the
// recorder passes just as happily against a Stop that keeps the backlog and a
// promotion that will still hand one out, which is what the first version of
// this test did.
func TestStopDropsQueuedFires(t *testing.T) {
	gate, open := newGate()
	defer open()
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "queue",
		Steps: []rules.StepConfig{push("a", "1")},
	}, nil)
	s := seqWith(t, r, rec.gated("run", gate))

	rn := NewRunner(startedPool(t, 2, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	waitFor(t, "the first run to start", func() bool { return len(rec.list()) == 1 })
	rn.Fire(s, eventbus.Event{Topic: "x.y"})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	rn.mu.Lock()
	queuedBefore := len(rn.queued["r"])
	rn.mu.Unlock()
	if queuedBefore != 2 {
		t.Fatalf("%d trigger(s) queued behind the live run before Stop, want 2", queuedBefore)
	}

	rn.Stop()
	if got := rn.Active(); got != 0 {
		t.Errorf("Active() = %d after Stop, want 0", got)
	}

	rn.mu.Lock()
	queuedAfter := len(rn.queued["r"])
	rn.mu.Unlock()
	if queuedAfter != 0 {
		t.Errorf("Stop left %d trigger(s) in the backlog, want 0", queuedAfter)
	}

	// Nothing can add to a backlog after Stop, so the guard is exercised with
	// a trigger put there by hand: it has to hold for whatever is in the map,
	// however it got there, because what it is protecting against is a step
	// being submitted into a pool that is on its way out.
	rn.mu.Lock()
	rn.queued["r"] = []queuedFire{{seq: s, event: eventbus.Event{Topic: "x.y"}}}
	promoted := rn.startQueuedLocked("r")
	rn.mu.Unlock()
	if promoted != nil {
		t.Errorf("a stopped runner promoted %s out of the backlog, want nothing", promoted.id)
	}

	open()
	// The second worker is free the whole time, so a backlog that survived
	// Stop would have somewhere to run immediately.
	time.Sleep(50 * time.Millisecond)
	if got := rec.list(); !equal(got, []string{"run"}) {
		t.Errorf("steps ran as %v after Stop, want only the one already started", got)
	}
}

// TestCancelLandingMidStepDoesNotLetOneMoreStepFire pins the re-check advance
// makes under the lock immediately before Submit. The window it closes is the
// gap between a run being found live and its next step being queued, and a
// step's own when is what can hold a run inside that gap: the condition is
// evaluated outside the lock, on purpose, because it may read the shadow
// store. Here state() blocks until the cancel has landed, so the run is
// already ended by the time the when comes back true.
//
// Mutating the re-check in runStep into a no-op must fail this test.
func TestCancelLandingMidStepDoesNotLetOneMoreStepFire(t *testing.T) {
	reached := make(chan struct{})
	release, open := newGate()
	defer open()
	var once sync.Once

	lookup := func(hash, field string) string {
		once.Do(func() {
			close(reached)
			<-release
		})
		return "armed"
	}

	rec := &recorder{}
	twoRan := make(chan struct{})
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.disarmed"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", When: `state("alarm", "status") == "armed"`},
		},
	}, lookup)
	two := fnAction{fn: func() error { close(twoRan); return nil }}
	s := seqWith(t, r, rec.step("one"), two)

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})

	<-reached
	if got := rn.CancelMatching("alarm.disarmed"); got != 1 {
		t.Fatalf("CancelMatching returned %d while the run sat in its step when, want 1", got)
	}
	open()

	select {
	case <-twoRan:
		t.Fatal("step two was submitted after the run was cancelled; the re-check before Submit is not holding")
	case <-time.After(300 * time.Millisecond):
	}
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one", got)
	}
}

// TestRepeatRunsTheWholeSequenceCountTimes fires a single-step rule with
// repeat count 3 and checks that the step itself, not just the run, executed
// three times: repeat runs the whole sequence again, not a bookkeeping loop
// around a run that only ever does the work once.
func TestRepeatRunsTheWholeSequenceCountTimes(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Repeat: &rules.RepeatConfig{Count: 3, Every: "5ms"},
		Steps:  []rules.StepConfig{push("a", "1")},
	}, nil)
	s := seqWith(t, r, rec.step("one"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one", "one", "one"}) {
		t.Errorf("steps ran as %v, want one, one, one; repeat must run the whole sequence count times", got)
	}
}

// TestRepeatWaitsEveryBetweenIterations checks the gap goes through the
// scheduler the same way a step's own after does: the second pass must not
// start the instant the first one's last step finishes.
func TestRepeatWaitsEveryBetweenIterations(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Repeat: &rules.RepeatConfig{Count: 2, Every: "100ms"},
		Steps:  []rules.StepConfig{push("a", "1")},
	}, nil)
	s := seqWith(t, r, rec.step("one"))

	rn := NewRunner(startedPool(t, 1, 8), testSched(t), nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the first pass to run", func() bool { return len(rec.list()) >= 1 })
	// A wide margin short of the 100ms gap: the second pass must not start
	// the moment the first one's steps finish.
	time.Sleep(20 * time.Millisecond)
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v before the gap elapsed, want only one", got)
	}

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one", "one"}) {
		t.Errorf("steps ran as %v, want one, one; the second pass must still run once the gap elapses", got)
	}
}

// TestRepeatStopsWhenCancelledMidGap uses an hour-long every so the timer
// cannot possibly have fired by the time the cancel returns, the same
// technique TestRunnerStopCancelsAPendingTail and TestCancelOnDropsThePendingTail
// use for a step's own after. A repeat cancelled between passes must stop,
// not complete its remaining passes: the gap goes through the same cancelTail
// as a step's after, so a cancel reaching a run parked in the gap finds and
// drops the timer through the same path.
func TestRepeatStopsWhenCancelledMidGap(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.disarmed"},
		Repeat: &rules.RepeatConfig{Count: 3, Every: "1h"},
		Steps:  []rules.StepConfig{push("a", "1")},
	}, nil)
	s := seqWith(t, r, rec.step("one"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "alarm.triggered"})

	waitFor(t, "the run to park on its repeat gap", func() bool { return sch.Pending() == 1 })

	if got := rn.CancelMatching("alarm.disarmed"); got != 1 {
		t.Errorf("CancelMatching returned %d, want 1", got)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s) right after the cancel, want 0; a cancel mid-gap must drop the repeat timer", got)
	}
	if got := rn.Active(); got != 0 {
		t.Errorf("Active() = %d after the cancel, want 0", got)
	}

	// Long enough for a second pass to have landed had the cancel not taken
	// hold.
	time.Sleep(30 * time.Millisecond)
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one; a repeat cancelled mid-gap must not complete its remaining passes", got)
	}
}

// TestRepeatCountOneRunsOnePass pins the boundary the brief calls out by
// name: count 1 is legal and means exactly one pass, with no gap parked on
// the scheduler at all.
func TestRepeatCountOneRunsOnePass(t *testing.T) {
	rec := &recorder{}
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Repeat: &rules.RepeatConfig{Count: 1},
		Steps:  []rules.StepConfig{push("a", "1")},
	}, nil)
	s := seqWith(t, r, rec.step("one"))

	sch := testSched(t)
	rn := NewRunner(startedPool(t, 1, 8), sch, nil, nopLog{})
	rn.Fire(s, eventbus.Event{Topic: "x.y"})

	waitFor(t, "the run to end", func() bool { return rn.Active() == 0 })
	if got := rec.list(); !equal(got, []string{"one"}) {
		t.Errorf("steps ran as %v, want only one; count 1 means one pass", got)
	}
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s) after a count-1 repeat, want 0; there is no gap to wait for", got)
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
