package engine

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/eventbus"
)

type nopLog struct{}

func (nopLog) Printf(string, ...any) {}

type countingPusher struct {
	mu sync.Mutex
	n  int
}

func (p *countingPusher) LPush(key string, values ...any) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	return 1, nil
}

func (p *countingPusher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

func testSched(t *testing.T) *sched.Scheduler {
	t.Helper()
	s := sched.New()
	t.Cleanup(s.Stop)
	return s
}

func compileRules(t *testing.T, cfgs ...rules.RuleConfig) []*rules.Rule {
	t.Helper()
	rs, errs := rules.Compile(cfgs, func(string, string) string { return "" })
	if len(errs) != 0 {
		t.Fatalf("compile: %v", errs)
	}
	return rs
}

func horn() rules.StepConfig {
	return rules.StepConfig{Do: "redis", List: "scooter:horn", Push: "on"}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	if !waitUntil(cond) {
		t.Fatal("condition not met within 2s")
	}
}

// waitUntil polls cond and reports whether it came true inside the window, so
// a caller that has something specific to say about the failure can say it
// instead of leaving a bare timeout behind.
func waitUntil(cond func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestHandleDispatchesMatchingRule(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t, rules.RuleConfig{Name: "r", On: []string{"alarm.triggered"}, Steps: []rules.StepConfig{horn()}})
	en, errs := New(rs, pool, testSched(t), nil, p, nopLog{})
	if len(errs) != 0 {
		t.Fatalf("New: %v", errs)
	}

	en.Handle(eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, func() bool { return p.count() == 1 })
}

func TestHandleIgnoresNonMatchingTopic(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t, rules.RuleConfig{Name: "r", On: []string{"alarm.triggered"}, Steps: []rules.StepConfig{horn()}})
	en, _ := New(rs, pool, testSched(t), nil, p, nopLog{})

	en.Handle(eventbus.Event{Topic: "vehicle.unlocked"})
	time.Sleep(100 * time.Millisecond)
	if p.count() != 0 {
		t.Errorf("dispatched %d times for a non-matching topic", p.count())
	}
}

func TestCooldownSuppressesRepeatFires(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"motion.detected"}, Cooldown: "10s", Steps: []rules.StepConfig{horn()},
	})
	en, _ := New(rs, pool, testSched(t), nil, p, nopLog{})

	for i := 0; i < 5; i++ {
		en.Handle(eventbus.Event{Topic: "motion.detected"})
	}
	waitFor(t, func() bool { return p.count() >= 1 })
	time.Sleep(100 * time.Millisecond)
	if got := p.count(); got != 1 {
		t.Errorf("fired %d times inside a 10s cooldown, want 1", got)
	}
}

// TestCooldownExpiresAndFiresAgain guards against a cooldown that latches
// permanently, which would look identical to a correctly working cooldown in
// TestCooldownSuppressesRepeatFires alone. A rule that fires once, then never
// again, passes that test forever.
func TestCooldownExpiresAndFiresAgain(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"motion.detected"}, Cooldown: "50ms", Steps: []rules.StepConfig{horn()},
	})
	en, _ := New(rs, pool, testSched(t), nil, p, nopLog{})

	en.Handle(eventbus.Event{Topic: "motion.detected"})
	waitFor(t, func() bool { return p.count() >= 1 })

	time.Sleep(100 * time.Millisecond)

	en.Handle(eventbus.Event{Topic: "motion.detected"})
	waitFor(t, func() bool { return p.count() >= 2 })

	if got := p.count(); got != 2 {
		t.Errorf("fired %d times across an expired cooldown, want 2", got)
	}
}

// recordingPusher keeps what was pushed, so a multi-step rule can be checked
// for order and not just for count.
type recordingPusher struct {
	mu   sync.Mutex
	seen []string
}

func (p *recordingPusher) LPush(key string, values ...any) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range values {
		p.seen = append(p.seen, fmt.Sprintf("%s=%v", key, v))
	}
	return 1, nil
}

func (p *recordingPusher) list() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.seen...)
}

func TestHandleRunsEveryStepOfAMultiStepRule(t *testing.T) {
	p := &recordingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"alarm.triggered"},
		Steps: []rules.StepConfig{
			{Do: "redis", List: "scooter:horn", Push: "on"},
			{Do: "redis", List: "scooter:blinker", Push: "both"},
			{Do: "redis", List: "scooter:horn", Push: "off"},
		},
	})
	en, errs := New(rs, pool, testSched(t), nil, p, nopLog{})
	if len(errs) != 0 {
		t.Fatalf("New: %v", errs)
	}
	defer en.Stop()

	en.Handle(eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, func() bool { return len(p.list()) == 3 })

	want := []string{"scooter:horn=on", "scooter:blinker=both", "scooter:horn=off"}
	got := p.list()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("steps ran as %v, want %v", got, want)
		}
	}
}

func TestPatternsCoverEveryRuleTopic(t *testing.T) {
	rs := compileRules(t,
		rules.RuleConfig{Name: "a", On: []string{"alarm.triggered"}, Steps: []rules.StepConfig{horn()}},
		rules.RuleConfig{Name: "b", On: []string{"battery.*"}, Steps: []rules.StepConfig{horn()}},
	)
	en, _ := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), nil, &countingPusher{}, nopLog{})

	got := en.Patterns()
	want := map[string]bool{"ev:alarm.triggered": false, "ev:battery.*": false}
	for _, p := range got {
		if _, ok := want[p]; !ok {
			t.Errorf("unexpected pattern %q", p)
		}
		want[p] = true
	}
	for p, seen := range want {
		if !seen {
			t.Errorf("missing pattern %q (got %v)", p, got)
		}
	}
}

// TestPatternsIncludesCancelOnTopics guards the failure this feature is one
// mistake away from: a topic named only in cancel-on is never subscribed, the
// cancelling event never reaches Handle, and the cancel quietly never happens
// on a real scooter while every test that calls Handle directly still passes.
func TestPatternsIncludesCancelOnTopics(t *testing.T) {
	rs := compileRules(t, rules.RuleConfig{
		Name: "a", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.disarmed", "vehicle.*"},
		Steps: []rules.StepConfig{horn()},
	})
	en, _ := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), nil, &countingPusher{}, nopLog{})

	got := en.Patterns()
	want := map[string]bool{"ev:alarm.triggered": false, "ev:alarm.disarmed": false, "ev:vehicle.*": false}
	for _, p := range got {
		if _, ok := want[p]; !ok {
			t.Errorf("unexpected pattern %q", p)
		}
		want[p] = true
	}
	for p, seen := range want {
		if !seen {
			t.Errorf("missing pattern %q (got %v)", p, got)
		}
	}
}

// TestOneEventCanCancelOneRuleAndFireAnother is the shape the motivating rule
// pair has: one rule reacts to the alarm, another reacts to the rider
// disarming it, and the disarm has to do both jobs.
func TestOneEventCanCancelOneRuleAndFireAnother(t *testing.T) {
	p := &recordingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t,
		rules.RuleConfig{
			Name: "hazards", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.disarmed"},
			Steps: []rules.StepConfig{
				{Do: "redis", List: "scooter:blinker", Push: "both"},
				{Do: "redis", List: "scooter:blinker", Push: "off", After: "1h"},
			},
		},
		rules.RuleConfig{
			Name: "chirp", On: []string{"alarm.disarmed"},
			Steps: []rules.StepConfig{{Do: "redis", List: "scooter:horn", Push: "short"}},
		},
	)
	sch := testSched(t)
	en, errs := New(rs, pool, sch, nil, p, nopLog{})
	if len(errs) != 0 {
		t.Fatalf("New: %v", errs)
	}
	defer en.Stop()

	en.Handle(eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, func() bool { return sch.Pending() == 1 })

	en.Handle(eventbus.Event{Topic: "alarm.disarmed"})
	waitFor(t, func() bool { return len(p.list()) == 2 })

	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s) after the disarm, want 0; the cancelled tail must be dropped", got)
	}
	want := []string{"scooter:blinker=both", "scooter:horn=short"}
	if got := p.list(); got[0] != want[0] || got[1] != want[1] {
		t.Errorf("pushes were %v, want %v; one event cancels one rule and fires another", got, want)
	}
}

// TestCancelIsAppliedBeforeMatching pins the order of the two halves of
// Handle. A rule that names one topic in both on and cancel-on is the case
// that can tell them apart: cancelling first finds nothing live and lets the
// rule run, which under restart is what naming a topic in both means, while
// cancelling afterwards kills the run the same event has just started and the
// rule never gets past its first step. The second rule is there to show the
// cancelling event still fires everything else it matches.
func TestCancelIsAppliedBeforeMatching(t *testing.T) {
	p := &recordingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t,
		rules.RuleConfig{
			Name: "hazards", On: []string{"alarm.triggered"}, CancelOn: []string{"alarm.triggered"},
			Steps: []rules.StepConfig{
				{Do: "redis", List: "scooter:blinker", Push: "both"},
				{Do: "redis", List: "scooter:blinker", Push: "off", After: "1h"},
			},
		},
		rules.RuleConfig{
			Name: "chirp", On: []string{"alarm.triggered"},
			Steps: []rules.StepConfig{{Do: "redis", List: "scooter:horn", Push: "short"}},
		},
	)
	sch := testSched(t)
	en, errs := New(rs, pool, sch, nil, p, nopLog{})
	if len(errs) != 0 {
		t.Fatalf("New: %v", errs)
	}
	defer en.Stop()

	en.Handle(eventbus.Event{Topic: "alarm.triggered"})
	waitFor(t, func() bool { return len(p.list()) == 2 })

	// The tail is scheduled from the first step's completion, so it is worth
	// the same polling window as the pushes rather than a single read.
	if !waitUntil(func() bool { return sch.Pending() == 1 }) {
		t.Errorf("the rule's tail is not pending; a cancel applied after matching kills the run the same event started")
	}
	if got := p.list(); got[1] != "scooter:horn=short" {
		t.Errorf("pushes were %v, want the second rule to fire on the same event", got)
	}
}

// countPushed counts the pushes recorded against one list, so a test running
// two rules side by side on the same recordingPusher can tell them apart.
func countPushed(p *recordingPusher, list string) int {
	n := 0
	prefix := list + "="
	for _, s := range p.list() {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

// TestDebounceDispatchesOnceAfterTheSourceGoesQuiet fires a flapping source
// with no pause longer than the debounce window and checks nothing dispatches
// until the flapping actually stops.
func TestDebounceDispatchesOnceAfterTheSourceGoesQuiet(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	d := "50ms"
	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"motion.detected"}, Debounce: &d, Steps: []rules.StepConfig{horn()},
	})
	en, _ := New(rs, pool, testSched(t), nil, p, nopLog{})
	defer en.Stop()

	for i := 0; i < 5; i++ {
		en.Handle(eventbus.Event{Topic: "motion.detected"})
		time.Sleep(10 * time.Millisecond)
	}
	if got := p.count(); got != 0 {
		t.Errorf("dispatched %d times while the source was still flapping, want 0; debounce must hold the fire", got)
	}

	waitFor(t, func() bool { return p.count() >= 1 })
	time.Sleep(80 * time.Millisecond)
	if got := p.count(); got != 1 {
		t.Errorf("fired %d times once the source went quiet, want 1", got)
	}
}

// TestDebounceCarriesTheMostRecentEventNotTheFirst fires three events with
// different data.n values inside one debounce window. The step's own when
// only lets the push through for the last one, n == 3: if the runner carried
// the first event forward instead, as a reader might guess, the when would
// see n == 1 and the run would end without ever pushing.
func TestDebounceCarriesTheMostRecentEventNotTheFirst(t *testing.T) {
	p := &recordingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	d := "30ms"
	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"motion.detected"}, Debounce: &d,
		Steps: []rules.StepConfig{
			{Do: "redis", List: "scooter:horn", Push: "on", When: `data.n == 3`},
		},
	})
	en, _ := New(rs, pool, testSched(t), nil, p, nopLog{})
	defer en.Stop()

	for n := 1; n <= 3; n++ {
		en.Handle(eventbus.Event{Topic: "motion.detected", Data: map[string]any{"n": n}})
		time.Sleep(5 * time.Millisecond)
	}

	waitFor(t, func() bool { return len(p.list()) >= 1 })
	time.Sleep(60 * time.Millisecond)
	if got := len(p.list()); got != 1 {
		t.Errorf("dispatched %d times, want 1; the debounced dispatch should see the last event's data", got)
	}
}

// TestDebounceIsDistinctFromCooldown puts a cooldown rule and a debounce rule
// on the same flapping source side by side. Cooldown is leading edge: it
// fires the instant the first event lands and then ignores the rest.
// Debounce is the opposite, trailing edge: it fires nothing while the source
// keeps flapping and dispatches once, late, after the flapping stops. The two
// rules see the exact same events and must diverge.
func TestDebounceIsDistinctFromCooldown(t *testing.T) {
	p := &recordingPusher{}
	pool := action.NewPool(2, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	d := "60ms"
	rs := compileRules(t,
		rules.RuleConfig{
			Name: "cooldown-rule", On: []string{"motion.detected"}, Cooldown: "10s",
			Steps: []rules.StepConfig{{Do: "redis", List: "cooldown", Push: "fired"}},
		},
		rules.RuleConfig{
			Name: "debounce-rule", On: []string{"motion.detected"}, Debounce: &d,
			Steps: []rules.StepConfig{{Do: "redis", List: "debounce", Push: "fired"}},
		},
	)
	en, _ := New(rs, pool, testSched(t), nil, p, nopLog{})
	defer en.Stop()

	for i := 0; i < 4; i++ {
		en.Handle(eventbus.Event{Topic: "motion.detected"})
		time.Sleep(15 * time.Millisecond)
	}

	waitFor(t, func() bool { return countPushed(p, "cooldown") >= 1 })
	if got := countPushed(p, "cooldown"); got != 1 {
		t.Errorf("cooldown pushed %d times during the flap, want 1; cooldown fires on the leading edge", got)
	}
	if got := countPushed(p, "debounce"); got != 0 {
		t.Errorf("debounce pushed %d times during the flap, want 0; debounce must not fire on the leading edge", got)
	}

	waitFor(t, func() bool { return countPushed(p, "debounce") >= 1 })
	if got := countPushed(p, "debounce"); got != 1 {
		t.Errorf("debounce pushed %d times, want 1", got)
	}
}

// TestPendingDebounceIsDroppedOnShutdown mirrors
// TestRunnerStopCancelsAPendingTail: Pending is checked immediately after
// Stop, with no sleep in between, so the empty registry can only be Stop's
// own cancel and not the timer's self-removal on fire.
func TestPendingDebounceIsDroppedOnShutdown(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	d := "1h"
	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"motion.detected"}, Debounce: &d, Steps: []rules.StepConfig{horn()},
	})
	sch := testSched(t)
	en, _ := New(rs, pool, sch, nil, p, nopLog{})

	en.Handle(eventbus.Event{Topic: "motion.detected"})
	waitFor(t, func() bool { return sch.Pending() == 1 })

	en.Stop()
	if got := sch.Pending(); got != 0 {
		t.Errorf("scheduler has %d pending fire(s) right after Stop, want 0; shutdown must drop a pending debounce timer", got)
	}
	if got := p.count(); got != 0 {
		t.Errorf("dispatched %d times after Stop, want 0", got)
	}
}

// TestActiveForwardsTheRunnersCount pins the forwarding rather than
// re-testing the runner's own bookkeeping, which internal/seq already
// covers: a rule that parks its only step for an hour keeps its run live for
// the length of the test.
func TestActiveForwardsTheRunnersCount(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{{Do: "redis", List: "a", Push: "1", After: "1h"}},
	})
	en, errs := New(rs, pool, testSched(t), nil, p, nopLog{})
	if len(errs) != 0 {
		t.Fatalf("New: %v", errs)
	}
	defer en.Stop()

	if got := en.Active(); got != 0 {
		t.Fatalf("Active() = %d before any trigger, want 0", got)
	}

	en.Handle(eventbus.Event{Topic: "x.y"})
	waitFor(t, func() bool { return en.Active() == 1 })
}

// TestRefusedForwardsTheRunnersCount is Refused()'s first production caller:
// nothing read this counter before the extensions-hash publisher. maxQueued
// is 8 (internal/seq), so the first trigger starts the live run, the next 8
// fill the backlog, and the rest are refused.
func TestRefusedForwardsTheRunnersCount(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"}, Concurrency: "queue",
		Steps: []rules.StepConfig{{Do: "redis", List: "a", Push: "1", After: "1h"}},
	})
	en, errs := New(rs, pool, testSched(t), nil, p, nopLog{})
	if len(errs) != 0 {
		t.Fatalf("New: %v", errs)
	}
	defer en.Stop()

	if got := en.Refused(); got != 0 {
		t.Fatalf("Refused() = %d before any trigger, want 0", got)
	}

	for i := 0; i < 1+8+3; i++ {
		en.Handle(eventbus.Event{Topic: "x.y"})
	}

	if got := en.Refused(); got != 3 {
		t.Errorf("Refused() = %d, want 3", got)
	}
}

func TestNewReportsUnbuildableRuleWithoutLosingTheRest(t *testing.T) {
	rs := compileRules(t,
		rules.RuleConfig{Name: "good", On: []string{"x.y"}, Steps: []rules.StepConfig{horn()}},
		rules.RuleConfig{Name: "bad", On: []string{"x.y"}, Steps: []rules.StepConfig{{Do: "telepathy"}}},
	)
	en, errs := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), nil, &countingPusher{}, nopLog{})
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	if n := en.RuleCount(); n != 1 {
		t.Errorf("RuleCount() = %d, want 1; the good rule must survive", n)
	}
}

// TestNewErrorNamesRuleAndFile guards finding 3: with several rule files
// installed, an error that names neither the rule nor the file leaves the
// user unable to tell which of their files to fix. rules.Rule.Source is set
// directly here rather than via rules.Load, since Source is a plain field
// and Load's file-tagging behaviour already has its own tests in the rules
// package.
func TestNewErrorNamesRuleAndFile(t *testing.T) {
	rs := compileRules(t, rules.RuleConfig{
		Name: "bad", Source: "horn.toml", On: []string{"x.y"}, Steps: []rules.StepConfig{{Do: "telepathy"}},
	})
	_, errs := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), nil, &countingPusher{}, nopLog{})
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	if !strings.Contains(msg, "bad") {
		t.Errorf("error should name the rule, got %q", msg)
	}
	if !strings.Contains(msg, "horn.toml") {
		t.Errorf("error should name the file, got %q", msg)
	}
}

// TestNewErrorNamesRuleFileAndStepTogether checks the three parts in one
// message. The rule and the file are added here, the step index inside
// seq.Build, and each half has its own test, so a regression that drops one
// of them would leave both of those green.
func TestNewErrorNamesRuleFileAndStepTogether(t *testing.T) {
	rs := compileRules(t, rules.RuleConfig{
		Name: "hazards", Source: "hazards.toml", On: []string{"x.y"},
		Steps: []rules.StepConfig{horn(), {Do: "telepathy"}},
	})
	_, errs := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), nil, &countingPusher{}, nopLog{})
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{"hazards", "hazards.toml", "step 1"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %s, got %q", want, msg)
		}
	}
}

// TestDebounceStillRespectsCooldown puts both keys on one rule. Debounce holds
// the fire until the source goes quiet; the cooldown is then measured against
// that dispatch, not against the events that only restarted the window. A rule
// carrying both must therefore fire once per cooldown however often the source
// goes quiet, and a debounce that skips the cooldown check would fire on every
// pause instead.
func TestDebounceStillRespectsCooldown(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	d := "20ms"
	rs := compileRules(t, rules.RuleConfig{
		Name: "r", On: []string{"motion.detected"}, Debounce: &d, Cooldown: "10s",
		Steps: []rules.StepConfig{horn()},
	})
	en, errs := New(rs, pool, testSched(t), nil, p, nopLog{})
	if len(errs) != 0 {
		t.Fatalf("New: %v", errs)
	}
	defer en.Stop()

	en.Handle(eventbus.Event{Topic: "motion.detected"})
	waitFor(t, func() bool { return p.count() == 1 })

	// A second quiet window, well inside a ten second cooldown. The wait is
	// several times the debounce, so a dispatch that was going to happen has
	// happened by the time it is checked.
	en.Handle(eventbus.Event{Topic: "motion.detected"})
	time.Sleep(150 * time.Millisecond)
	if got := p.count(); got != 1 {
		t.Errorf("dispatched %d times inside a 10s cooldown, want 1; the debounced fire is what the cooldown counts", got)
	}
}
