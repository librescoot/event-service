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
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within 2s")
	}
}

func TestHandleDispatchesMatchingRule(t *testing.T) {
	p := &countingPusher{}
	pool := action.NewPool(1, 8, nopLog{})
	pool.Start()
	defer pool.Stop()

	rs := compileRules(t, rules.RuleConfig{Name: "r", On: []string{"alarm.triggered"}, Steps: []rules.StepConfig{horn()}})
	en, errs := New(rs, pool, testSched(t), p, nopLog{})
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
	en, _ := New(rs, pool, testSched(t), p, nopLog{})

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
	en, _ := New(rs, pool, testSched(t), p, nopLog{})

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
	en, _ := New(rs, pool, testSched(t), p, nopLog{})

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
	en, errs := New(rs, pool, testSched(t), p, nopLog{})
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
	en, _ := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), &countingPusher{}, nopLog{})

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

func TestNewReportsUnbuildableRuleWithoutLosingTheRest(t *testing.T) {
	rs := compileRules(t,
		rules.RuleConfig{Name: "good", On: []string{"x.y"}, Steps: []rules.StepConfig{horn()}},
		rules.RuleConfig{Name: "bad", On: []string{"x.y"}, Steps: []rules.StepConfig{{Do: "telepathy"}}},
	)
	en, errs := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), &countingPusher{}, nopLog{})
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
	_, errs := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), &countingPusher{}, nopLog{})
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
	_, errs := New(rs, action.NewPool(1, 1, nopLog{}), testSched(t), &countingPusher{}, nopLog{})
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
