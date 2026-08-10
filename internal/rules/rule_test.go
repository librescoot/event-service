package rules

import (
	"strings"
	"testing"
	"time"

	"github.com/librescoot/eventbus"
)

func noState(hash, field string) string { return "" }

func mustCompileOne(t *testing.T, c RuleConfig, lookup StateFunc) *Rule {
	t.Helper()
	rs, errs := Compile([]RuleConfig{c}, lookup)
	if len(errs) != 0 {
		t.Fatalf("compile: %v", errs)
	}
	if len(rs) != 1 {
		t.Fatalf("got %d rules, want 1", len(rs))
	}
	return rs[0]
}

func redisStep() StepConfig {
	return StepConfig{Do: "redis", List: "scooter:horn", Push: "on"}
}

func TestMatchesOnTopicAlone(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"alarm.triggered"}, Steps: []StepConfig{redisStep()},
	}, noState)

	ok, err := r.Matches(eventbus.Event{Topic: "alarm.triggered"})
	if err != nil || !ok {
		t.Errorf("want match, got ok=%v err=%v", ok, err)
	}
	ok, _ = r.Matches(eventbus.Event{Topic: "alarm.armed"})
	if ok {
		t.Error("must not match a different topic")
	}
}

func TestMatchesEvaluatesWhenAgainstTheEnvelope(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"alarm.triggered"},
		When:  `to == "level-2-triggered"`,
		Steps: []StepConfig{redisStep()},
	}, noState)

	ok, err := r.Matches(eventbus.Event{Topic: "alarm.triggered", To: "level-2-triggered"})
	if err != nil || !ok {
		t.Errorf("want match, got ok=%v err=%v", ok, err)
	}
	ok, _ = r.Matches(eventbus.Event{Topic: "alarm.triggered", To: "level-1-triggered"})
	if ok {
		t.Error("when clause should have rejected level-1")
	}
}

func TestMatchesReadsDataMap(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"battery.inserted"},
		When:  `data.slot == 1`,
		Steps: []StepConfig{redisStep()},
	}, noState)

	ok, _ := r.Matches(eventbus.Event{Topic: "battery.inserted", Data: map[string]any{"slot": 1}})
	if !ok {
		t.Error("want match on data.slot == 1")
	}
	ok, _ = r.Matches(eventbus.Event{Topic: "battery.inserted", Data: map[string]any{"slot": 0}})
	if ok {
		t.Error("slot 0 must not match")
	}
}

func TestMatchesExposesStateFunction(t *testing.T) {
	lookup := func(hash, field string) string {
		if hash == "alarm" && field == "status" {
			return "armed"
		}
		return ""
	}
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"motion.detected"},
		When:  `state("alarm", "status") == "armed"`,
		Steps: []StepConfig{redisStep()},
	}, lookup)

	ok, err := r.Matches(eventbus.Event{Topic: "motion.detected"})
	if err != nil || !ok {
		t.Errorf("want match, got ok=%v err=%v", ok, err)
	}

	disarmed := func(hash, field string) string {
		if hash == "alarm" && field == "status" {
			return "disarmed"
		}
		return ""
	}
	r2 := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"motion.detected"},
		When:  `state("alarm", "status") == "armed"`,
		Steps: []StepConfig{redisStep()},
	}, disarmed)
	ok, _ = r2.Matches(eventbus.Event{Topic: "motion.detected"})
	if ok {
		t.Error("state() returning disarmed must not match")
	}
}

func TestCompileRejectsMissingName(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		On: []string{"x.y"}, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "missing name") {
		t.Errorf("error should name the problem, got %v", errs[0])
	}
}

func TestCompileRejectsMissingOn(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "missing on") {
		t.Errorf("error should name the problem, got %v", errs[0])
	}
}

func TestCompileRejectsMissingSteps(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "missing step") {
		t.Errorf("error should name the problem, got %v", errs[0])
	}
}

func TestCompileParsesStepAfter(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []StepConfig{
			{Do: "redis", List: "l", Push: "p"},
			{Do: "redis", List: "l", Push: "p", After: "30s"},
		},
	}, noState)

	if r.Steps[0].After != 0 {
		t.Errorf("a step without after should parse to a zero duration, got %v", r.Steps[0].After)
	}
	if r.Steps[1].After != 30*time.Second {
		t.Errorf("after = %v, want 30s", r.Steps[1].After)
	}
}

func TestCompileRejectsNegativeAfterNamingTheStep(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"},
		Steps: []StepConfig{
			{Do: "redis", List: "l", Push: "p"},
			{Do: "redis", List: "l", Push: "p", After: "-1s"},
		},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	msg := errs[0].Error()
	if !strings.Contains(msg, "step 1") {
		t.Errorf("error should name the step index, got %q", msg)
	}
}

func TestCompileKeepsEveryStepInOrder(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []StepConfig{
			{Do: "redis", List: "l", Push: "first"},
			{Do: "redis", List: "l", Push: "second"},
			{Do: "exec", Command: "true"},
		},
	}, noState)

	if len(r.Steps) != 3 {
		t.Fatalf("got %d steps, want 3", len(r.Steps))
	}
	if r.Steps[0].Config.Push != "first" || r.Steps[1].Config.Push != "second" {
		t.Errorf("steps came out in the wrong order: %v", r.Steps)
	}
	if r.Steps[2].Config.Command != "true" {
		t.Errorf("step 2 lost its command: %v", r.Steps[2].Config)
	}
}

func TestCompileCompilesStepWhen(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []StepConfig{
			{Do: "redis", List: "l", Push: "p"},
			{Do: "redis", List: "l", Push: "p", When: `state("alarm", "status") == "armed"`},
		},
	}, noState)

	if r.Steps[0].When != nil {
		t.Error("a step without when should have a nil program")
	}
	if r.Steps[1].When == nil {
		t.Fatal("a step with when should have a compiled program")
	}

	ok, err := r.EvalWhen(r.Steps[1].When, eventbus.Event{Topic: "x.y"})
	if err != nil {
		t.Fatalf("EvalWhen: %v", err)
	}
	if ok {
		t.Error("noState returns empty, so the condition should be false")
	}
}

func TestCompileRejectsBadStepWhenNamingRuleFileAndIndex(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", Source: "horn.toml", On: []string{"x.y"},
		Steps: []StepConfig{
			{Do: "redis", List: "l", Push: "p"},
			{Do: "redis", List: "l", Push: "p", When: "this is not ( valid"},
		},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	msg := errs[0].Error()
	for _, want := range []string{`"r"`, "horn.toml", "step 1"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %s, got %q", want, msg)
		}
	}
}

func TestCompileRejectsBadWhenExpression(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, When: "this is not ( valid", Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
}

func TestCompileSkipsDisabledRules(t *testing.T) {
	off := false
	rs, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, Enabled: &off, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(rs) != 0 {
		t.Errorf("a disabled rule must not compile into a live rule")
	}
}

func TestCompileDefaultsToEnabled(t *testing.T) {
	rs, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 0 || len(rs) != 1 {
		t.Fatalf("omitting enabled should mean enabled, got %d rules %v", len(rs), errs)
	}
}

func TestCompileParsesDurations(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"}, Cooldown: "60s", Steps: []StepConfig{redisStep()},
	}, noState)
	if r.Cooldown.Seconds() != 60 {
		t.Errorf("cooldown = %v, want 60s", r.Cooldown)
	}
}

func TestCompileRejectsConcurrency(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, Concurrency: "restart", Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "not supported yet") {
		t.Errorf("error should name the limitation, got %v", errs[0])
	}
}

func TestCompileRejectsCancelOn(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, CancelOn: []string{"alarm.disarmed"}, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "not supported yet") {
		t.Errorf("error should name the limitation, got %v", errs[0])
	}
}

func TestCompileRejectsRepeat(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, Repeat: map[string]any{"every": "5s"}, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "not supported yet") {
		t.Errorf("error should name the limitation, got %v", errs[0])
	}
}

func TestCompileRejectsDebounce(t *testing.T) {
	d := "500ms"
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, Debounce: &d, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "not supported yet") {
		t.Errorf("error should name the limitation, got %v", errs[0])
	}
}

// The three tests below guard finding 8: an empty spelling of an unsupported
// feature (an empty list, an empty table, a zero duration) must be rejected
// exactly like its non-empty form. All three parse to the same Go zero value
// as an omitted key, so a check that looks at truthiness instead of presence
// would let them through unnoticed.

func TestCompileRejectsEmptyCancelOn(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, CancelOn: []string{}, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "not supported yet") {
		t.Errorf("error should name the limitation, got %v", errs[0])
	}
}

func TestCompileRejectsEmptyRepeat(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, Repeat: map[string]any{}, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "not supported yet") {
		t.Errorf("error should name the limitation, got %v", errs[0])
	}
}

func TestCompileRejectsZeroDebounce(t *testing.T) {
	d := "0s"
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"}, Debounce: &d, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "not supported yet") {
		t.Errorf("error should name the limitation, got %v", errs[0])
	}
}
