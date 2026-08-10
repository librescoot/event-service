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

// TestCompileRejectsADuplicateRuleNameNamingBothFiles covers the mistake this
// check exists for: copying hazards.toml to hazards-v2.toml to try a variant
// and leaving the name inside as it was. Both definitions would then share a
// concurrency policy, a cancel-on list and a queue, and one would cancel the
// other's runs on a topic it never mentions. The error has to name both files
// or the user has one name and nowhere to look.
func TestCompileRejectsADuplicateRuleNameNamingBothFiles(t *testing.T) {
	rs, errs := Compile([]RuleConfig{
		{Name: "hazards", Source: "hazards.toml", On: []string{"alarm.triggered"}, Steps: []StepConfig{redisStep()}},
		{Name: "hazards", Source: "hazards-v2.toml", On: []string{"alarm.triggered"}, Steps: []StepConfig{redisStep()}},
		{Name: "chirp", Source: "chirp.toml", On: []string{"alarm.disarmed"}, Steps: []StepConfig{redisStep()}},
	}, noState)

	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{`"hazards"`, "hazards.toml", "hazards-v2.toml"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %s, got %q", want, msg)
		}
	}

	if len(rs) != 2 {
		t.Fatalf("got %d rules, want 2; the first definition and the unrelated rule must still load", len(rs))
	}
	if rs[0].Source != "hazards.toml" || rs[1].Name != "chirp" {
		t.Errorf("loaded %q from %s and %q, want hazards from hazards.toml and chirp", rs[0].Name, rs[0].Source, rs[1].Name)
	}
}

// TestCompileLetsADisabledRuleShareAName: a disabled rule never reaches the
// runner, so it cannot collide with anything. Rejecting it would make
// enabled = false useless for the one thing it is most obviously for, keeping
// the old copy of a rule around while a new one is tried.
func TestCompileLetsADisabledRuleShareAName(t *testing.T) {
	off := false
	rs, errs := Compile([]RuleConfig{
		{Name: "hazards", Source: "hazards.toml", Enabled: &off, On: []string{"alarm.triggered"}, Steps: []StepConfig{redisStep()}},
		{Name: "hazards", Source: "hazards-v2.toml", On: []string{"alarm.triggered"}, Steps: []StepConfig{redisStep()}},
	}, noState)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(rs) != 1 || rs[0].Source != "hazards-v2.toml" {
		t.Errorf("got %d rules, want only the enabled one from hazards-v2.toml", len(rs))
	}
}

// TestCompileFreesTheNameOfARuleThatFailedToLoad: a rule that does not load
// holds no name, or a typo in one file would take the name away from a
// working rule in another.
func TestCompileFreesTheNameOfARuleThatFailedToLoad(t *testing.T) {
	rs, errs := Compile([]RuleConfig{
		{Name: "hazards", Source: "broken.toml", On: []string{"alarm.triggered"}, Steps: []StepConfig{{Do: "redis", List: "l", Push: "p", When: "this is not ( valid"}}},
		{Name: "hazards", Source: "hazards.toml", On: []string{"alarm.triggered"}, Steps: []StepConfig{redisStep()}},
	}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	if len(rs) != 1 || rs[0].Source != "hazards.toml" {
		t.Errorf("got %d rules, want the working one from hazards.toml", len(rs))
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

func TestCompileDefaultsToTheRestartPolicy(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"}, Steps: []StepConfig{redisStep()},
	}, noState)
	if r.Concurrency != PolicyRestart {
		t.Errorf("concurrency = %q with the key omitted, want %q", r.Concurrency, PolicyRestart)
	}
}

func TestCompileParsesEveryPolicy(t *testing.T) {
	for _, want := range []Policy{PolicyRestart, PolicyDrop, PolicyQueue} {
		r := mustCompileOne(t, RuleConfig{
			Name: "r", On: []string{"x.y"}, Concurrency: string(want), Steps: []StepConfig{redisStep()},
		}, noState)
		if r.Concurrency != want {
			t.Errorf("concurrency = %q, want %q", r.Concurrency, want)
		}
	}
}

// TestUnknownPolicyIsRejectedNamingTheValidValues covers the typo case, which
// is the one a user actually hits. A message that says only "invalid" leaves
// them guessing at the spelling, so the three accepted words have to be in it,
// along with the rule and the file to find them in.
func TestUnknownPolicyIsRejectedNamingTheValidValues(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", Source: "horn.toml", On: []string{"x.y"},
		Concurrency: "restrat", Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{`"r"`, "horn.toml", "restart", "drop", "queue"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %s, got %q", want, msg)
		}
	}
}

func TestCompileKeepsCancelOnTopics(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"}, CancelOn: []string{"alarm.disarmed", "vehicle.*"},
		Steps: []StepConfig{redisStep()},
	}, noState)
	if len(r.CancelOn) != 2 || r.CancelOn[0] != "alarm.disarmed" || r.CancelOn[1] != "vehicle.*" {
		t.Errorf("cancel-on = %v, want alarm.disarmed and vehicle.*", r.CancelOn)
	}
}

func TestCancelledByMatchesExactTopicsAndGlobs(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"}, CancelOn: []string{"alarm.disarmed", "vehicle.*"},
		Steps: []StepConfig{redisStep()},
	}, noState)
	for topic, want := range map[string]bool{
		"alarm.disarmed":  true,
		"vehicle.locked":  true,
		"alarm.triggered": false,
		"vehicle":         false,
	} {
		if got := r.CancelledBy(topic); got != want {
			t.Errorf("CancelledBy(%q) = %v, want %v", topic, got, want)
		}
	}

	plain := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"}, Steps: []StepConfig{redisStep()},
	}, noState)
	if plain.CancelledBy("alarm.disarmed") {
		t.Error("a rule with no cancel-on must not be cancelled by anything")
	}
}

func TestCompileParsesRepeat(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"},
		Repeat: &RepeatConfig{Count: 3, Every: "700ms"},
		Steps:  []StepConfig{redisStep()},
	}, noState)
	if r.Repeat == nil {
		t.Fatal("Repeat should be set")
	}
	if r.Repeat.Count != 3 {
		t.Errorf("Repeat.Count = %d, want 3", r.Repeat.Count)
	}
	if r.Repeat.Every != 700*time.Millisecond {
		t.Errorf("Repeat.Every = %v, want 700ms", r.Repeat.Every)
	}
}

// TestCompileAllowsRepeatCountOfOneWithoutEvery is the boundary the brief
// calls out by name: count = 1 is a legal repeat, meaning one pass, and since
// there is no gap to wait between a single pass and itself, every is not
// required or checked.
func TestCompileAllowsRepeatCountOfOneWithoutEvery(t *testing.T) {
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"},
		Repeat: &RepeatConfig{Count: 1},
		Steps:  []StepConfig{redisStep()},
	}, noState)
	if r.Repeat == nil || r.Repeat.Count != 1 {
		t.Fatalf("Repeat = %+v, want Count 1", r.Repeat)
	}
}

// TestCompileRejectsRepeatCountBelowOne also covers repeat = {}: an empty
// table decodes to a non-nil RepeatConfig with Count at its Go zero value, 0,
// which is exactly the value this check rejects. A rule author who wrote the
// key at all meant to use the feature, so an empty table gets the same
// rejection as any other invalid count rather than being read as "no repeat".
func TestCompileRejectsRepeatCountBelowOne(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", Source: "horn.toml", On: []string{"x.y"},
		Repeat: &RepeatConfig{},
		Steps:  []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{`"r"`, "horn.toml", "count"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %s, got %q", want, msg)
		}
	}
}

func TestCompileRejectsRepeatWithZeroEveryWhenCountAboveOne(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"},
		Repeat: &RepeatConfig{Count: 3},
		Steps:  []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	if !strings.Contains(errs[0].Error(), "every") {
		t.Errorf("error should name every, got %v", errs[0])
	}
}

func TestCompileRejectsRepeatWithNegativeEveryWhenCountAboveOne(t *testing.T) {
	_, errs := Compile([]RuleConfig{{
		Name: "r", On: []string{"x.y"},
		Repeat: &RepeatConfig{Count: 3, Every: "-1s"},
		Steps:  []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	if !strings.Contains(errs[0].Error(), "every") {
		t.Errorf("error should name every, got %v", errs[0])
	}
}

func TestCompileParsesDebounce(t *testing.T) {
	d := "300ms"
	r := mustCompileOne(t, RuleConfig{
		Name: "r", On: []string{"x.y"}, Debounce: &d, Steps: []StepConfig{redisStep()},
	}, noState)
	if r.Debounce != 300*time.Millisecond {
		t.Errorf("Debounce = %v, want 300ms", r.Debounce)
	}
}

// TestCompileRejectsZeroDebounceNamingTheRule is why Debounce stays a pointer
// even now that the feature works: a duration has no nil of its own, so
// "debounce was never written" and "debounce was written as 0s" both parse to
// the same zero time.Duration and only the pointer tells them apart. Only the
// latter is a rule author believing a no-op holds a fire, so it is rejected
// rather than silently treated the same as an omitted key.
func TestCompileRejectsZeroDebounceNamingTheRule(t *testing.T) {
	d := "0s"
	_, errs := Compile([]RuleConfig{{
		Name: "r", Source: "horn.toml", On: []string{"x.y"}, Debounce: &d, Steps: []StepConfig{redisStep()},
	}}, noState)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{`"r"`, "horn.toml"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %s, got %q", want, msg)
		}
	}
}
