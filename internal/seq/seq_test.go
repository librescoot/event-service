package seq

import (
	"strings"
	"testing"

	"github.com/librescoot/event-service/internal/rules"
)

type nopPusher struct{}

func (nopPusher) LPush(string, ...any) (int64, error) { return 1, nil }

func compileRule(t *testing.T, c rules.RuleConfig, lookup rules.StateFunc) *rules.Rule {
	t.Helper()
	if lookup == nil {
		lookup = func(string, string) string { return "" }
	}
	rs, errs := rules.Compile([]rules.RuleConfig{c}, lookup)
	if len(errs) != 0 {
		t.Fatalf("compile: %v", errs)
	}
	if len(rs) != 1 {
		t.Fatalf("got %d rules, want 1", len(rs))
	}
	return rs[0]
}

func push(list, value string) rules.StepConfig {
	return rules.StepConfig{Do: "redis", List: list, Push: value}
}

func TestBuildCompilesEveryStep(t *testing.T) {
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), push("b", "2"), push("c", "3")},
	}, nil)

	s, err := Build(r, nopPusher{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(s.Steps) != 3 {
		t.Fatalf("got %d compiled steps, want 3", len(s.Steps))
	}
	if s.Rule != r {
		t.Error("sequence should carry the rule it was built from")
	}
	for i, cs := range s.Steps {
		if cs.Action == nil {
			t.Errorf("step %d has no action", i)
		}
	}
}

func TestBuildCarriesTheStepWhenProgram(t *testing.T) {
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{
			push("a", "1"),
			{Do: "redis", List: "b", Push: "2", When: `to == "moving"`},
		},
	}, nil)

	s, err := Build(r, nopPusher{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if s.Steps[0].When != nil {
		t.Error("a step without when should compile to a nil program")
	}
	if s.Steps[1].When == nil {
		t.Error("a step with when should carry its compiled program")
	}
}

func TestBuildRejectsAnUnknownActionInAnyStep(t *testing.T) {
	r := compileRule(t, rules.RuleConfig{
		Name: "r", On: []string{"x.y"},
		Steps: []rules.StepConfig{push("a", "1"), {Do: "telepathy"}},
	}, nil)

	_, err := Build(r, nopPusher{})
	if err == nil {
		t.Fatal("a bad action in a later step must fail the build")
	}
	if !strings.Contains(err.Error(), "step 1") {
		t.Errorf("error should name the step index, got %q", err)
	}
	if !strings.Contains(err.Error(), "telepathy") {
		t.Errorf("error should name the unknown action, got %q", err)
	}
}
