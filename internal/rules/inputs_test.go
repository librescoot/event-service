package rules

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/librescoot/event-service/internal/inputs"
)

const inputRuleTOML = `
[[rule]]
name = "sensors"
on = ["input.sensor"]
[[rule.input]]
hash = "sensor:status"
fields = ["temperature", "ready"]
topic = "input.sensor"
max-bytes = 1024
[[rule.input]]
hash = "sensor:limits"
fields = ["maximum"]
[[rule.input]]
channel = "sensor:raw"
topic = "input.raw"
format = "string"
[[rule.input]]
channel = "sensor:json"
topic = "input.json"
format = "json"
max-bytes = 65536
[[rule.step]]
do = "redis"
list = "test:commands"
push = "test"
after = "1s"
`

func TestInputsLoadAndCompilePreserveDefinitions(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "inputs.toml", inputRuleTOML)
	cfg, errs := Load(dir)
	if len(errs) != 0 || len(cfg.Rules) != 1 {
		t.Fatalf("load: %+v %v", cfg, errs)
	}
	want := []inputs.Config{
		{Hash: "sensor:status", Fields: []string{"temperature", "ready"}, Topic: "input.sensor", MaxBytes: 1024},
		{Hash: "sensor:limits", Fields: []string{"maximum"}},
		{Channel: "sensor:raw", Topic: "input.raw", Format: "string"},
		{Channel: "sensor:json", Topic: "input.json", Format: "json", MaxBytes: 65536},
	}
	if !reflect.DeepEqual(cfg.Rules[0].Inputs, want) {
		t.Fatalf("parsed inputs: %+v", cfg.Rules[0].Inputs)
	}
	compiled, errs := Compile(cfg.Rules, nil)
	if len(errs) != 0 || len(compiled) != 1 {
		t.Fatalf("compile: %v %v", compiled, errs)
	}
	if !reflect.DeepEqual(compiled[0].Inputs, want) || compiled[0].Source != "inputs.toml" {
		t.Fatalf("compiled rule: %+v", compiled[0])
	}
}

func TestInputsLoadRejectsUnknownKeys(t *testing.T) {
	for _, key := range []string{"max-byte", "field", "unknown", "nested.typo"} {
		t.Run(key, func(t *testing.T) {
			dir := t.TempDir()
			writeTOML(t, dir, "bad.toml", strings.Replace(inputRuleTOML, "[[rule.input]]", "[[rule.input]]\n"+key+" = 1", 1))
			writeTOML(t, dir, "good.toml", inputRuleTOML)
			cfg, errs := Load(dir)
			if len(errs) != 1 || !strings.Contains(errs[0].Error(), "bad.toml") || !strings.Contains(errs[0].Error(), key) {
				t.Fatalf("errors: %v", errs)
			}
			if len(cfg.Rules) != 1 || cfg.Rules[0].Source != "good.toml" {
				t.Fatalf("unknown key did not reject whole file: %+v", cfg.Rules)
			}
		})
	}
}

func inputRuleConfig() RuleConfig {
	return RuleConfig{Name: "sensor", Source: "inputs.toml", On: []string{"input.sensor"}, Steps: []StepConfig{{Do: "redis", List: "test:commands", Push: "test", After: "1s"}}}
}

func TestInputsCompileLimitsAndErrors(t *testing.T) {
	base := inputRuleConfig()
	for i := 0; i < inputs.MaxPerRule; i++ {
		base.Inputs = append(base.Inputs, inputs.Config{Channel: fmt.Sprintf("events:%d", i), Topic: "input.sensor"})
	}
	if _, err := ValidateDefinition(base, nil); err != nil {
		t.Fatalf("16 inputs: %v", err)
	}
	bad := base
	bad.Inputs = append(append([]inputs.Config(nil), base.Inputs...), base.Inputs[0])
	if _, err := ValidateDefinition(bad, nil); err == nil {
		t.Fatal("per-rule count must include duplicates")
	}
	cases := [][]inputs.Config{
		{{Hash: "ev:private", Fields: []string{"value"}}},
		{{Hash: "sensor", Fields: []string{"value"}}, {Channel: "sensor", Topic: "input.sensor"}},
	}
	fields := make([]string, 64)
	for i := range fields {
		fields[i] = fmt.Sprintf("field-%d", i)
	}
	cases = append(cases,
		[]inputs.Config{{Hash: "sensor", Fields: fields}, {Hash: "sensor", Fields: []string{"extra"}}},
		[]inputs.Config{{Hash: "sensor", Fields: fields[:17], MaxBytes: inputs.MaxBytes}},
	)
	for i, cs := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			invalid := inputRuleConfig()
			invalid.Inputs = cs
			good := inputRuleConfig()
			good.Name = "good"
			out, errs := Compile([]RuleConfig{invalid, good}, nil)
			if len(out) != 1 || out[0].Name != "good" || len(errs) != 1 {
				t.Fatalf("compile: %v %v", out, errs)
			}
			if !strings.Contains(errs[0].Error(), "sensor") || !strings.Contains(errs[0].Error(), "inputs.toml") {
				t.Fatalf("missing rule/source diagnostic: %v", errs)
			}
		})
	}
}

func TestInputsDisabledCompileSemantics(t *testing.T) {
	disabled := false
	c := inputRuleConfig()
	c.Enabled = &disabled
	c.Inputs = []inputs.Config{{Hash: "sensor", Fields: []string{"value"}}}
	out, errs := Compile([]RuleConfig{c}, nil)
	if len(out) != 0 || len(errs) != 0 {
		t.Fatalf("disabled ordinary compile: %v %v", out, errs)
	}
	out, errs = CompileForRuntime([]RuleConfig{c}, nil)
	if len(out) != 1 || len(errs) != 0 || !out[0].ReplayOnly || !reflect.DeepEqual(out[0].Inputs, c.Inputs) {
		t.Fatalf("disabled replay: %v %v", out, errs)
	}
	c.Inputs[0].Hash = "ev:forbidden"
	out, errs = Compile([]RuleConfig{c}, nil)
	if len(out) != 0 || len(errs) != 0 {
		t.Fatalf("disabled invalid ordinary compile: %v %v", out, errs)
	}
	if _, err := ValidateDefinition(c, nil); err == nil {
		t.Fatal("definition validation skipped disabled inputs")
	}
	out, errs = CompileForRuntime([]RuleConfig{c}, nil)
	if len(out) != 0 || len(errs) != 1 {
		t.Fatalf("invalid disabled replay accepted: %v %v", out, errs)
	}
}

func TestInputsDoNotChangeLegacyStepFingerprints(t *testing.T) {
	c := inputRuleConfig()
	c.Steps = append(c.Steps, StepConfig{Do: "exec", Command: "/bin/true", Timeout: "2s", After: "3s", When: `to == "ready"`})
	before, err := ValidateDefinition(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Inputs = []inputs.Config{{Hash: "sensor", Fields: []string{"value"}, Topic: "input.sensor"}, {Channel: "events", Topic: "input.raw", Format: "json"}}
	after, err := ValidateDefinition(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range before.Steps {
		if before.Steps[i].Fingerprint == "" || before.Steps[i].Fingerprint != after.Steps[i].Fingerprint {
			t.Fatalf("step %d fingerprint changed: %q -> %q", i, before.Steps[i].Fingerprint, after.Steps[i].Fingerprint)
		}
	}
}
