package rules

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/librescoot/eventbus"
)

func TestPreviewDoesNotExecuteOrRequireHardware(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "executed")
	script := filepath.Join(dir, "script")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntouch "+marker+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	no := false
	c := RuleConfig{Name: "preview", On: []string{"test.event"}, Enabled: &no,
		When: "state('vehicle', 'state') == 'parked'", Repeat: &RepeatConfig{Count: 1000000, Every: "1s"},
		Steps: []StepConfig{
			{Do: "exec", Command: script},
			{Do: "redis", List: "test:preview", Push: "never"},
			{Do: "can", Iface: "does-not-exist", ID: "123", Data: "01", After: "1s", When: "to == 'go'"},
		}}
	lookup := func(h, f string) string {
		if h == "vehicle" && f == "state" {
			return "parked"
		}
		return ""
	}
	out, err := Preview(c, eventbus.Event{Topic: "test.event", To: "stop"}, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if out.Enabled || !out.Matched || out.RepeatCount != 1000000 || len(out.Steps) != 3 || out.Steps[2].Condition || !out.Steps[2].Durable {
		t.Fatalf("preview: %+v", out)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("dry run executed a script: %v", err)
	}
	if !strings.Contains(out.Note, "no actions") {
		t.Fatal("dry-run qualification missing")
	}
}

func TestPreviewReportsEvaluationAndValidationErrors(t *testing.T) {
	c := RuleConfig{Name: "test", On: []string{"test"}, When: "int(to) > 3", Steps: []StepConfig{{Do: "redis", List: "test:list", Push: "x"}}}
	out, err := Preview(c, eventbus.Event{Topic: "test", To: "not-a-number"}, func(string, string) string { return "" })
	if err != nil || out.Error == "" || out.Matched {
		t.Fatalf("evaluation: %+v %v", out, err)
	}
	c.Steps[0] = StepConfig{Do: "can", Iface: "can0", ID: "20000000"}
	if _, err := Preview(c, eventbus.Event{Topic: "test"}, nil); err == nil {
		t.Fatal("invalid CAN frame accepted by preview")
	}
}
