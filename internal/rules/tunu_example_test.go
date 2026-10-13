package rules_test

import (
	"testing"

	"github.com/librescoot/event-service/internal/rules"
)

func TestTunuBatteryCurrentExampleCompiles(t *testing.T) {
	cfg, errs := rules.Load("../../examples/tunu-battery-current")
	if len(errs) != 0 {
		t.Fatalf("load example: %v", errs)
	}
	compiled, errs := rules.Compile(cfg.Rules, func(string, string) string { return "" })
	if len(errs) != 0 {
		t.Fatalf("compile example: %v", errs)
	}
	if len(compiled) != 2 {
		t.Fatalf("compiled %d rules, want 2", len(compiled))
	}
}
