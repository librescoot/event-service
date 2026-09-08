package rules_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/librescoot/event-service/internal/rules"
)

func TestREADMEExamplesCompile(t *testing.T) {
	body, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}

	var examples strings.Builder
	inRule := false
	for _, line := range strings.Split(string(body), "\n") {
		if line == "    [[rule]]" {
			inRule = true
		}
		if inRule {
			switch {
			case strings.HasPrefix(line, "    "):
				examples.WriteString(strings.TrimPrefix(line, "    "))
				examples.WriteByte('\n')
			case line == "":
				examples.WriteByte('\n')
			default:
				inRule = false
			}
		}
	}
	if examples.Len() == 0 {
		t.Fatal("no indented TOML rule examples found in README")
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "readme.toml"), []byte(examples.String()), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, errs := rules.Load(dir)
	if len(errs) != 0 {
		t.Fatalf("load README examples: %v", errs)
	}
	compiled, errs := rules.Compile(cfg.Rules, func(string, string) string { return "" })
	if len(errs) != 0 {
		t.Fatalf("compile README examples: %v", errs)
	}
	if len(compiled) == 0 || len(compiled) != len(cfg.Rules) {
		t.Fatalf("compiled %d of %d README rules", len(compiled), len(cfg.Rules))
	}
}
