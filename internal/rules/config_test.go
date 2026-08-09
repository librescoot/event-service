package rules

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTOML(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func TestLoadReadsEveryTOMLFile(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "a.toml", `
[[rule]]
name = "one"
on   = ["alarm.triggered"]
  [[rule.step]]
  do = "redis"
  list = "scooter:horn"
  push = "on"
`)
	writeTOML(t, dir, "b.toml", `
[[rule]]
name = "two"
on   = ["vehicle.unlocked"]
  [[rule.step]]
  do = "exec"
  command = "/bin/true"
`)

	cfg, errs := Load(dir)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(cfg.Rules) != 2 {
		t.Fatalf("got %d rules, want 2", len(cfg.Rules))
	}
}

func TestLoadIgnoresNonTOMLFiles(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "a.toml", "[[rule]]\nname=\"one\"\non=[\"x.y\"]\n[[rule.step]]\ndo=\"redis\"\nlist=\"l\"\npush=\"p\"\n")
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not toml"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, errs := Load(dir)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(cfg.Rules) != 1 {
		t.Fatalf("got %d rules, want 1", len(cfg.Rules))
	}
}

// One malformed file must not take the others down with it: a user editing
// one rule should not silently lose every other rule they have.
func TestLoadReportsBadFileButKeepsGoodOnes(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "good.toml", "[[rule]]\nname=\"ok\"\non=[\"x.y\"]\n[[rule.step]]\ndo=\"redis\"\nlist=\"l\"\npush=\"p\"\n")
	writeTOML(t, dir, "bad.toml", "[[rule]\nthis is not toml")

	cfg, errs := Load(dir)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	if len(cfg.Rules) != 1 || cfg.Rules[0].Name != "ok" {
		t.Fatalf("good rule was lost: %+v", cfg.Rules)
	}
}

func TestLoadMissingDirectoryIsNotAnError(t *testing.T) {
	cfg, errs := Load(filepath.Join(t.TempDir(), "does-not-exist"))
	if len(errs) != 0 {
		t.Fatalf("a missing extensions directory is the normal case, got %v", errs)
	}
	if len(cfg.Rules) != 0 {
		t.Fatalf("got %d rules, want 0", len(cfg.Rules))
	}
}

// Parsed-but-unused fields (concurrency, cancel-on, repeat) must round-trip
// into the struct so a later task can read and validate them.
func TestLoadParsesUnusedFields(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "unused.toml", `
[[rule]]
name = "with-unused"
on   = ["x.y"]
concurrency = "restart"
cancel-on = ["other.event"]
repeat = { interval = "5s" }
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)

	cfg, errs := Load(dir)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(cfg.Rules) != 1 {
		t.Fatalf("got %d rules, want 1", len(cfg.Rules))
	}

	rule := cfg.Rules[0]
	if rule.Concurrency != "restart" {
		t.Errorf("Concurrency = %q, want \"restart\"", rule.Concurrency)
	}
	if len(rule.CancelOn) != 1 || rule.CancelOn[0] != "other.event" {
		t.Errorf("CancelOn = %v, want [\"other.event\"]", rule.CancelOn)
	}
	if rule.Repeat == nil || rule.Repeat["interval"] != "5s" {
		t.Errorf("Repeat = %v, want {interval: 5s}", rule.Repeat)
	}
}
