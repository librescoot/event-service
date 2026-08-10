package rules

import (
	"os"
	"path/filepath"
	"strings"
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

// TestLoadRejectsUnknownKeys guards finding 2: toml.DecodeFile does not
// reject unknown keys on its own, so a typo like "cooldwn" instead of
// "cooldown" would otherwise load silently with no cooldown applied. The
// good file must still load, the same as any other single-file error.
func TestLoadRejectsUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "good.toml", "[[rule]]\nname=\"ok\"\non=[\"x.y\"]\n[[rule.step]]\ndo=\"redis\"\nlist=\"l\"\npush=\"p\"\n")
	writeTOML(t, dir, "typo.toml", `
[[rule]]
name = "typo"
on   = ["x.y"]
cooldwn = "30s"
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)

	cfg, errs := Load(dir)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	if !strings.Contains(msg, "typo.toml") {
		t.Errorf("error should name the file, got %q", msg)
	}
	if !strings.Contains(msg, "cooldwn") {
		t.Errorf("error should name the offending key, got %q", msg)
	}
	if len(cfg.Rules) != 1 || cfg.Rules[0].Name != "ok" {
		t.Fatalf("good rule was lost: %+v", cfg.Rules)
	}
}

// TestLoadRejectsDurableKey guards the specific case named in the design
// discussion: "durable" is not a recognised key on a rule or a step, and
// writing it must not be silently ignored.
func TestLoadRejectsDurableKey(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "durable.toml", `
[[rule]]
name = "r"
on   = ["x.y"]
durable = true
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)

	_, errs := Load(dir)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	if !strings.Contains(errs[0].Error(), "durable") {
		t.Errorf("error should name the offending key, got %v", errs[0])
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

// Parsed fields (concurrency, cancel-on, repeat) must round-trip into the
// struct so Compile can read and validate them.
func TestLoadParsesUnusedFields(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "unused.toml", `
[[rule]]
name = "with-unused"
on   = ["x.y"]
concurrency = "restart"
cancel-on = ["other.event"]
repeat = { count = 3, every = "5s" }
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
	if rule.Repeat == nil || rule.Repeat.Count != 3 || rule.Repeat.Every != "5s" {
		t.Errorf("Repeat = %+v, want {Count: 3, Every: 5s}", rule.Repeat)
	}
}

// TestRepeatWithATypoedKeyIsALoadError is the reason Repeat is a typed struct
// rather than map[string]any: a typed field lets md.Undecoded() see "conut"
// as a key nothing claimed, so the whole file is rejected naming the typo. A
// map would have absorbed it silently and the rule would have loaded with no
// repeat at all, leaving its author believing the feature worked.
func TestRepeatWithATypoedKeyIsALoadError(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "typo.toml", `
[[rule]]
name = "typo"
on   = ["x.y"]
repeat = { conut = 3 }
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)

	_, errs := Load(dir)
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	if !strings.Contains(msg, "typo.toml") {
		t.Errorf("error should name the file, got %q", msg)
	}
	if !strings.Contains(msg, "conut") {
		t.Errorf("error should name the offending key, got %q", msg)
	}
}

// Each rule must carry the correct Source filename so error messages can
// tell the user which file to edit. A regression that dropped tagging or
// only tagged the first file would pass the other tests.
func TestLoadTagsRulesWithSourceFilename(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, "rules-a.toml", `
[[rule]]
name = "from-a"
on   = ["x.y"]
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)
	writeTOML(t, dir, "rules-b.toml", `
[[rule]]
name = "from-b-first"
on   = ["x.y"]
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
[[rule]]
name = "from-b-second"
on   = ["x.y"]
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)

	cfg, errs := Load(dir)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	// Verify each rule has the correct source file tagged.
	if len(cfg.Rules) != 3 {
		t.Fatalf("got %d rules, want 3", len(cfg.Rules))
	}

	if cfg.Rules[0].Name != "from-a" || cfg.Rules[0].Source != "rules-a.toml" {
		t.Errorf("rule 0: name=%q, source=%q, want name=from-a source=rules-a.toml", cfg.Rules[0].Name, cfg.Rules[0].Source)
	}
	if cfg.Rules[1].Name != "from-b-first" || cfg.Rules[1].Source != "rules-b.toml" {
		t.Errorf("rule 1: name=%q, source=%q, want name=from-b-first source=rules-b.toml", cfg.Rules[1].Name, cfg.Rules[1].Source)
	}
	if cfg.Rules[2].Name != "from-b-second" || cfg.Rules[2].Source != "rules-b.toml" {
		t.Errorf("rule 2: name=%q, source=%q, want name=from-b-second source=rules-b.toml", cfg.Rules[2].Name, cfg.Rules[2].Source)
	}
}

// Rules must be loaded in alphabetical order by filename so rule numbering
// is predictable and stable across boots. This test verifies the observable
// property.
//
// Note: os.ReadDir already guarantees alphabetical order per the Go spec, so
// this test cannot distinguish between ordering enforced by sort.Strings and
// ordering from os.ReadDir. A deletion of sort.Strings would not cause this
// test to fail on current Go versions. The sort call exists as insurance for
// future changes to the directory reading mechanism.
func TestLoadRulesInFilenameOrder(t *testing.T) {
	dir := t.TempDir()

	// Write files in reverse alphabetical order (z, y, x).
	writeTOML(t, dir, "z-last.toml", `
[[rule]]
name = "z"
on   = ["x.y"]
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)
	writeTOML(t, dir, "y-middle.toml", `
[[rule]]
name = "y"
on   = ["x.y"]
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)
	writeTOML(t, dir, "x-first.toml", `
[[rule]]
name = "x"
on   = ["x.y"]
  [[rule.step]]
  do = "redis"
  list = "l"
  push = "p"
`)

	cfg, errs := Load(dir)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(cfg.Rules) != 3 {
		t.Fatalf("got %d rules, want 3", len(cfg.Rules))
	}

	// Rules should be in alphabetical order by filename.
	want := []string{"x", "y", "z"}
	for i, wantName := range want {
		if cfg.Rules[i].Name != wantName {
			t.Errorf("rule %d: name=%q, want %q", i, cfg.Rules[i].Name, wantName)
		}
	}
}
