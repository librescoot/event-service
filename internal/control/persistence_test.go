package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/librescoot/event-service/api"
	"github.com/librescoot/event-service/internal/rules"
)

func TestManagedDestinationNeverClobbered(t *testing.T) {
	dir := t.TempDir()
	name := fmt.Sprintf("managed-%x.toml", sha256.Sum256([]byte("new")))
	source := "# hand-authored file reserved this name\n" + definition("existing")
	put(t, dir, name, source)
	m := New(dir, nil)
	rejected(t, m, api.MethodAdd, api.AddRequest{Definition: definition("new"), ExpectedRevision: list(t, m).Revision})
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil || string(data) != source {
		t.Fatalf("destination changed: %q %v", data, err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("temp leaked: %v", entries)
	}
}

func TestDisabledAddAndEnableOverride(t *testing.T) {
	dir := t.TempDir()
	m := New(dir, nil)
	def := strings.Replace(definition("disabled"), "name = \"disabled\"", "name = \"disabled\"\nenabled = false", 1)
	request(t, m, api.MethodAdd, api.AddRequest{Definition: def, ExpectedRevision: list(t, m).Revision})
	out := list(t, m)
	if out.Total != 1 || out.Rules[0].Enabled {
		t.Fatalf("desired default: %+v", out)
	}
	request(t, m, api.MethodSetEnabled, api.SetEnabledRequest{Name: "disabled", Enabled: true, ExpectedRevision: out.Revision})
	restarted := New(dir, nil)
	cfg, errs := restarted.RuntimeConfig()
	if len(errs) != 0 || len(cfg) != 1 || !*cfg[0].Enabled {
		t.Fatalf("override: %+v %v", cfg, errs)
	}
	name := fmt.Sprintf("managed-%x.toml", sha256.Sum256([]byte("disabled")))
	data, _ := os.ReadFile(filepath.Join(dir, name))
	original, err := parseDefinition(data)
	if err != nil || *original[0].Enabled {
		t.Fatalf("TOML rewritten: %s %v", data, err)
	}
}

func TestRecheckRefusesExternalEditAndCleansTemp(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "one.toml", definition("one"))
	m := New(dir, nil)
	revision := list(t, m).Revision
	snapshots := 0
	m.snapshot = func() rules.StateFunc {
		snapshots++
		// Add's semantic validation follows its initial directory snapshot.
		if snapshots == 2 {
			put(t, dir, "one.toml", definition("one")+"\n# concurrent editor\n")
		}
		return func(string, string) string { return "" }
	}
	rejected(t, m, api.MethodAdd, api.AddRequest{Definition: definition("new"), ExpectedRevision: revision})
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != "one.toml" {
		t.Fatalf("temp or new definition survived: %v", entries)
	}
}

func TestCancellationDuringValidationCreatesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	m := New(dir, nil)
	revision := list(t, m).Revision
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.snapshot = func() rules.StateFunc { cancel(); return func(string, string) string { return "" } }
	payload, _ := json.Marshal(api.AddRequest{Definition: definition("new"), ExpectedRevision: revision})
	if _, err := m.Handle(ctx, api.MethodAdd, payload); err != context.Canceled {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("cancel created directory: %v", err)
	}
}
