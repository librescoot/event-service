package control

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/librescoot/event-service/api"
)

func TestInvalidOverrideFailsRuntimeClosed(t *testing.T) {
	for _, data := range []string{`{`, `{"version":2,"rules":{}}`, `{"version":1,"rules":{"one":null}}`, `{"version":1,"rules":{},"typo":true}`, strings.Repeat("x", maxFileBytes+1)} {
		t.Run(fmt.Sprintf("bytes-%d-%x", len(data), []byte(data[:min(8, len(data))])), func(t *testing.T) {
			dir := t.TempDir()
			put(t, dir, "one.toml", definition("one"))
			put(t, dir, overrideFile, data)
			m := New(dir, nil)
			cfg, errs := m.RuntimeConfig()
			if len(cfg) != 0 || len(errs) == 0 {
				t.Fatalf("unsafe runtime configs: %+v %v", cfg, errs)
			}
			out := list(t, m)
			if out.Total != 1 || !strings.Contains(strings.Join(out.Diagnostics, " "), "no runtime rules will be activated") {
				t.Fatalf("missing catalogue or fatal diagnostic: %+v", out)
			}
			rejected(t, m, api.MethodSetEnabled, api.SetEnabledRequest{Name: "one", Enabled: false, ExpectedRevision: out.Revision})
			rejected(t, m, api.MethodAdd, api.AddRequest{Definition: definition("new"), ExpectedRevision: out.Revision})
			put(t, dir, overrideFile, `{"version":1,"rules":{"one":false}}`)
			cfg, errs = New(dir, nil).RuntimeConfig()
			if len(cfg) != 1 || len(errs) != 0 || *cfg[0].Enabled {
				t.Fatalf("repair failed: %+v %v", cfg, errs)
			}
		})
	}
}

func TestUnreadableOverrideFailsRuntimeClosed(t *testing.T) {
	for _, kind := range []string{"symlink", "directory", "permissions"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			put(t, dir, "one.toml", definition("one"))
			path := filepath.Join(dir, overrideFile)
			switch kind {
			case "symlink":
				if err := os.Symlink("missing", path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if os.Geteuid() == 0 {
					t.Skip("root bypasses file permissions")
				}
				put(t, dir, overrideFile, `{"version":1,"rules":{"one":false}}`)
				if err := os.Chmod(path, 0); err != nil {
					t.Fatal(err)
				}
			}
			m := New(dir, nil)
			cfg, errs := m.RuntimeConfig()
			if len(cfg) != 0 || len(errs) == 0 {
				t.Fatalf("unsafe runtime configs: %+v %v", cfg, errs)
			}
			if list(t, m).Total != 1 {
				t.Fatal("lost diagnostic catalogue")
			}
		})
	}
}

func TestSkippedOverrideAtAggregateLimitFailsClosed(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "!000.toml", definition("one"))
	for i := 1; i <= 17; i++ {
		put(t, dir, fmt.Sprintf("!%03d.toml", i), strings.Repeat("#", maxFileBytes))
	}
	put(t, dir, overrideFile, `{"version":1,"rules":{"one":false}}`)
	cfg, errs := New(dir, nil).RuntimeConfig()
	if len(cfg) != 0 || len(errs) == 0 {
		t.Fatalf("skipped authority enabled TOML defaults: %+v %v", cfg, errs)
	}
}

func TestMalformedTOMLStillOnlySkipsItsFile(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "one.toml", definition("one"))
	put(t, dir, "bad.toml", "[[rule")
	cfg, errs := New(dir, nil).RuntimeConfig()
	if len(cfg) != 1 || !*cfg[0].Enabled || len(errs) == 0 {
		t.Fatalf("ordinary TOML failure became fatal: %+v %v", cfg, errs)
	}
}

func TestEncodedOutputLimitCheckedBeforeSaving(t *testing.T) {
	dir := t.TempDir()
	m := New(dir, nil)
	def := "[[rule]]\nname='expanded'\non=['vehicle.state']\n" + strings.Repeat("[[rule.step]]\ndo='exec'\ncommand='"+strings.Repeat("x", 350)+"'\n", 128)
	cfg, err := parseDefinition([]byte(def))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeRule(cfg[0])
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(api.AddRequest{Definition: def, ExpectedRevision: list(t, m).Revision})
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > api.MaxRequestBytes || len(encoded) <= maxFileBytes {
		t.Fatalf("bad expansion fixture: request %d, encoded %d", len(payload), len(encoded))
	}
	if _, err := m.Handle(context.Background(), api.MethodAdd, payload); err == nil || !strings.Contains(err.Error(), "64 KiB") {
		t.Fatalf("output size not rejected: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("oversized output persisted: %v", entries)
	}
}
