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
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/eventbus"
)

func definition(name string) string {
	return fmt.Sprintf("[[rule]]\nname = %q\non = [\"vehicle.state\"]\n[[rule.step]]\ndo = \"redis\"\nlist = \"test:commands\"\npush = \"test\"\n", name)
}

func put(t *testing.T, dir, name, data string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func request(t *testing.T, m *Manager, method string, payload any) any {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.Handle(context.Background(), method, data)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	return out
}

func rejected(t *testing.T, m *Manager, method string, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Handle(context.Background(), method, data); err == nil {
		t.Fatalf("%s accepted %#v", method, payload)
	}
}

func list(t *testing.T, m *Manager) api.ListResponse {
	t.Helper()
	return request(t, m, api.MethodList, api.ListRequest{}).(api.ListResponse)
}

func TestMissingAddShowAndRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing", "extensions")
	m := New(dir, nil)
	configs, errs := m.RuntimeConfig()
	if len(configs) != 0 || len(errs) != 0 {
		t.Fatalf("missing: %v %v", configs, errs)
	}
	initial := list(t, m)
	if initial.Total != 0 || initial.PendingRestart || initial.Revision == "" {
		t.Fatalf("initial: %+v", initial)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("read created dir: %v", err)
	}
	out := request(t, m, api.MethodAdd, api.AddRequest{Definition: definition("example"), ExpectedRevision: initial.Revision}).(api.MutationResponse)
	if !out.PendingRestart || out.Revision == initial.Revision {
		t.Fatalf("mutation: %+v", out)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("files %v: %v", entries, err)
	}
	if !strings.HasPrefix(entries[0].Name(), "managed-") || !strings.HasSuffix(entries[0].Name(), ".toml") {
		t.Fatal(entries[0].Name())
	}
	st, _ := entries[0].Info()
	if st.Mode().Perm() != 0600 {
		t.Fatalf("mode %v", st.Mode())
	}
	show := request(t, m, api.MethodShow, api.ShowRequest{Name: "example"}).(api.ShowResponse)
	parsed, err := parseDefinition([]byte(show.Definition))
	if err != nil || len(parsed) != 1 || parsed[0].Enabled == nil || !*parsed[0].Enabled {
		t.Fatalf("show definition %q: %v", show.Definition, err)
	}
	if show.Rule.Loaded || !show.PendingRestart {
		t.Fatalf("show: %+v", show)
	}
	restarted := New(dir, nil)
	configs, errs = restarted.RuntimeConfig()
	if len(configs) != 1 || len(errs) != 0 {
		t.Fatalf("restart: %v %v", configs, errs)
	}
	if list(t, restarted).PendingRestart {
		t.Fatal("restart still pending")
	}
	if !list(t, m).PendingRestart {
		t.Fatal("read changed captured runtime revision")
	}
}

func TestOverrideDoesNotRewriteSourceOrRuntime(t *testing.T) {
	dir := t.TempDir()
	source := definition("one") + definition("two")
	put(t, dir, "hand.toml", source)
	m := New(dir, nil)
	m.RuntimeConfig()
	m.SetRuntime(func(name string) api.RuleSummary {
		return api.RuleSummary{Loaded: true, LastFire: 42, Errors: 3, ActiveRuns: 2}
	}, func() api.StatusResponse {
		return api.StatusResponse{Version: "v-test", QueueDepth: 3, QueueCapacity: 32, Workers: 4, Counters: map[string]string{"count": "5"}}
	})
	initial := list(t, m)
	disabled := request(t, m, api.MethodSetEnabled, api.SetEnabledRequest{Name: "one", Enabled: false, ExpectedRevision: initial.Revision}).(api.MutationResponse)
	if !disabled.PendingRestart {
		t.Fatal("not pending")
	}
	unchanged := request(t, m, api.MethodSetEnabled, api.SetEnabledRequest{Name: "one", Enabled: false, ExpectedRevision: disabled.Revision}).(api.MutationResponse)
	if unchanged.Revision != disabled.Revision || !unchanged.PendingRestart {
		t.Fatalf("no-op: %+v", unchanged)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "hand.toml"))
	if string(data) != source {
		t.Fatal("hand-authored multi-rule file changed")
	}
	show := request(t, m, api.MethodShow, api.ShowRequest{Name: "one"}).(api.ShowResponse)
	if show.Rule.Enabled || !show.Rule.Loaded || show.Rule.LastFire != 42 || show.Rule.ActiveRuns != 2 || show.Rule.Errors != 3 {
		t.Fatalf("metrics: %+v", show)
	}
	status := request(t, m, api.MethodStatus, api.Empty{}).(api.StatusResponse)
	if status.Version != "v-test" || status.QueueDepth != 3 || status.APIVersion != 1 || status.AppliedRevision != initial.Revision || status.Revision != disabled.Revision || !status.PendingRestart {
		t.Fatalf("status %+v", status)
	}
	restarted := New(dir, nil)
	configs, errs := restarted.RuntimeConfig()
	if len(errs) != 0 || len(configs) != 2 || *configs[0].Enabled || !*configs[1].Enabled {
		t.Fatalf("restart desired: %+v %v", configs, errs)
	}
	if list(t, restarted).PendingRestart {
		t.Fatal("restart pending")
	}
}

func TestPaginationAndRawFileRevision(t *testing.T) {
	dir := t.TempDir()
	var source strings.Builder
	for i := 0; i < 110; i++ {
		source.WriteString(definition(fmt.Sprintf("rule-%03d", i)))
	}
	put(t, dir, "rules.toml", source.String())
	m := New(dir, nil)
	first := list(t, m)
	if first.Total != 110 || len(first.Rules) != 50 {
		t.Fatalf("default page %+v", first)
	}
	page := request(t, m, api.MethodList, api.ListRequest{Offset: 100, Limit: 100}).(api.ListResponse)
	if len(page.Rules) != 10 || page.Rules[0].Name != "rule-100" {
		t.Fatalf("page %+v", page)
	}
	empty := request(t, m, api.MethodList, api.ListRequest{Offset: int(^uint(0) >> 1), Limit: 100}).(api.ListResponse)
	if len(empty.Rules) != 0 {
		t.Fatal("out of range page")
	}
	rejected(t, m, api.MethodList, api.ListRequest{Offset: -1})
	rejected(t, m, api.MethodList, api.ListRequest{Limit: 101})
	put(t, dir, "rules.toml", source.String()+"\n# external comment\n")
	next := list(t, m)
	if next.Revision == first.Revision {
		t.Fatal("raw comment not hashed")
	}
	rejected(t, m, api.MethodSetEnabled, api.SetEnabledRequest{Name: "rule-000", Enabled: false, ExpectedRevision: first.Revision})
	put(t, dir, "ancillary.txt", "ignored")
	if list(t, m).Revision != next.Revision {
		t.Fatal("ancillary content hashed")
	}
	if err := os.Rename(filepath.Join(dir, "rules.toml"), filepath.Join(dir, "renamed.toml")); err != nil {
		t.Fatal(err)
	}
	if list(t, m).Revision == next.Revision {
		t.Fatal("filename not hashed")
	}
}

func TestInvalidAndDuplicateCatalogue(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "invalid.toml", "[[rule]]\nname=\"invalid\"\nenabled=false\non=[\"vehicle.state\"]\n[[rule.step]]\ndo=\"invalid\"\n")
	put(t, dir, "a.toml", definition("duplicate"))
	put(t, dir, "b.toml", definition("duplicate"))
	m := New(dir, nil)
	out := list(t, m)
	if out.Total != 3 || len(out.Diagnostics) < 3 || out.Rules[2].Enabled || len(out.Rules[2].Diagnostics) == 0 {
		t.Fatalf("invalid: %+v", out)
	}
	request(t, m, api.MethodShow, api.ShowRequest{Name: "invalid"})
	rejected(t, m, api.MethodShow, api.ShowRequest{Name: "duplicate"})
	rejected(t, m, api.MethodSetEnabled, api.SetEnabledRequest{Name: "duplicate", Enabled: false, ExpectedRevision: out.Revision})
	rejected(t, m, api.MethodAdd, api.AddRequest{Definition: definition("duplicate"), ExpectedRevision: out.Revision})
	// Semantic errors do not hide definitions, so unrelated additions remain safe.
	request(t, m, api.MethodAdd, api.AddRequest{Definition: definition("valid"), ExpectedRevision: out.Revision})
}

func TestRejectDefinitionsAndRequestFields(t *testing.T) {
	m := New(t.TempDir(), nil)
	rev := list(t, m).Revision
	definitions := []string{"", definition("one") + definition("two"), definition("../escape"), definition(" a"), definition("."), definition(".."), definition("a/b"), definition("a\\b"), definition("a\n"), definition(strings.Repeat("a", 129)), "unknown=true\n" + definition("one"), strings.Replace(definition("one"), "do = \"redis\"", "do = \"invalid\"", 1), strings.Replace(strings.Replace(definition("one"), "name = \"one\"", "name = \"one\"\nenabled=false", 1), "do = \"redis\"", "do = \"invalid\"", 1)}
	for _, def := range definitions {
		rejected(t, m, api.MethodAdd, api.AddRequest{Definition: def, ExpectedRevision: rev})
	}
	for _, method := range []string{api.MethodList, api.MethodShow, api.MethodAdd, api.MethodSetEnabled, api.MethodTest, api.MethodStatus} {
		rejected(t, m, method, map[string]any{"unexpected": true})
	}
	rejected(t, m, api.MethodAdd, map[string]any{"definition": definition("one")})
	rejected(t, m, api.MethodSetEnabled, map[string]any{"name": "one", "expected_revision": rev})
	rejected(t, m, api.MethodShow, api.ShowRequest{Name: "missing"})
	rejected(t, m, "v9.list", api.Empty{})
}

func TestUnsafeSourcesRefuseMutation(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{"syntax", func(t *testing.T, d string) { put(t, d, "bad.toml", "[[rule") }},
		{"unknown TOML", func(t *testing.T, d string) { put(t, d, "bad.toml", "typo=true") }},
		{"large file", func(t *testing.T, d string) { put(t, d, "big.toml", strings.Repeat("#", maxFileBytes+1)) }},
		{"aggregate", func(t *testing.T, d string) {
			for i := 0; i < 17; i++ {
				put(t, d, fmt.Sprintf("%02d.toml", i), strings.Repeat("#", maxFileBytes))
			}
		}},
		{"entries", func(t *testing.T, d string) {
			for i := 0; i < 129; i++ {
				put(t, d, fmt.Sprintf("%03d.txt", i), "")
			}
		}},
		{"rules", func(t *testing.T, d string) { put(t, d, "many.toml", strings.Repeat("[[rule]]\nname=\"a\"\n", 513)) }},
		{"steps", func(t *testing.T, d string) {
			put(t, d, "steps.toml", "[[rule]]\nname=\"a\"\non=[\"vehicle.state\"]\n"+strings.Repeat("[[rule.step]]\ndo=\"redis\"\nlist=\"x\"\npush=\"x\"\n", 129))
		}},
		{"symlink", func(t *testing.T, d string) {
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, []byte(definition("outside")), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(d, "link.toml")); err != nil {
				t.Fatal(err)
			}
		}},
		{"directory entry", func(t *testing.T, d string) {
			if err := os.Mkdir(filepath.Join(d, "dir.toml"), 0700); err != nil {
				t.Fatal(err)
			}
		}},
		{"override symlink", func(t *testing.T, d string) {
			if err := os.Symlink("missing", filepath.Join(d, overrideFile)); err != nil {
				t.Fatal(err)
			}
		}},
		{"override version", func(t *testing.T, d string) { put(t, d, overrideFile, `{"version":2,"rules":{}}`) }},
		{"override unknown", func(t *testing.T, d string) { put(t, d, overrideFile, `{"version":1,"rules":{},"unexpected":true}`) }},
		{"override null", func(t *testing.T, d string) { put(t, d, overrideFile, `{"version":1,"rules":{"one":null}}`) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tt.setup(t, dir)
			m := New(dir, nil)
			out := list(t, m)
			if len(out.Diagnostics) == 0 {
				t.Fatal("no diagnostics")
			}
			rejected(t, m, api.MethodAdd, api.AddRequest{Definition: definition("new"), ExpectedRevision: out.Revision})
		})
	}
}

func TestRootSymlinkAndCancellation(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	m := New(link+"/", nil)
	out := list(t, m)
	if len(out.Diagnostics) == 0 {
		t.Fatal("accepted symlink root")
	}
	rejected(t, m, api.MethodAdd, api.AddRequest{Definition: definition("new"), ExpectedRevision: out.Revision})
	missing := filepath.Join(t.TempDir(), "missing")
	m = New(missing, nil)
	data, _ := json.Marshal(api.AddRequest{Definition: definition("new"), ExpectedRevision: list(t, m).Revision})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Handle(ctx, api.MethodAdd, data); err != context.Canceled {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("cancel created dir: %v", err)
	}
}

func TestPurePreviewAndFreshState(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "one.toml", strings.Replace(definition("one"), "on = [\"vehicle.state\"]", "on = [\"vehicle.state\"]\nwhen = 'state(\"vehicle\", \"state\") == \"parked\"'", 1))
	value := "parked"
	snapshots := 0
	m := New(dir, func() rules.StateFunc {
		snapshots++
		captured := value
		return func(string, string) string { return captured }
	})
	m.SetRuntime(func(string) api.RuleSummary { t.Fatal("test invoked runtime info"); return api.RuleSummary{} }, func() api.StatusResponse { t.Fatal("test invoked runtime status"); return api.StatusResponse{} })
	out := request(t, m, api.MethodTest, api.TestRequest{Name: "one", Event: eventbus.Event{Topic: "vehicle.state"}}).(api.TestResponse)
	if !out.Matched || len(out.Steps) != 1 {
		t.Fatalf("preview: %+v", out)
	}
	value = "off"
	out = request(t, m, api.MethodTest, api.TestRequest{Name: "one", Event: eventbus.Event{Topic: "vehicle.state"}}).(api.TestResponse)
	if out.Matched || snapshots < 2 {
		t.Fatalf("stale preview: %+v", out)
	}
}

func TestFullDirectoryOverrideAndNoClobber(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "one.toml", definition("one"))
	put(t, dir, overrideFile, `{"version":1,"rules":{"one":true}}`)
	for i := 0; i < 126; i++ {
		put(t, dir, fmt.Sprintf("%03d.txt", i), "")
	}
	m := New(dir, nil)
	out := list(t, m)
	request(t, m, api.MethodSetEnabled, api.SetEnabledRequest{Name: "one", Enabled: false, ExpectedRevision: out.Revision})
	entries, _ := os.ReadDir(dir)
	if len(entries) != 128 {
		t.Fatalf("temp leaked: %d", len(entries))
	}
	rejected(t, m, api.MethodAdd, api.AddRequest{Definition: definition("new"), ExpectedRevision: list(t, m).Revision})
}
