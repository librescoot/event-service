package control

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/librescoot/event-service/api"
	"github.com/librescoot/event-service/internal/inputs"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/eventbus"
)

const managedInputsTOML = `
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
`

func managedInputDefinition(name string) string {
	return definition(name) + managedInputsTOML
}

func TestInputsManagerPersistsDesiredUntilRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "extensions")
	m := New(dir, nil)
	applied, errs := m.RuntimeConfig()
	if len(applied) != 0 || len(errs) != 0 {
		t.Fatalf("initial runtime: %v %v", applied, errs)
	}
	initial := list(t, m)
	def := managedInputDefinition("sensors")
	parsed, err := parseDefinition([]byte(def))
	if err != nil {
		t.Fatal(err)
	}
	want := []inputs.Config{
		{Hash: "sensor:status", Fields: []string{"temperature", "ready"}, Topic: "input.sensor", MaxBytes: 1024},
		{Hash: "sensor:limits", Fields: []string{"maximum"}},
		{Channel: "sensor:raw", Topic: "input.raw", Format: "string"},
		{Channel: "sensor:json", Topic: "input.json", Format: "json", MaxBytes: 65536},
	}
	if len(parsed) != 1 || !reflect.DeepEqual(parsed[0].Inputs, want) {
		t.Fatalf("parsed: %+v", parsed)
	}
	added := request(t, m, api.MethodAdd, api.AddRequest{Definition: def, ExpectedRevision: initial.Revision}).(api.MutationResponse)
	desired := list(t, m)
	if !added.PendingRestart || desired.Revision != added.Revision || desired.AppliedRevision != initial.Revision || !desired.PendingRestart || desired.Total != 1 || len(desired.Diagnostics) != 0 {
		t.Fatalf("desired: %+v; add: %+v", desired, added)
	}
	show := request(t, m, api.MethodShow, api.ShowRequest{Name: "sensors"}).(api.ShowResponse)
	shown, err := parseDefinition([]byte(show.Definition))
	if err != nil || len(shown) != 1 || !reflect.DeepEqual(shown[0].Inputs, want) {
		t.Fatalf("show lost inputs: %s (%v)", show.Definition, err)
	}
	if show.Rule.Loaded || !show.PendingRestart {
		t.Fatalf("add activated runtime: %+v", show)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("files: %v %v", entries, err)
	}
	path := filepath.Join(dir, entries[0].Name())
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := parseDefinition(stored)
	if err != nil || len(persisted) != 1 || !reflect.DeepEqual(persisted[0].Inputs, want) {
		t.Fatalf("stored inputs: %s (%v)", stored, err)
	}
	request(t, m, api.MethodSetEnabled, api.SetEnabledRequest{Name: "sensors", Enabled: false, ExpectedRevision: desired.Revision})
	unchanged, err := os.ReadFile(path)
	if err != nil || string(unchanged) != string(stored) {
		t.Fatalf("override rewrote input definition: %v", err)
	}
	restarted := New(dir, nil)
	runtime, errs := restarted.RuntimeConfig()
	if len(errs) != 0 || len(runtime) != 1 || runtime[0].Enabled == nil || *runtime[0].Enabled || !reflect.DeepEqual(runtime[0].Inputs, want) {
		t.Fatalf("restart: %+v %v", runtime, errs)
	}
	compiled, errs := rules.CompileForRuntime(runtime, nil)
	if len(errs) != 0 || len(compiled) != 1 || !compiled[0].ReplayOnly || !reflect.DeepEqual(compiled[0].Inputs, want) {
		t.Fatalf("restart compile: %v %v", compiled, errs)
	}
	after := list(t, restarted)
	if after.PendingRestart || after.AppliedRevision != after.Revision {
		t.Fatalf("restart revisions: %+v", after)
	}
	if old := list(t, m); !old.PendingRestart || old.AppliedRevision != initial.Revision {
		t.Fatalf("reads advanced original runtime: %+v", old)
	}
}

func TestInputsManagerRejectsUnknownAndInvalidDefinitions(t *testing.T) {
	for _, suffix := range []string{
		"hash = 'sensor'\nfields = ['value']\nmax-byte = 1",
		"hash = 'ev:private'\nfields = ['value']",
		"channel = '__keyspace@0__:sensor'\ntopic = 'input.sensor'",
		"hash = 'sensor*'\nfields = ['value']",
		"channel = 'events'\ntopic = 'vehicle.state'",
		"channel = 'events'\ntopic = 'input.sensor'\nformat = 'xml'",
		"hash = 'sensor'\nfields = ['value', 'value']",
		"hash = 'sensor'\nfields = ['value']\nmax-bytes = 65537",
	} {
		for _, disabled := range []bool{false, true} {
			t.Run(suffix+map[bool]string{true: "/disabled", false: "/enabled"}[disabled], func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "missing")
				m := New(dir, nil)
				m.RuntimeConfig()
				initial := list(t, m)
				def := definition("bad")
				if disabled {
					def = strings.Replace(def, "name = \"bad\"", "name = \"bad\"\nenabled = false", 1)
				}
				rejected(t, m, api.MethodAdd, api.AddRequest{Definition: def + "\n[[rule.input]]\n" + suffix + "\n", ExpectedRevision: initial.Revision})
				after := list(t, m)
				if after.Revision != initial.Revision || after.Total != 0 || after.PendingRestart {
					t.Fatalf("rejected add changed desired state: %+v", after)
				}
				if _, err := os.Stat(dir); !os.IsNotExist(err) {
					t.Fatalf("rejected add wrote directory: %v", err)
				}
			})
		}
	}
}

func TestInputsManagerDryRunUsesSnapshotWithoutSynthesizingWatches(t *testing.T) {
	dir := t.TempDir()
	def := strings.Replace(managedInputDefinition("sensors"), "on = [\"vehicle.state\"]", "on = [\"input.sensor\"]\nwhen = 'state(\"sensor:status\", \"ready\") == \"yes\"'", 1)
	put(t, dir, "sensors.toml", def)
	value := ""
	lookups := 0
	m := New(dir, func() rules.StateFunc {
		captured := value
		return func(hash, field string) string {
			lookups++
			if hash != "sensor:status" || field != "ready" {
				t.Fatalf("unexpected state lookup %q/%q", hash, field)
			}
			return captured
		}
	})
	m.RuntimeConfig()
	initial := list(t, m)
	m.SetRuntime(func(string) api.RuleSummary { t.Fatal("dry run read runtime info"); return api.RuleSummary{} }, func() api.StatusResponse { t.Fatal("dry run read runtime status"); return api.StatusResponse{} })
	event := eventbus.Event{Topic: "input.sensor", To: "yes", Data: map[string]any{"ready": "yes", "hash": "sensor:status", "field": "ready", "value": "yes"}}
	out := request(t, m, api.MethodTest, api.TestRequest{Name: "sensors", Event: event}).(api.TestResponse)
	if out.Matched || out.Error != "" || len(out.Steps) != 1 {
		t.Fatalf("missing watch synthesized from event: %+v", out)
	}
	value = "yes"
	out = request(t, m, api.MethodTest, api.TestRequest{Name: "sensors", Event: event}).(api.TestResponse)
	if !out.Matched || out.Error != "" || lookups < 2 {
		t.Fatalf("fresh snapshot not used: %+v (%d lookups)", out, lookups)
	}
	value = ""
	out = request(t, m, api.MethodTest, api.TestRequest{Name: "sensors", Event: event}).(api.TestResponse)
	if out.Matched {
		t.Fatal("dry run retained synthesized watch state")
	}
	data, err := os.ReadFile(filepath.Join(dir, "sensors.toml"))
	if err != nil || string(data) != def {
		t.Fatalf("dry run rewrote source: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("dry run wrote files: %v %v", entries, err)
	}
	m.SetRuntime(nil, nil)
	after := list(t, m)
	if after.Revision != initial.Revision || after.AppliedRevision != initial.AppliedRevision || after.PendingRestart {
		t.Fatalf("dry run changed revisions: %+v", after)
	}
}
