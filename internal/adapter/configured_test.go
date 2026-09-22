package adapter

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/librescoot/event-service/internal/inputs"
	"github.com/librescoot/event-service/internal/shadow"
)

func configuredForTest(t *testing.T, configs ...inputs.Config) Source {
	t.Helper()
	s, err := NewConfiguredSource(configs)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConfiguredValidation(t *testing.T) {
	for _, c := range []inputs.Config{
		{},
		{Hash: "ev:input.test", Fields: []string{"x"}},
		{Channel: "ev:input.test", Topic: "input.test"},
		{Channel: "__keyspace@0__:x", Topic: "input.test"},
		{Channel: "raw*", Topic: "input.test"},
		{Channel: "raw", Topic: "vehicle.changed"},
		{Channel: "raw", Topic: "input.test", Format: "envelope"},
		{Channel: "raw", Topic: "input.test", MaxBytes: 65537},
		{Hash: "state", Fields: []string{"x", "x"}},
	} {
		if s, err := NewConfiguredSource([]inputs.Config{c}); err == nil || s != nil {
			t.Errorf("accepted invalid config %+v", c)
		}
	}
	s := configuredForTest(t)
	if len(s.Hashes()) != 0 || len(s.Channels()) != 0 || len(s.OnField("x", "y", "z", "")) != 0 || len(s.OnMessage("x", "y")) != 0 {
		t.Fatal("empty config subscribed or emitted")
	}
}

func TestConfiguredFields(t *testing.T) {
	s := configuredForTest(t,
		inputs.Config{Hash: "state", Fields: []string{"a", "b"}, Topic: "input.changed"},
		inputs.Config{Hash: "state", Fields: []string{"b", "a"}, Topic: "input.changed"},
		inputs.Config{Hash: "state", Fields: []string{"b", "c"}, Topic: "input.changed"},
		inputs.Config{Hash: "state", Fields: []string{"b"}, Topic: "input.other"},
		inputs.Config{Hash: "state", Fields: []string{"silent"}},
		inputs.Config{Hash: "dependency", Fields: []string{"a"}},
	)
	if !reflect.DeepEqual(s.Hashes(), []string{"state", "dependency"}) || len(s.Channels()) != 0 {
		t.Fatalf("subscriptions: %v / %v", s.Hashes(), s.Channels())
	}
	for _, tc := range []struct {
		hash, field string
		count       int
	}{
		{"state", "a", 1}, {"state", "b", 2}, {"state", "c", 1},
		{"state", "unknown", 0}, {"unknown", "a", 0},
		{"state", "silent", 0}, {"dependency", "a", 0},
	} {
		for _, prev := range []string{"", "old"} {
			events := s.OnField(tc.hash, tc.field, "new", prev)
			if len(events) != tc.count {
				t.Fatalf("%+v prev=%q: %v", tc, prev, events)
			}
			for _, e := range events {
				if e.Src != "adapter" || e.From != prev || e.To != "new" || !reflect.DeepEqual(e.Data, map[string]any{"hash": tc.hash, "field": tc.field}) {
					t.Fatalf("bad edge: %+v", e)
				}
			}
		}
	}
	if got := topics(s.OnField("state", "b", "new", "old")); !reflect.DeepEqual(got, []string{"input.changed", "input.other"}) {
		t.Fatal(got)
	}
}

func TestConfiguredAdapterSuppressesNoopAndSeeding(t *testing.T) {
	s := configuredForTest(t, inputs.Config{Hash: "state", Fields: []string{"a"}, Topic: "input.changed"})
	em := &collectingEmitter{}
	a := New(nil, em, shadow.NewStore())
	a.Register(s)
	a.setSeeding("state", true)
	a.dispatchField("state", "a", "seed")
	a.setSeeding("state", false)
	a.dispatchField("state", "a", "seed")
	if em.count() != 0 {
		t.Fatal("seed/no-op emitted")
	}
	a.dispatchField("state", "a", "live")
	a.dispatchField("state", "a", "live")
	if em.count() != 1 || em.events[0].From != "seed" {
		t.Fatalf("events: %+v", em.events)
	}
}

func TestConfiguredMessages(t *testing.T) {
	for _, tc := range []struct {
		name, format, payload string
		want                  any
		valid                 bool
	}{
		{"default", "", "raw text", "raw text", true},
		{"string JSON", "string", `{"x":1}`, `{"x":1}`, true},
		{"object", "json", `{"x":1}`, map[string]any{"x": float64(1)}, true},
		{"array", "json", `[1,true]`, []any{float64(1), true}, true},
		{"number", "json", `42`, float64(42), true},
		{"boolean", "json", `false`, false, true},
		{"string", "json", `"hello"`, "hello", true},
		{"null", "json", `null`, nil, true},
		{"invalid", "json", `{`, nil, false},
		{"trailing", "json", `{} {}`, nil, false},
		{"empty", "json", ``, nil, false},
		{"envelope", "json", `{"topic":"vehicle.unlocked","src":"remote","data":null}`, map[string]any{"topic": "vehicle.unlocked", "src": "remote", "data": nil}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := inputs.Config{Channel: "raw", Topic: "input.message", Format: tc.format}
			s := configuredForTest(t, c, c)
			if !reflect.DeepEqual(s.Channels(), []string{"raw"}) || len(s.Hashes()) != 0 {
				t.Fatal("subscriptions")
			}
			if len(s.OnMessage("unknown", tc.payload)) != 0 {
				t.Fatal("unknown channel emitted")
			}
			events := s.OnMessage("raw", tc.payload)
			if !tc.valid {
				if len(events) != 0 {
					t.Fatal(events)
				}
				return
			}
			if len(events) != 1 {
				t.Fatal(events)
			}
			e := events[0]
			if e.Src != "adapter" || e.Topic != "input.message" || e.From != "" || e.To != "" || !reflect.DeepEqual(e.Data, map[string]any{"channel": "raw", "payload": tc.want}) {
				t.Fatalf("event: %+v", e)
			}
		})
	}
}

func TestConfiguredLimits(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	for _, format := range []string{"string", "json"} {
		s := configuredForTest(t, inputs.Config{Channel: "raw", Topic: "input.small", Format: format, MaxBytes: 4}, inputs.Config{Channel: "raw", Topic: "input.large", Format: format, MaxBytes: 8})
		if len(s.OnMessage("raw", "1234")) != 2 {
			t.Fatal("exact limit rejected")
		}
		if got := topics(s.OnMessage("raw", "12345")); !reflect.DeepEqual(got, []string{"input.large"}) {
			t.Fatal(got)
		}
		if len(s.OnMessage("raw", "123456789")) != 0 {
			t.Fatal("oversized accepted")
		}
	}
	s := configuredForTest(t, inputs.Config{Hash: "state", Fields: []string{"x"}, Topic: "input.edge", MaxBytes: 4})
	for _, values := range [][2]string{{"12345", "old"}, {"new", "12345"}} {
		if len(s.OnField("state", "x", values[0], values[1])) != 0 {
			t.Fatal("oversized edge accepted")
		}
	}
	if len(s.OnField("state", "x", "1234", "1234")) != 1 {
		t.Fatal("source should leave no-op suppression upstream")
	}
	for _, limit := range []int{0, 65536} {
		c := inputs.Config{Channel: "raw", Topic: "input.limit", MaxBytes: limit}
		s := configuredForTest(t, c)
		if len(s.OnMessage("raw", strings.Repeat("x", c.Limit()))) != 1 || len(s.OnMessage("raw", strings.Repeat("x", c.Limit()+1))) != 0 {
			t.Fatal("default/max limit")
		}
	}
	s = configuredForTest(t, inputs.Config{Channel: "raw", Topic: "input.json", Format: "json", MaxBytes: 4})
	logs.Reset()
	s.OnMessage("raw", "secret invalid JSON")
	if strings.Contains(logs.String(), "secret") || strings.Contains(logs.String(), "invalid JSON") || !strings.Contains(logs.String(), "oversized") {
		t.Fatalf("decode before size check or leaked payload: %s", &logs)
	}
	logs.Reset()
	s.OnMessage("raw", "oops")
	if strings.Contains(logs.String(), "oops") || !strings.Contains(logs.String(), "invalid JSON") {
		t.Fatal(logs.String())
	}
}

func TestConfiguredFieldLimits(t *testing.T) {
	s := configuredForTest(t,
		inputs.Config{Hash: "state", Fields: []string{"a", "b"}, Topic: "input.edge", MaxBytes: 8},
		inputs.Config{Hash: "state", Fields: []string{"b", "c"}, MaxBytes: 16},
		inputs.Config{Hash: "dependency", Fields: []string{"x"}},
	)
	selected, ok := s.(interface{ FieldLimits(string) map[string]int })
	if !ok {
		t.Fatal("missing selected-field interface")
	}
	want := map[string]int{"a": 8, "b": 16, "c": 16}
	if got := selected.FieldLimits("state"); !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
	selected.FieldLimits("state")["a"] = 1
	if !reflect.DeepEqual(selected.FieldLimits("state"), want) {
		t.Fatal("limits were mutable")
	}
	if got := selected.FieldLimits("dependency"); !reflect.DeepEqual(got, map[string]int{"x": 4096}) {
		t.Fatal(got)
	}
	if len(selected.FieldLimits("unknown")) != 0 {
		t.Fatal("unknown hash selected")
	}
}

func TestConfiguredEquivalentRoutesAndOwnership(t *testing.T) {
	configs := []inputs.Config{
		{Hash: "state", Fields: []string{"a"}, Topic: "input.edge", MaxBytes: 2},
		{Hash: "state", Fields: []string{"a", "b"}, Topic: "input.edge", MaxBytes: 8},
		{Channel: "raw", Topic: "input.message"},
		{Channel: "raw", Topic: "input.message", Format: "string", MaxBytes: 4096},
		{Channel: "raw", Topic: "input.other"},
	}
	s := configuredForTest(t, configs...)
	configs[0].Fields[0] = "mutated"
	configs[2].Topic = "input.mutated"
	s.Hashes()[0] = "mutated"
	s.Channels()[0] = "mutated"
	if len(s.OnField("state", "a", "long", "old")) != 1 {
		t.Fatal("overlapping route limit union")
	}
	if got := topics(s.OnMessage("raw", "x")); !reflect.DeepEqual(got, []string{"input.message", "input.other"}) {
		t.Fatal(got)
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				s.Hashes()[0] = "local"
				s.Channels()[0] = "local"
				e := s.OnField("state", "a", "new", "old")
				e[0].Data["hash"] = "local"
				m := s.OnMessage("raw", "x")
				m[0].Data["payload"] = "local"
			}
		}()
	}
	wg.Wait()
}
