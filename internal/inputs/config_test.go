package inputs

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func inputFields(n int) []string {
	fields := make([]string, n)
	for i := range fields {
		fields[i] = fmt.Sprintf("field-%02d", i)
	}
	return fields
}

func TestConfigValid(t *testing.T) {
	for _, c := range []Config{
		{Hash: "sensor:status", Fields: []string{"temperature", "ready"}, Topic: "input.sensor.changed"},
		{Hash: "sensor:status", Fields: []string{"temperature"}},
		{Channel: "sensor:events", Topic: "input.sensor.raw"},
		{Channel: "sensor:events", Topic: "input.sensor.raw", Format: "string"},
		{Channel: "sensor:events", Topic: "input.sensor.json", Format: "json"},
		{Hash: strings.Repeat("h", 128), Fields: []string{strings.Repeat("f", 128)}, Topic: "input." + strings.Repeat("t", 122)},
		{Hash: "sensor", Fields: inputFields(MaxFields)},
	} {
		t.Run(fmt.Sprintf("%s/%s/%s", c.Hash, c.Channel, c.Format), func(t *testing.T) {
			if err := c.Validate(); err != nil {
				t.Fatal(err)
			}
			if c.Limit() != 4096 {
				t.Fatalf("default limit = %d", c.Limit())
			}
		})
	}
	for _, n := range []int{0, 1, MaxBytes} {
		c := Config{Hash: "sensor", Fields: []string{"value"}, MaxBytes: n}
		if err := c.Validate(); err != nil {
			t.Fatalf("limit %d: %v", n, err)
		}
		want := n
		if n == 0 {
			want = 4096
		}
		if c.Limit() != want {
			t.Fatalf("limit %d: got %d", n, c.Limit())
		}
	}
}

func TestConfigInvalid(t *testing.T) {
	tests := []struct {
		name string
		c    Config
	}{
		{"no source", Config{}},
		{"two sources", Config{Hash: "sensor", Channel: "events", Fields: []string{"value"}, Topic: "input.sensor"}},
		{"hash without fields", Config{Hash: "sensor"}},
		{"hash with format", Config{Hash: "sensor", Fields: []string{"value"}, Format: "string"}},
		{"too many fields", Config{Hash: "sensor", Fields: inputFields(MaxFields + 1)}},
		{"duplicate field", Config{Hash: "sensor", Fields: []string{"value", "value"}}},
		{"channel without topic", Config{Channel: "events"}},
		{"channel with fields", Config{Channel: "events", Topic: "input.sensor", Fields: []string{"value"}}},
		{"unknown format", Config{Channel: "events", Topic: "input.sensor", Format: "JSON"}},
	}
	for _, n := range []int{-1, MaxBytes + 1} {
		tests = append(tests, struct {
			name string
			c    Config
		}{fmt.Sprintf("limit %d", n), Config{Hash: "sensor", Fields: []string{"value"}, MaxBytes: n}})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Validate(); err == nil {
				t.Fatalf("accepted %+v", tt.c)
			}
		})
	}
}

func TestConfigRejectsInvalidNamesAndTopics(t *testing.T) {
	invalidNames := []string{"*", "a?", "a[0]", "a]", `a\b`, "a b", "a\t", "a\n", "a\x00", "a\u2003b", strings.Repeat("a", 129)}
	for _, name := range append(append([]string(nil), invalidNames...), "ev:events", "ev:", "__keyspace@0__:sensor", "__keyevent@0__:set", "__keycustom") {
		t.Run("source/"+name, func(t *testing.T) {
			for _, c := range []Config{{Hash: name, Fields: []string{"value"}}, {Channel: name, Topic: "input.sensor"}} {
				if err := c.Validate(); err == nil {
					t.Fatalf("accepted %+v", c)
				}
			}
		})
	}
	for _, field := range append(invalidNames, "") {
		t.Run("field/"+field, func(t *testing.T) {
			if err := (Config{Hash: "sensor", Fields: []string{field}}).Validate(); err == nil {
				t.Fatalf("accepted field %q", field)
			}
		})
	}
	for _, topic := range []string{"vehicle.state", "input", "input.", "input..sensor", "input.sensor.", "input.*", "input.?", "input.[a]", `input.a\b`, "input.a:b", "input.a b", "input.a\n", "input.a\u2003b", "input." + strings.Repeat("t", 123)} {
		t.Run("topic/"+topic, func(t *testing.T) {
			for _, c := range []Config{{Hash: "sensor", Fields: []string{"value"}, Topic: topic}, {Channel: "events", Topic: topic}} {
				if err := c.Validate(); err == nil {
					t.Fatalf("accepted topic %q", topic)
				}
			}
		})
	}
}

func TestValidateSetDistinctLimitAndNormalization(t *testing.T) {
	if err := ValidateSet(nil); err != nil {
		t.Fatal(err)
	}
	configs := make([]Config, MaxTotal)
	for i := range configs {
		configs[i] = Config{Channel: fmt.Sprintf("events:%d", i), Topic: "input.sensor"}
	}
	if err := ValidateSet(configs); err != nil {
		t.Fatalf("exact cap: %v", err)
	}
	duplicate := configs[0]
	duplicate.Format, duplicate.MaxBytes = "string", 4096
	if err := ValidateSet(append(configs, duplicate)); err != nil {
		t.Fatalf("normalized raw duplicate counted twice: %v", err)
	}
	distinct := configs[0]
	distinct.Topic = "input.other"
	if err := ValidateSet(append(configs, distinct)); err == nil {
		t.Fatal("accepted 65 distinct definitions")
	}
	distinct = configs[0]
	distinct.Format = "json"
	if err := ValidateSet(append(configs, distinct)); err == nil {
		t.Fatal("different format treated as duplicate")
	}
	distinct = configs[0]
	distinct.MaxBytes = 1
	if err := ValidateSet(append(configs, distinct)); err == nil {
		t.Fatal("different byte limit treated as duplicate")
	}
	configs[0] = Config{Hash: "sensor", Fields: []string{"z", "a"}}
	duplicate = Config{Hash: "sensor", Fields: []string{"a", "z"}, MaxBytes: 4096}
	if err := ValidateSet(append(configs, duplicate)); err != nil {
		t.Fatalf("normalized hash duplicate counted twice: %v", err)
	}
	if !reflect.DeepEqual(configs[0].Fields, []string{"z", "a"}) {
		t.Fatal("validation reordered caller fields")
	}
}

func TestValidateSetSelectedFieldUnion(t *testing.T) {
	fields := inputFields(MaxFields + 1)
	configs := []Config{{Hash: "sensor", Fields: fields[:32]}, {Hash: "sensor", Fields: fields[32:64]}, {Hash: "sensor", Fields: []string{fields[0]}, Topic: "input.sensor"}}
	if err := ValidateSet(configs); err != nil {
		t.Fatalf("64-field union: %v", err)
	}
	configs = append(configs, Config{Hash: "sensor", Fields: fields[64:]})
	if err := ValidateSet(configs); err == nil {
		t.Fatal("accepted 65 fields across definitions for one hash")
	}
	configs[len(configs)-1].Hash = "other"
	if err := ValidateSet(configs); err != nil {
		t.Fatalf("field cap should be per hash: %v", err)
	}
}

func TestValidateSetStateBudget(t *testing.T) {
	fields := inputFields(MaxStateBytes / MaxBytes)
	configs := []Config{{Hash: "sensor", Fields: fields, MaxBytes: MaxBytes}}
	if err := ValidateSet(configs); err != nil {
		t.Fatalf("exact 1 MiB budget: %v", err)
	}
	configs = append(configs, Config{Hash: "sensor", Fields: []string{fields[0]}, Topic: "input.sensor", MaxBytes: 1})
	if err := ValidateSet(configs); err != nil {
		t.Fatalf("shared field should count only largest limit: %v", err)
	}
	configs = append(configs, Config{Channel: "events", Topic: "input.events", MaxBytes: MaxBytes})
	if err := ValidateSet(configs); err != nil {
		t.Fatalf("raw channel must not consume selected state budget: %v", err)
	}
	configs = append(configs, Config{Hash: "other", Fields: []string{"value"}, MaxBytes: 1})
	if err := ValidateSet(configs); err == nil {
		t.Fatal("accepted 1 MiB + 1 selected bytes")
	}
	// The larger limit wins regardless of definition order.
	for _, reverse := range []bool{false, true} {
		cs := []Config{{Hash: "sensor", Fields: fields, MaxBytes: MaxBytes - 1}, {Hash: "sensor", Fields: fields, MaxBytes: MaxBytes}, {Hash: "other", Fields: []string{"value"}, MaxBytes: 1}}
		if reverse {
			cs[0], cs[1] = cs[1], cs[0]
		}
		if err := ValidateSet(cs); err == nil {
			t.Fatalf("larger shared-field limit lost (reverse=%t)", reverse)
		}
	}
}

func TestValidateSetRejectsInvalidAndConflictingKinds(t *testing.T) {
	if err := ValidateSet([]Config{{Channel: "events"}}); err == nil {
		t.Fatal("set did not validate members")
	}
	hash := Config{Hash: "sensor", Fields: []string{"value"}}
	raw := Config{Channel: "sensor", Topic: "input.sensor"}
	for _, cs := range [][]Config{{hash, raw}, {raw, hash}} {
		if err := ValidateSet(cs); err == nil {
			t.Fatal("accepted hash/raw interpretation conflict")
		}
	}
}
