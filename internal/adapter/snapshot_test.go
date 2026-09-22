package adapter

import (
	"testing"

	"github.com/librescoot/event-service/internal/inputs"
	"github.com/librescoot/event-service/internal/shadow"
	"github.com/librescoot/eventbus"
)

type snapshotEmitter func(eventbus.Event) error

func (f snapshotEmitter) Emit(e eventbus.Event) error { return f(e) }

func TestSelectedSnapshotInstallsStateBeforeEvents(t *testing.T) {
	sh := shadow.NewStore()
	var events []eventbus.Event
	em := snapshotEmitter(func(e eventbus.Event) error {
		if sh.Get("custom", "a") != "new" || sh.Get("custom", "b") != "new" || sh.Get("custom", "invalid") != "" {
			t.Fatal("event published before full snapshot/invalidation installed")
		}
		events = append(events, e)
		return nil
	})
	ad := New(nil, em, sh)
	source, err := NewConfiguredSource([]inputs.Config{{Hash: "custom", Fields: []string{"a", "b", "invalid"}, Topic: "input.custom"}})
	if err != nil {
		t.Fatal(err)
	}
	ad.Register(source)
	ad.setSeeding("custom", true)
	ad.dispatchSnapshot("custom", map[string]string{"a": "old", "b": "old", "invalid": "stale"}, nil)
	if len(events) != 0 {
		t.Fatal("initial snapshot generated events")
	}
	ad.setSeeding("custom", false)
	ad.dispatchSnapshot("custom", map[string]string{"a": "new", "b": "new"}, []string{"invalid"})
	if len(events) != 2 || events[0].Data["field"] != "a" || events[1].Data["field"] != "b" {
		t.Fatalf("events = %#v", events)
	}
	ad.dispatchSnapshot("custom", map[string]string{"a": "new", "b": "new"}, nil)
	if len(events) != 2 {
		t.Fatal("unchanged snapshot generated events")
	}
}

func TestGenericOnlyHashesUseSelectedFields(t *testing.T) {
	ad := New(nil, nil, shadow.NewStore())
	ad.Register(NewVehicleSource())
	source, err := NewConfiguredSource([]inputs.Config{
		{Hash: "custom", Fields: []string{"mode"}, MaxBytes: 32},
		{Hash: "vehicle", Fields: []string{"engine-power"}, Topic: "input.power"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ad.Register(source)
	if ad.selectedFields("custom")["mode"] != 32 {
		t.Fatal("generic-only hash did not select bounded field reads")
	}
	if ad.selectedFields("vehicle") != nil {
		t.Fatal("changed built-in hash watcher semantics")
	}
}
