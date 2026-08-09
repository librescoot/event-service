package adapter

import (
	"testing"

	"github.com/librescoot/eventbus"
)

func TestBatteryPresenceEmitsInsertRemoveWithSlot(t *testing.T) {
	b := NewBatterySource()

	got := b.OnField("battery:0", "present", "true", "false")
	if len(got) != 1 || got[0].Topic != eventbus.TopicBatteryInserted {
		t.Fatalf("got %v, want [battery.inserted]", topics(got))
	}
	if got[0].Data["slot"] != 0 {
		t.Errorf("slot = %v, want 0", got[0].Data["slot"])
	}

	got = b.OnField("battery:1", "present", "false", "true")
	if len(got) != 1 || got[0].Topic != eventbus.TopicBatteryRemoved {
		t.Fatalf("got %v, want [battery.removed]", topics(got))
	}
	if got[0].Data["slot"] != 1 {
		t.Errorf("slot = %v, want 1", got[0].Data["slot"])
	}
}

func TestBatteryFirstPresenceObservationEmitsNothing(t *testing.T) {
	if got := NewBatterySource().OnField("battery:0", "present", "true", ""); len(got) != 0 {
		t.Errorf("got %v, want nothing on first observation", topics(got))
	}
}

func TestBatteryChargeChangedCarriesBothReadings(t *testing.T) {
	got := NewBatterySource().OnField("battery:0", "charge", "49", "50")
	if len(got) != 1 || got[0].Topic != eventbus.TopicBatteryChargeChanged {
		t.Fatalf("got %v, want [battery.charge.changed]", topics(got))
	}
	if got[0].From != "50" || got[0].To != "49" {
		t.Errorf("from/to = %q/%q, want 50/49", got[0].From, got[0].To)
	}
	if got[0].Data["slot"] != 0 {
		t.Errorf("slot = %v, want 0", got[0].Data["slot"])
	}
}

func TestBatteryChargeFirstObservationEmitsNothing(t *testing.T) {
	if got := NewBatterySource().OnField("battery:0", "charge", "80", ""); len(got) != 0 {
		t.Errorf("got %v, want nothing; there is no change from an unknown value", topics(got))
	}
}

func TestBatteryIgnoresNonNumericCharge(t *testing.T) {
	if got := NewBatterySource().OnField("battery:0", "charge", "", "80"); len(got) != 0 {
		t.Errorf("got %v, want nothing for a non-numeric charge", topics(got))
	}
}

func TestAuxAndCBBUseTheirOwnTopics(t *testing.T) {
	b := NewBatterySource()

	got := b.OnField("aux-battery", "charge", "20", "25")
	if len(got) != 1 || got[0].Topic != eventbus.TopicAuxChargeChanged {
		t.Errorf("got %v, want [aux.charge.changed]", topics(got))
	}
	if _, ok := got[0].Data["slot"]; ok {
		t.Error("aux has no slot; the field should be absent")
	}

	got = b.OnField("cb-battery", "charge", "99", "100")
	if len(got) != 1 || got[0].Topic != eventbus.TopicCBBChargeChanged {
		t.Errorf("got %v, want [cbb.charge.changed]", topics(got))
	}
}

func TestBatteryVoltageAndCurrentAreNotEmitted(t *testing.T) {
	// Measured on deep-blue: aux-battery[voltage] changed 54 times and
	// cb-battery[current] 83 times in 100 seconds while parked. Both are
	// continuous analogue fields and belong with the hot sources.
	b := NewBatterySource()
	for _, f := range []string{"voltage", "current", "temperature:0"} {
		if got := b.OnField("aux-battery", f, "11700", "11680"); len(got) != 0 {
			t.Errorf("field %q emitted %v, want nothing", f, topics(got))
		}
	}
}
