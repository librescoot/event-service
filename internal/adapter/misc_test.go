package adapter

import (
	"testing"

	"github.com/librescoot/eventbus"
)

func TestMiscAlarmStatus(t *testing.T) {
	m := NewMiscSource(nil)
	cases := []struct{ value, prev, want string }{
		{"armed", "disarmed", eventbus.TopicAlarmArmed},
		{"disarmed", "armed", eventbus.TopicAlarmDisarmed},
		{"disabled", "armed", eventbus.TopicAlarmDisarmed},
		{"level-1-triggered", "armed", eventbus.TopicAlarmTriggered},
		{"level-2-triggered", "level-1-triggered", eventbus.TopicAlarmTriggered},
	}
	for _, c := range cases {
		got := m.OnField("alarm", "status", c.value, c.prev)
		if len(got) != 2 || got[0].Topic != eventbus.TopicAlarmStatusChanged || got[1].Topic != c.want {
			t.Errorf("alarm status %s: got %v, want [alarm.status.changed %s]", c.value, topics(got), c.want)
		}
	}
}

func TestMiscAlarmUnnamedStatesEmitOnlyTheCompleteRecord(t *testing.T) {
	m := NewMiscSource(nil)
	for _, value := range []string{"delay-armed", "seatbox-access"} {
		got := m.OnField("alarm", "status", value, "armed")
		if len(got) != 1 || got[0].Topic != eventbus.TopicAlarmStatusChanged {
			t.Errorf("alarm status %s: got %v, want only [alarm.status.changed]", value, topics(got))
		}
	}
}

func TestMiscAlarmFirstObservationEmitsNothing(t *testing.T) {
	if got := NewMiscSource(nil).OnField("alarm", "status", "armed", ""); len(got) != 0 {
		t.Errorf("got %v, want nothing on first observation", topics(got))
	}
}

func TestMiscOTAStatusIsPerComponent(t *testing.T) {
	m := NewMiscSource(nil)

	// There is no bare "status" field on the ota hash.
	if got := m.OnField("ota", "status", "installing", "idle"); len(got) != 0 {
		t.Errorf("bare status field emitted %v, want nothing", topics(got))
	}

	got := m.OnField("ota", "status:mdb", "downloading", "idle")
	if len(got) != 1 || got[0].Topic != eventbus.TopicOTAStatusChanged {
		t.Fatalf("got %v, want [ota.status.changed]", topics(got))
	}
	if got[0].Data["component"] != "mdb" {
		t.Errorf("component = %v, want mdb", got[0].Data["component"])
	}

	got = m.OnField("ota", "status:dbc", "installing", "downloading")
	if len(got) != 1 || got[0].Data["component"] != "dbc" {
		t.Errorf("got %v with component %v, want [ota.status.changed] dbc", topics(got), got[0].Data["component"])
	}
}

type fakeLookup map[string]string

func (f fakeLookup) Get(hash, field string) string { return f[hash+"/"+field] }

func TestKeycardEventCarriesUIDFromShadow(t *testing.T) {
	sh := fakeLookup{
		"keycard/uid":  "04a1b2c3",
		"keycard/type": "scooter",
	}
	got := NewMiscSource(sh).OnField("keycard", "authentication", "passed", "")
	if len(got) != 1 {
		t.Fatalf("got %v, want one event", topics(got))
	}
	if got[0].Data["uid"] != "04a1b2c3" {
		t.Errorf("uid = %v, want 04a1b2c3", got[0].Data["uid"])
	}
	if got[0].Data["type"] != "scooter" {
		t.Errorf("type = %v, want scooter", got[0].Data["type"])
	}
}

func TestKeycardEventOmitsUnknownUID(t *testing.T) {
	got := NewMiscSource(fakeLookup{}).OnField("keycard", "authentication", "failed", "")
	if len(got) != 1 {
		t.Fatalf("got %v, want one event", topics(got))
	}
	if _, ok := got[0].Data["uid"]; ok {
		t.Error("uid present but unknown; omit rather than emitting an empty string")
	}
}

func TestMiscMotionInterruptIsEdgeOnly(t *testing.T) {
	m := NewMiscSource(nil)
	got := m.OnMessage("motion:interrupt", `{"type":"edge","timestamp":1,"engine":"any-motion"}`)
	if len(got) != 1 || got[0].Topic != eventbus.TopicMotionDetected {
		t.Errorf("got %v, want [motion.detected]", topics(got))
	}
}

func TestMiscDoesNotWatchHotHashes(t *testing.T) {
	for _, h := range NewMiscSource(nil).Hashes() {
		if h == "motion" {
			t.Error("the motion hash updates at 10 Hz; watch motion:interrupt instead")
		}
	}
	for _, c := range NewMiscSource(nil).Channels() {
		if c == "motion:sensors" || c == "motion:heading" || c == "gps:tpv" {
			t.Errorf("channel %q is a hot source and must not be subscribed", c)
		}
	}
}
