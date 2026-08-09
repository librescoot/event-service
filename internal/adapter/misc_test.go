package adapter

import (
	"testing"

	"github.com/librescoot/eventbus"
)

func TestMiscAlarmStatus(t *testing.T) {
	m := NewMiscSource()
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
	m := NewMiscSource()
	for _, value := range []string{"delay-armed", "seatbox-access"} {
		got := m.OnField("alarm", "status", value, "armed")
		if len(got) != 1 || got[0].Topic != eventbus.TopicAlarmStatusChanged {
			t.Errorf("alarm status %s: got %v, want only [alarm.status.changed]", value, topics(got))
		}
	}
}

func TestMiscAlarmFirstObservationEmitsNothing(t *testing.T) {
	if got := NewMiscSource().OnField("alarm", "status", "armed", ""); len(got) != 0 {
		t.Errorf("got %v, want nothing on first observation", topics(got))
	}
}

func TestMiscGPSFixEdgesOnly(t *testing.T) {
	m := NewMiscSource()
	if got := m.OnField("gps", "fix", "3d", "none"); len(got) != 1 || got[0].Topic != eventbus.TopicGPSFixAcquired {
		t.Errorf("got %v, want [gps.fix.acquired]", topics(got))
	}
	if got := m.OnField("gps", "fix", "2d", "3d"); len(got) != 0 {
		t.Errorf("3d -> 2d is still a fix, got %v, want nothing", topics(got))
	}
	if got := m.OnField("gps", "fix", "none", "3d"); len(got) != 1 || got[0].Topic != eventbus.TopicGPSFixLost {
		t.Errorf("got %v, want [gps.fix.lost]", topics(got))
	}
}

func TestMiscECUFaultRaisedAndCleared(t *testing.T) {
	m := NewMiscSource()
	got := m.OnField("engine-ecu", "fault:code", "42", "0")
	if len(got) != 1 || got[0].Topic != eventbus.TopicECUFaultRaised {
		t.Fatalf("got %v, want [ecu.fault.raised]", topics(got))
	}
	if got[0].Data["code"] != 42 {
		t.Errorf("code = %v, want 42", got[0].Data["code"])
	}
	got = m.OnField("engine-ecu", "fault:code", "0", "42")
	if len(got) != 1 || got[0].Topic != eventbus.TopicECUFaultCleared {
		t.Errorf("got %v, want [ecu.fault.cleared]", topics(got))
	}
}

func TestMiscOTAStatusIsPerComponent(t *testing.T) {
	m := NewMiscSource()

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

func TestMiscMotionInterruptIsEdgeOnly(t *testing.T) {
	m := NewMiscSource()
	got := m.OnMessage("motion:interrupt", `{"type":"edge","timestamp":1,"engine":"any-motion"}`)
	if len(got) != 1 || got[0].Topic != eventbus.TopicMotionDetected {
		t.Errorf("got %v, want [motion.detected]", topics(got))
	}
}

func TestMiscDoesNotWatchHotHashes(t *testing.T) {
	for _, h := range NewMiscSource().Hashes() {
		if h == "motion" {
			t.Error("the motion hash updates at 10 Hz; watch motion:interrupt instead")
		}
	}
	for _, c := range NewMiscSource().Channels() {
		if c == "motion:sensors" || c == "motion:heading" || c == "gps:tpv" {
			t.Errorf("channel %q is a hot source and must not be subscribed", c)
		}
	}
}
