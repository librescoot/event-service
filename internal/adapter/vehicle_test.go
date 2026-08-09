package adapter

import (
	"testing"

	"github.com/librescoot/eventbus"
)

func topics(evs []eventbus.Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Topic
	}
	return out
}

func TestVehicleStateChangeAlwaysEmitsTheCompleteRecord(t *testing.T) {
	v := NewVehicleSource()
	evs := v.OnField("vehicle", "state", "waiting-seatbox", "parked")

	if len(evs) != 1 {
		t.Fatalf("topics = %v, want exactly the complete record", topics(evs))
	}
	e := evs[0]
	if e.Topic != eventbus.TopicVehicleStateChanged {
		t.Errorf("Topic = %q", e.Topic)
	}
	if e.From != "parked" || e.To != "waiting-seatbox" {
		t.Errorf("from/to = %q/%q, want parked/waiting-seatbox", e.From, e.To)
	}
	if e.Src != "adapter" {
		t.Errorf("Src = %q, want adapter", e.Src)
	}
}

func TestVehicleUnlockEmitsBothTopics(t *testing.T) {
	v := NewVehicleSource()
	got := topics(v.OnField("vehicle", "state", "parked", "stand-by"))

	want := map[string]bool{
		eventbus.TopicVehicleUnlocked:     false,
		eventbus.TopicVehicleStateChanged: false,
	}
	for _, tp := range got {
		if _, ok := want[tp]; !ok {
			t.Errorf("unexpected topic %q", tp)
			continue
		}
		want[tp] = true
	}
	for tp, seen := range want {
		if !seen {
			t.Errorf("missing topic %q (got %v)", tp, got)
		}
	}
}

func TestVehicleNamedTransitions(t *testing.T) {
	cases := []struct {
		from, to string
		want     string
	}{
		{"stand-by", "parked", eventbus.TopicVehicleUnlocked},
		{"parked", "stand-by", eventbus.TopicVehicleLocked},
		{"parked", "ready-to-drive", eventbus.TopicRideStarted},
		{"ready-to-drive", "parked", eventbus.TopicRideEnded},
		{"parked", "waiting-hibernation", eventbus.TopicVehicleHibernating},
	}
	for _, c := range cases {
		got := topics(NewVehicleSource().OnField("vehicle", "state", c.to, c.from))
		found := false
		for _, tp := range got {
			if tp == c.want {
				found = true
			}
		}
		if !found {
			t.Errorf("%s -> %s: got %v, want it to include %q", c.from, c.to, got, c.want)
		}
	}
}

func TestVehicleLockingWhileRidingEndsTheRideToo(t *testing.T) {
	got := topics(NewVehicleSource().OnField("vehicle", "state", "stand-by", "ready-to-drive"))

	want := map[string]bool{
		eventbus.TopicVehicleStateChanged: false,
		eventbus.TopicVehicleLocked:       false,
		eventbus.TopicRideEnded:           false,
	}
	for _, tp := range got {
		if _, ok := want[tp]; !ok {
			t.Errorf("unexpected topic %q", tp)
			continue
		}
		want[tp] = true
	}
	for tp, seen := range want {
		if !seen {
			t.Errorf("missing topic %q (got %v)", tp, got)
		}
	}
}

func TestVehicleParkingFromStandByDoesNotClaimARideEnded(t *testing.T) {
	got := topics(NewVehicleSource().OnField("vehicle", "state", "stand-by", "parked"))
	for _, tp := range got {
		if tp == eventbus.TopicRideEnded {
			t.Errorf("got %v, parked -> stand-by is not a ride ending", got)
		}
	}
	if len(got) != 2 || got[0] != eventbus.TopicVehicleStateChanged || got[1] != eventbus.TopicVehicleLocked {
		t.Errorf("got %v, want exactly [%s %s]", got, eventbus.TopicVehicleStateChanged, eventbus.TopicVehicleLocked)
	}
}

func TestVehicleHibernationLadderNamesOnlyTheEntry(t *testing.T) {
	v := NewVehicleSource()

	entry := topics(v.OnField("vehicle", "state", "waiting-hibernation", "parked"))
	if len(entry) != 2 || entry[0] != eventbus.TopicVehicleStateChanged || entry[1] != eventbus.TopicVehicleHibernating {
		t.Errorf("parked -> waiting-hibernation: got %v, want exactly [%s %s]",
			entry, eventbus.TopicVehicleStateChanged, eventbus.TopicVehicleHibernating)
	}

	movements := []struct {
		from, to string
	}{
		{"waiting-hibernation", "waiting-hibernation-confirm"},
		{"waiting-hibernation", "waiting-hibernation-seatbox"},
		{"waiting-hibernation-seatbox", "waiting-hibernation-confirm"},
	}
	for _, c := range movements {
		got := topics(v.OnField("vehicle", "state", c.to, c.from))
		if len(got) != 1 || got[0] != eventbus.TopicVehicleStateChanged {
			t.Errorf("%s -> %s: got %v, want only the complete record; the ladder was already entered", c.from, c.to, got)
		}
	}
}

func TestVehicleNoNamedTopicForUninterestingTransition(t *testing.T) {
	got := topics(NewVehicleSource().OnField("vehicle", "state", "hop-on", "parked"))
	if len(got) != 1 || got[0] != eventbus.TopicVehicleStateChanged {
		t.Errorf("got %v, want only the complete record", got)
	}
}

func TestVehicleFirstObservationEmitsNothing(t *testing.T) {
	got := NewVehicleSource().OnField("vehicle", "state", "parked", "")
	if len(got) != 0 {
		t.Errorf("got %v, want nothing; there is no transition from an unknown state", topics(got))
	}
}

func TestVehicleSeatboxAndKickstand(t *testing.T) {
	v := NewVehicleSource()
	cases := []struct {
		field, value, prev string
		want               string
	}{
		{"seatbox:lock", "open", "closed", eventbus.TopicVehicleSeatboxOpened},
		{"seatbox:lock", "closed", "open", eventbus.TopicVehicleSeatboxClosed},
		{"kickstand", "up", "down", eventbus.TopicVehicleKickstandUp},
		{"kickstand", "down", "up", eventbus.TopicVehicleKickstandDown},
		{"handlebar:lock-sensor", "locked", "unlocked", eventbus.TopicVehicleHandlebarLocked},
		{"blinker:switch", "left", "off", eventbus.TopicVehicleBlinkerChanged},
	}
	for _, c := range cases {
		got := topics(v.OnField("vehicle", c.field, c.value, c.prev))
		if len(got) != 1 || got[0] != c.want {
			t.Errorf("%s=%s (was %s): got %v, want [%s]", c.field, c.value, c.prev, got, c.want)
		}
	}
}

func TestVehicleSeatboxFirstObservationEmitsNothing(t *testing.T) {
	got := NewVehicleSource().OnField("vehicle", "seatbox:lock", "closed", "")
	if len(got) != 0 {
		t.Errorf("got %v, want nothing on first observation", topics(got))
	}
}

func TestVehicleKickstandFirstObservationEmitsNothing(t *testing.T) {
	got := NewVehicleSource().OnField("vehicle", "kickstand", "up", "")
	if len(got) != 0 {
		t.Errorf("got %v, want nothing on first observation", topics(got))
	}
}

func TestVehicleHandlebarLockFirstObservationEmitsNothing(t *testing.T) {
	got := NewVehicleSource().OnField("vehicle", "handlebar:lock-sensor", "locked", "")
	if len(got) != 0 {
		t.Errorf("got %v, want nothing on first observation", topics(got))
	}
}

func TestVehicleBlinkerFirstObservationEmitsNothing(t *testing.T) {
	got := NewVehicleSource().OnField("vehicle", "blinker:switch", "off", "")
	if len(got) != 0 {
		t.Errorf("got %v, want nothing on first observation", topics(got))
	}
}

func TestVehicleIgnoresUnrelatedFields(t *testing.T) {
	got := NewVehicleSource().OnField("vehicle", "auto-standby-deadline", "1780000000", "0")
	if len(got) != 0 {
		t.Errorf("got %v, want nothing", topics(got))
	}
}
