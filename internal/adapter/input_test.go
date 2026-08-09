package adapter

import (
	"testing"
)

func TestInputParsesTwoPartSource(t *testing.T) {
	got := NewInputSource().OnMessage("input-events", "horn:tap")
	if len(got) != 1 {
		t.Fatalf("got %v, want one event", topics(got))
	}
	if got[0].Topic != "button.horn.tap" {
		t.Errorf("Topic = %q, want button.horn.tap", got[0].Topic)
	}
	if got[0].Src != "adapter" {
		t.Errorf("Src = %q", got[0].Src)
	}
}

func TestInputParsesThreePartSource(t *testing.T) {
	got := NewInputSource().OnMessage("input-events", "brake:left:hold")
	if len(got) != 1 || got[0].Topic != "button.brake.left.hold" {
		t.Fatalf("got %v, want [button.brake.left.hold]", topics(got))
	}
}

func TestInputAllGestures(t *testing.T) {
	for _, g := range []string{"press", "release", "tap", "long-tap", "hold", "double-tap"} {
		got := NewInputSource().OnMessage("input-events", "seatbox:"+g)
		want := "button.seatbox." + g
		if len(got) != 1 || got[0].Topic != want {
			t.Errorf("gesture %q: got %v, want [%s]", g, topics(got), want)
		}
	}
}

func TestInputRejectsMalformed(t *testing.T) {
	for _, payload := range []string{"", "horn", "horn:", ":tap", "a:b:c:d", ":brake:left:hold", "a:b:c:tap"} {
		if got := NewInputSource().OnMessage("input-events", payload); len(got) != 0 {
			t.Errorf("payload %q: got %v, want nothing", payload, topics(got))
		}
	}
}

func TestInputRejectsUnknownGesture(t *testing.T) {
	if got := NewInputSource().OnMessage("input-events", "horn:wiggle"); len(got) != 0 {
		t.Errorf("got %v, want nothing for an unknown gesture", topics(got))
	}
}

func TestInputSubscribesToInputEventsNotButtons(t *testing.T) {
	s := NewInputSource()
	ch := s.Channels()
	if len(ch) != 1 || ch[0] != "input-events" {
		t.Fatalf("Channels() = %v, want [input-events]", ch)
	}
	for _, c := range ch {
		if c == "buttons" {
			t.Error("the buttons channel duplicates horn and seatbox edges; use input-events")
		}
	}
	if len(s.Hashes()) != 0 {
		t.Errorf("Hashes() = %v, want none", s.Hashes())
	}
}
