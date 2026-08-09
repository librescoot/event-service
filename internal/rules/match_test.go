package rules

import "testing"

func TestMatchTopicExact(t *testing.T) {
	if !MatchTopic("alarm.triggered", "alarm.triggered") {
		t.Error("exact topic should match itself")
	}
	if MatchTopic("alarm.triggered", "alarm.armed") {
		t.Error("different topics must not match")
	}
}

func TestMatchTopicTrailingGlob(t *testing.T) {
	cases := map[string]bool{
		"battery.inserted":      true,
		"battery.charge.changed": true,
		"battery":               false, // the prefix alone is not a child
		"batteryx.inserted":     false, // must break on a dot, not a prefix
		"vehicle.unlocked":      false,
	}
	for topic, want := range cases {
		if got := MatchTopic("battery.*", topic); got != want {
			t.Errorf("MatchTopic(battery.*, %q) = %v, want %v", topic, got, want)
		}
	}
}

func TestMatchTopicBareStarMatchesEverything(t *testing.T) {
	for _, topic := range []string{"a", "a.b", "vehicle.state.changed"} {
		if !MatchTopic("*", topic) {
			t.Errorf("bare * should match %q", topic)
		}
	}
}

func TestMatchTopicMidPatternStarIsNotSupported(t *testing.T) {
	// Only a trailing .* is a glob. Anything else is treated literally, so a
	// user typo fails closed rather than matching unexpectedly broadly.
	if MatchTopic("battery.*.changed", "battery.charge.changed") {
		t.Error("mid-pattern glob must not match; only a trailing .* is a glob")
	}
}

func TestMatchTopicEmptyPatternMatchesNothing(t *testing.T) {
	if MatchTopic("", "anything") {
		t.Error("an empty pattern must not match")
	}
}
