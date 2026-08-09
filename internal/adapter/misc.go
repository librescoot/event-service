package adapter

import (
	"strings"

	"github.com/librescoot/eventbus"
	ipc "github.com/librescoot/redis-ipc"
)

// Lookup reads a single hash field. Narrow so tests can pass a map.
//
// The production implementation reads the datastore live. It deliberately does
// NOT read the shadow store: keycard-service writes uid and type with
// SetManyPublishOne, which notifies only on "authentication", so those fields
// never reach the shadow store and reading them from there would return a
// stale UID from an earlier tap.
type Lookup interface {
	Get(hash, field string) string
}

// LiveLookup reads fields straight from the datastore.
//
// Safe for the keycard case because SetManyPublishOne pipelines the field
// writes ahead of the single publish, so every field is committed before the
// notification that triggers this read.
type LiveLookup struct {
	client *ipc.Client
}

// NewLiveLookup returns a Lookup backed by the datastore.
func NewLiveLookup(c *ipc.Client) *LiveLookup { return &LiveLookup{client: c} }

// Get returns the field value, or empty if it is missing or unreadable. A
// failed read is indistinguishable from an absent field here, which is what
// the caller wants: it omits the key either way rather than emitting a blank.
func (l *LiveLookup) Get(hash, field string) string {
	v, err := l.client.HGet(hash, field)
	if err != nil {
		return ""
	}
	return v
}

// MiscSource covers the domains that need one or two derivations each and do
// not justify their own file.
type MiscSource struct {
	look Lookup
}

// NewMiscSource returns a MiscSource that reads companion fields through look.
func NewMiscSource(look Lookup) *MiscSource { return &MiscSource{look: look} }

// Hashes implements Source.
//
// The motion hash is deliberately absent: it carries 10 Hz sensor state. The
// motion:interrupt channel gives the edges, which is all a rule wants.
func (m *MiscSource) Hashes() []string {
	return []string{
		"power-manager", "alarm", "internet",
		"ota", "keycard", "dashboard",
	}
}

// Channels implements Source. Hot channels (motion:sensors, motion:heading,
// gps:tpv) are never subscribed.
func (m *MiscSource) Channels() []string {
	return []string{"motion:interrupt", "sms:received"}
}

// OnField implements Source.
func (m *MiscSource) OnField(hash, field, value, prev string) []eventbus.Event {
	switch {
	case hash == "alarm" && field == "status":
		return m.alarm(value, prev)
	case hash == "power-manager" && field == "state":
		if prev == "" {
			return nil
		}
		return one(eventbus.TopicPowerStateChanged, prev, value)
	case hash == "power-manager" && field == "wakeup-source":
		if prev == "" {
			return nil
		}
		e := ev(eventbus.TopicPowerWake, prev, value)
		e.Data = map[string]any{"source": value}
		return []eventbus.Event{e}
	case hash == "internet" && field == "connectivity":
		if prev == "" {
			return nil
		}
		return one(eventbus.TopicNetConnectivityChanged, prev, value)
	case hash == "ota" && strings.HasPrefix(field, "status:"):
		// There is no bare "status" field. update-service writes one per
		// component, "status:mdb" and "status:dbc", from
		// internal/updater/updater.go. Verified absent on a live vehicle:
		// hkeys ota returns status:mdb only, with no status.
		if prev == "" {
			return nil
		}
		e := ev(eventbus.TopicOTAStatusChanged, prev, value)
		e.Data = map[string]any{"component": strings.TrimPrefix(field, "status:")}
		return []eventbus.Event{e}
	case hash == "keycard" && field == "authentication":
		return m.keycard(value, prev)
	case hash == "dashboard" && field == "ready":
		if value != "true" {
			return nil
		}
		return one(eventbus.TopicDashboardReady, prev, value)
	}
	return nil
}

// OnMessage implements Source.
func (m *MiscSource) OnMessage(channel, payload string) []eventbus.Event {
	switch channel {
	case "motion:interrupt":
		e := eventbus.New(eventbus.TopicMotionDetected, "adapter")
		e.Data = map[string]any{"raw": payload}
		return []eventbus.Event{e}
	case "sms:received":
		e := eventbus.New(eventbus.TopicSMSReceived, "adapter")
		e.Data = map[string]any{"raw": payload}
		return []eventbus.Event{e}
	}
	return nil
}

// alarm always emits the complete record when there is a known previous
// value, then appends a named topic for the three groups a rule is likely to
// key on. delay-armed, seatbox-access and unknown carry no name of their own;
// a rule that wants them reads alarm.status.changed directly.
func (m *MiscSource) alarm(value, prev string) []eventbus.Event {
	if prev == "" {
		return nil
	}

	out := []eventbus.Event{ev(eventbus.TopicAlarmStatusChanged, prev, value)}

	switch {
	case value == "armed":
		out = append(out, ev(eventbus.TopicAlarmArmed, prev, value))
	case value == "disarmed" || value == "disabled":
		out = append(out, ev(eventbus.TopicAlarmDisarmed, prev, value))
	case strings.HasSuffix(value, "-triggered"):
		e := ev(eventbus.TopicAlarmTriggered, prev, value)
		e.Data = map[string]any{"level": alarmLevel(value)}
		out = append(out, e)
	}
	return out
}

func alarmLevel(status string) int {
	switch status {
	case "level-1-triggered":
		return 1
	case "level-2-triggered":
		return 2
	}
	return 0
}

func (m *MiscSource) keycard(value, prev string) []eventbus.Event {
	var topic string
	switch value {
	case "passed":
		topic = eventbus.TopicKeycardAuthPassed
	case "failed":
		topic = eventbus.TopicKeycardAuthFailed
	default:
		return nil
	}

	e := ev(topic, prev, value)
	e.Data = map[string]any{}
	// Omitting beats emitting an empty string, which a rule cannot
	// distinguish from a real blank UID.
	if m.look != nil {
		if uid := m.look.Get("keycard", "uid"); uid != "" {
			e.Data["uid"] = uid
		}
		if typ := m.look.Get("keycard", "type"); typ != "" {
			e.Data["type"] = typ
		}
	}
	if len(e.Data) == 0 {
		e.Data = nil
	}
	return []eventbus.Event{e}
}
