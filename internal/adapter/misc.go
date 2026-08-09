package adapter

import (
	"strconv"
	"strings"

	"github.com/librescoot/eventbus"
)

// MiscSource covers the domains that need one or two derivations each and do
// not justify their own file.
type MiscSource struct{}

// NewMiscSource returns a MiscSource.
func NewMiscSource() *MiscSource { return &MiscSource{} }

// Hashes implements Source.
//
// The motion hash is deliberately absent: it carries 10 Hz sensor state. The
// motion:interrupt channel gives the edges, which is all a rule wants.
func (m *MiscSource) Hashes() []string {
	return []string{
		"power-manager", "alarm", "gps", "internet",
		"ota", "keycard", "dashboard", "engine-ecu",
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
	case hash == "gps" && field == "fix":
		return m.gpsFix(value, prev)
	case hash == "engine-ecu" && field == "fault:code":
		return m.ecuFault(value, prev)
	case hash == "power-manager" && field == "state":
		if prev == "" {
			return nil
		}
		return one(eventbus.TopicPowerStateChanged, prev, value)
	case hash == "power-manager" && field == "wakeup-source":
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

func (m *MiscSource) alarm(value, prev string) []eventbus.Event {
	switch {
	case value == "armed":
		return one(eventbus.TopicAlarmArmed, prev, value)
	case value == "disarmed" || value == "disabled":
		return one(eventbus.TopicAlarmDisarmed, prev, value)
	case strings.HasSuffix(value, "-triggered"):
		e := ev(eventbus.TopicAlarmTriggered, prev, value)
		e.Data = map[string]any{"level": alarmLevel(value)}
		return []eventbus.Event{e}
	}
	return nil
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

// gpsFix fires only on the edge between having a fix and not having one.
// Moving between 2d and 3d is not something a rule cares about, and emitting
// it would add noise every time the sky opened up.
func (m *MiscSource) gpsFix(value, prev string) []eventbus.Event {
	had := prev != "" && prev != "none"
	has := value != "" && value != "none"
	switch {
	case !had && has:
		return one(eventbus.TopicGPSFixAcquired, prev, value)
	case had && !has:
		return one(eventbus.TopicGPSFixLost, prev, value)
	}
	return nil
}

// ecuFault reports the fault edge. ecu-service writes fault:code as a decimal
// uint32 (SetFault in ipc_tx.go), and only when it changes, so the field is
// absent on a vehicle that has never faulted. First appearance therefore has
// prev == "", which Atoi treats as 0, giving a correct "raised".
func (m *MiscSource) ecuFault(value, prev string) []eventbus.Event {
	code, err := strconv.Atoi(value)
	if err != nil {
		return nil
	}
	prevCode, _ := strconv.Atoi(prev)

	switch {
	case code != 0 && prevCode == 0:
		e := ev(eventbus.TopicECUFaultRaised, prev, value)
		e.Data = map[string]any{"code": code}
		return []eventbus.Event{e}
	case code == 0 && prevCode != 0:
		e := ev(eventbus.TopicECUFaultCleared, prev, value)
		e.Data = map[string]any{"code": prevCode}
		return []eventbus.Event{e}
	}
	return nil
}

func (m *MiscSource) keycard(value, prev string) []eventbus.Event {
	switch value {
	case "passed":
		return one(eventbus.TopicKeycardAuthPassed, prev, value)
	case "failed":
		return one(eventbus.TopicKeycardAuthFailed, prev, value)
	}
	return nil
}
