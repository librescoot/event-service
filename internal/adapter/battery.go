package adapter

import (
	"strconv"
	"strings"

	"github.com/librescoot/eventbus"
)

// BatterySource derives events from the main packs and the two auxiliary
// batteries.
//
// It holds no state: the shadow store already suppresses repeated writes of the
// same value, and the envelope carries from and to, so a rule that wants a
// threshold crossing writes a comparison rather than relying on the adapter to
// have guessed the right levels.
type BatterySource struct{}

// NewBatterySource returns a BatterySource.
func NewBatterySource() *BatterySource { return &BatterySource{} }

// Hashes implements Source.
func (b *BatterySource) Hashes() []string {
	return []string{"battery:0", "battery:1", "aux-battery", "cb-battery"}
}

// Channels implements Source.
func (b *BatterySource) Channels() []string { return nil }

// OnMessage implements Source.
func (b *BatterySource) OnMessage(channel, payload string) []eventbus.Event { return nil }

// OnField implements Source.
func (b *BatterySource) OnField(hash, field, value, prev string) []eventbus.Event {
	switch field {
	case "present":
		return b.presence(hash, value, prev)
	case "charge":
		return b.chargeChanged(hash, value, prev)
	case "state":
		if !strings.HasPrefix(hash, "battery:") || prev == "" {
			return nil
		}
		e := ev(eventbus.TopicBatteryStateChanged, prev, value)
		e.Data = map[string]any{"slot": slotOf(hash)}
		return []eventbus.Event{e}
	}
	return nil
}

func (b *BatterySource) presence(hash, value, prev string) []eventbus.Event {
	if prev == "" || !strings.HasPrefix(hash, "battery:") {
		return nil
	}
	topic := eventbus.TopicBatteryRemoved
	if value == "true" {
		topic = eventbus.TopicBatteryInserted
	}
	e := ev(topic, prev, value)
	e.Data = map[string]any{"slot": slotOf(hash)}
	return []eventbus.Event{e}
}

// chargeChanged emits on every real change. The shadow store has already
// dropped repeats, so reaching here means the reading actually moved.
//
// No thresholds and no hysteresis: those are policy, and policy belongs in the
// rule that cares. from and to are on the envelope, so a rule expresses a
// crossing as int(to) < 50 and int(from) >= 50, with cooldown for anything that
// flaps.
func (b *BatterySource) chargeChanged(hash, value, prev string) []eventbus.Event {
	if prev == "" {
		return nil
	}
	if _, err := strconv.Atoi(value); err != nil {
		return nil
	}

	var topic string
	data := map[string]any{}
	switch hash {
	case "aux-battery":
		topic = eventbus.TopicAuxChargeChanged
	case "cb-battery":
		topic = eventbus.TopicCBBChargeChanged
	default:
		topic = eventbus.TopicBatteryChargeChanged
		data["slot"] = slotOf(hash)
	}

	e := ev(topic, prev, value)
	if len(data) > 0 {
		e.Data = data
	}
	return []eventbus.Event{e}
}

// slotOf turns "battery:1" into 1. Anything unparseable becomes 0, which is
// the front slot and the only one wired on a stock scooter.
func slotOf(hash string) int {
	_, num, ok := strings.Cut(hash, ":")
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(num)
	if err != nil {
		return 0
	}
	return n
}
