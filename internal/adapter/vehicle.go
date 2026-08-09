package adapter

import (
	"strings"

	"github.com/librescoot/eventbus"
)

// Vehicle states, verified against vehicle-service internal/types/state.go.
// Duplicated here deliberately: the adapter reads them off the wire, so it must
// not depend on vehicle-service's package just to name them.
//
// Only the states this file actually branches on are declared. The repo's lint
// config treats an unused constant as a finding, and the full set is documented
// in tech-reference rather than mirrored here. The others are stand-by's
// siblings hop-on, hop-on-learning, waiting-seatbox, shutting-down, updating,
// and the three waiting-hibernation-* variants, all reachable through
// vehicle.state.changed without needing a name here.
const (
	stateStandBy      = "stand-by"
	stateParked       = "parked"
	stateReadyToDrive = "ready-to-drive"

	// hibernationPrefix matches waiting-hibernation and its -advanced,
	// -seatbox and -confirm variants.
	hibernationPrefix = "waiting-hibernation"
)

// VehicleSource derives events from the vehicle hash.
type VehicleSource struct{}

// NewVehicleSource returns a VehicleSource.
func NewVehicleSource() *VehicleSource { return &VehicleSource{} }

// Hashes implements Source.
func (v *VehicleSource) Hashes() []string { return []string{"vehicle"} }

// Channels implements Source.
func (v *VehicleSource) Channels() []string { return nil }

// OnMessage implements Source. VehicleSource watches no raw channels.
func (v *VehicleSource) OnMessage(channel, payload string) []eventbus.Event { return nil }

// OnField implements Source.
func (v *VehicleSource) OnField(hash, field, value, prev string) []eventbus.Event {
	switch field {
	case "state":
		return v.stateChange(value, prev)
	case "seatbox:lock":
		return one(pick(value == "open",
			eventbus.TopicVehicleSeatboxOpened, eventbus.TopicVehicleSeatboxClosed), prev, value)
	case "kickstand":
		return one(pick(value == "up",
			eventbus.TopicVehicleKickstandUp, eventbus.TopicVehicleKickstandDown), prev, value)
	case "handlebar:lock-sensor":
		return one(pick(value == "locked",
			eventbus.TopicVehicleHandlebarLocked, eventbus.TopicVehicleHandlebarUnlocked), prev, value)
	case "blinker:switch":
		return one(eventbus.TopicVehicleBlinkerChanged, prev, value)
	}
	return nil
}

// stateChange emits the complete record for every transition, plus a named
// topic for each of the five transitions worth naming that apply.
//
// Locking while riding is both a ride ending and a lock happening, so a
// single hash transition can carry two names; that is intended, not a bug.
//
// A transition out of an unknown previous state is not a transition, it is the
// adapter learning where the vehicle already was, so it emits nothing.
func (v *VehicleSource) stateChange(to, from string) []eventbus.Event {
	if from == "" {
		return nil
	}

	out := []eventbus.Event{ev(eventbus.TopicVehicleStateChanged, from, to)}

	for _, named := range namedTransition(from, to) {
		out = append(out, ev(named, from, to))
	}
	return out
}

// namedTransition reports every name this transition deserves, not just the
// first match. ready-to-drive -> stand-by, for example, is both ride.ended
// and vehicle.locked.
func namedTransition(from, to string) []string {
	var names []string
	if to == stateParked && from == stateStandBy {
		names = append(names, eventbus.TopicVehicleUnlocked)
	}
	if to == stateStandBy {
		names = append(names, eventbus.TopicVehicleLocked)
	}
	if to == stateReadyToDrive {
		names = append(names, eventbus.TopicRideStarted)
	}
	if from == stateReadyToDrive {
		names = append(names, eventbus.TopicRideEnded)
	}
	// Guarded by !isHibernating(from) so climbing the waiting-hibernation ->
	// -seatbox -> -confirm ladder only fires once, on the step that enters
	// it. Movement within the ladder is still visible via
	// vehicle.state.changed.
	if isHibernating(to) && !isHibernating(from) {
		names = append(names, eventbus.TopicVehicleHibernating)
	}
	return names
}

func isHibernating(state string) bool {
	return strings.HasPrefix(state, hibernationPrefix)
}

func ev(topic, from, to string) eventbus.Event {
	e := eventbus.New(topic, "adapter")
	e.From = from
	e.To = to
	return e
}

func one(topic, from, to string) []eventbus.Event {
	return []eventbus.Event{ev(topic, from, to)}
}

func pick(cond bool, whenTrue, whenFalse string) string {
	if cond {
		return whenTrue
	}
	return whenFalse
}
