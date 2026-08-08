package adapter

import (
	"github.com/librescoot/eventbus"
)

// Emitter is the subset of eventbus.Publisher the adapter needs. Narrow so tests
// can collect events in a slice instead of standing up a datastore.
type Emitter interface {
	Emit(eventbus.Event) error
}

// Source derives events from one domain.
//
// Implementations are pure: they return the events a change implies and never
// publish, sleep, or touch the datastore. That keeps every derivation
// unit-testable and keeps all I/O in Adapter.
type Source interface {
	// Hashes lists the hashes this source needs field-level notifications for.
	// The adapter watches each one and calls OnField.
	Hashes() []string

	// Channels lists raw pub/sub channels this source needs. The adapter
	// subscribes and calls OnMessage with the payload verbatim.
	Channels() []string

	// OnField is called when a watched hash field changes. prev is the value
	// the shadow store held before, empty on the first observation.
	OnField(hash, field, value, prev string) []eventbus.Event

	// OnMessage is called for each message on a channel from Channels().
	OnMessage(channel, payload string) []eventbus.Event
}
