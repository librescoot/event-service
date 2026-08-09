package adapter

import (
	"strings"

	"github.com/librescoot/eventbus"
)

// validGestures are the gestures vehicle-service's detector emits. Anything
// else on the channel is either a new gesture we do not understand yet or a
// malformed message, and in both cases inventing a topic for it would be worse
// than dropping it.
var validGestures = map[string]bool{
	"press": true, "release": true, "tap": true,
	"long-tap": true, "hold": true, "double-tap": true,
}

// InputSource derives button events from the input-events channel.
//
// It deliberately ignores the buttons channel. That one carries raw edges, and
// horn and seatbox edges arrive on it twice from two different code paths, so
// counting anything off it is unsound. input-events emits each gesture once.
type InputSource struct{}

// NewInputSource returns an InputSource.
func NewInputSource() *InputSource { return &InputSource{} }

// Hashes implements Source. InputSource watches no hashes.
func (i *InputSource) Hashes() []string { return nil }

// Channels implements Source.
func (i *InputSource) Channels() []string { return []string{"input-events"} }

// OnField implements Source. InputSource watches no hashes.
func (i *InputSource) OnField(hash, field, value, prev string) []eventbus.Event { return nil }

// OnMessage implements Source. Payloads look like "horn:tap" or
// "brake:left:hold": everything up to the last colon is the source, the rest
// is the gesture.
func (i *InputSource) OnMessage(channel, payload string) []eventbus.Event {
	idx := strings.LastIndex(payload, ":")
	if idx <= 0 || idx == len(payload)-1 {
		return nil
	}
	source, gesture := payload[:idx], payload[idx+1:]
	if !validGestures[gesture] {
		return nil
	}
	if strings.Count(source, ":") > 1 {
		return nil
	}
	return []eventbus.Event{eventbus.New(eventbus.ButtonTopic(source, gesture), "adapter")}
}
