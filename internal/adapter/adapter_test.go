package adapter

import (
	"sort"
	"testing"

	"github.com/librescoot/event-service/internal/shadow"
	"github.com/librescoot/eventbus"
)

type fakeSource struct {
	hashes   []string
	channels []string
	fields   []eventbus.Event
	messages []eventbus.Event
}

func (f *fakeSource) Hashes() []string   { return f.hashes }
func (f *fakeSource) Channels() []string { return f.channels }
func (f *fakeSource) OnField(hash, field, value, prev string) []eventbus.Event {
	return f.fields
}
func (f *fakeSource) OnMessage(channel, payload string) []eventbus.Event {
	return f.messages
}

// panicSource simulates a bug in a derivation, to prove one source panicking
// does not take down the dispatch loop or the sources registered after it.
type panicSource struct {
	hashes   []string
	channels []string
}

func (p *panicSource) Hashes() []string   { return p.hashes }
func (p *panicSource) Channels() []string { return p.channels }
func (p *panicSource) OnField(hash, field, value, prev string) []eventbus.Event {
	panic("boom: OnField")
}
func (p *panicSource) OnMessage(channel, payload string) []eventbus.Event {
	panic("boom: OnMessage")
}

func TestSubscriptionsIsTheUnionOfSources(t *testing.T) {
	a := New(nil, nil, nil)
	a.Register(&fakeSource{hashes: []string{"vehicle"}, channels: []string{"input-events"}})
	a.Register(&fakeSource{hashes: []string{"battery:0", "vehicle"}})

	got := a.Subscriptions()
	sort.Strings(got)
	want := []string{"battery:0", "input-events", "vehicle"}
	if len(got) != len(want) {
		t.Fatalf("Subscriptions() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Subscriptions() = %v, want %v", got, want)
		}
	}
}

func TestSubscriptionsNeverContainsAWildcard(t *testing.T) {
	a := New(nil, nil, nil)
	a.Register(&fakeSource{hashes: []string{"vehicle"}})
	for _, s := range a.Subscriptions() {
		if s == "*" || s == "" {
			t.Errorf("Subscriptions() contains %q; the adapter must never subscribe broadly", s)
		}
	}
}

func TestDispatchFieldRecoversPanicAndOtherSourcesStillFire(t *testing.T) {
	em := &collectingEmitter{}
	a := New(nil, em, shadow.NewStore())
	a.Register(&panicSource{hashes: []string{"vehicle"}})
	a.Register(&fakeSource{hashes: []string{"vehicle"}, fields: []eventbus.Event{eventbus.New("survivor.field", "test")}})

	a.dispatchField("vehicle", "state", "parked")

	if got := em.count(); got != 1 {
		t.Fatalf("emitted %d events, want 1 from the source registered after the panicking one", got)
	}
}

func TestDispatchMessageRecoversPanicAndOtherSourcesStillFire(t *testing.T) {
	em := &collectingEmitter{}
	a := New(nil, em, shadow.NewStore())
	a.Register(&panicSource{channels: []string{"input-events"}})
	a.Register(&fakeSource{channels: []string{"input-events"}, messages: []eventbus.Event{eventbus.New("survivor.message", "test")}})

	a.dispatchMessage("input-events", "horn:tap")

	if got := em.count(); got != 1 {
		t.Fatalf("emitted %d events, want 1 from the source registered after the panicking one", got)
	}
}
