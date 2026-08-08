package adapter

import (
	"sort"
	"testing"

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
