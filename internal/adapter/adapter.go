package adapter

import (
	"fmt"
	"log"

	"github.com/librescoot/event-service/internal/shadow"
	"github.com/librescoot/eventbus"
	ipc "github.com/librescoot/redis-ipc"
)

// Adapter turns existing datastore traffic into bus events.
//
// It subscribes only to what its registered sources ask for. Subscribing
// broadly would pull in the hot channels (motion:sensors at 10 Hz,
// motion:heading at 5 Hz, gps:tpv, cb-battery), which together carry several
// messages a second of traffic no rule wants, on a single-core box that is
// already short of headroom.
type Adapter struct {
	client   *ipc.Client
	emitter  Emitter
	shadow   *shadow.Store
	sources  []Source
	watchers []*ipc.HashWatcher
	subs     []*ipc.Subscription[string]
}

// New returns an Adapter. client and emitter may be nil in tests that only
// exercise Subscriptions.
func New(client *ipc.Client, em Emitter, sh *shadow.Store) *Adapter {
	return &Adapter{client: client, emitter: em, shadow: sh}
}

// Register adds a source. Call before Start.
func (a *Adapter) Register(s Source) {
	a.sources = append(a.sources, s)
}

// Subscriptions returns every hash and channel the registered sources need,
// deduplicated. Two sources watching the same hash cost one watcher.
func (a *Adapter) Subscriptions() []string {
	seen := make(map[string]bool)
	var out []string
	add := func(names []string) {
		for _, n := range names {
			if n == "" || seen[n] {
				continue
			}
			seen[n] = true
			out = append(out, n)
		}
	}
	for _, s := range a.sources {
		add(s.Hashes())
		add(s.Channels())
	}
	return out
}

// Start seeds the shadow store, then subscribes.
//
// Seeding first means the first notification after startup produces a correct
// "from" rather than an empty one. redis-ipc multiplexes every watcher onto
// one shared connection, so the count here does not translate into connections.
func (a *Adapter) Start() error {
	hashes := make(map[string]bool)
	channels := make(map[string]bool)
	for _, s := range a.sources {
		for _, h := range s.Hashes() {
			hashes[h] = true
		}
		for _, c := range s.Channels() {
			channels[c] = true
		}
	}

	for h := range hashes {
		hash := h
		w := a.client.NewHashWatcher(hash)
		w.OnAny(func(field, value string) error {
			a.dispatchField(hash, field, value)
			return nil
		})
		if err := w.StartWithSync(); err != nil {
			return fmt.Errorf("watch hash %s: %w", hash, err)
		}
		a.watchers = append(a.watchers, w)
	}

	for c := range channels {
		ch := c
		sub, err := ipc.Subscribe(a.client, ch, func(payload string) error {
			a.dispatchMessage(ch, payload)
			return nil
		})
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", ch, err)
		}
		a.subs = append(a.subs, sub)
	}

	log.Printf("adapter watching %d hashes and %d channels", len(hashes), len(channels))
	return nil
}

// Stop releases every watcher and subscription. Errors are not actionable
// during shutdown, so they are discarded rather than surfaced.
func (a *Adapter) Stop() {
	for _, w := range a.watchers {
		_ = w.Stop()
	}
	for _, s := range a.subs {
		_ = s.Unsubscribe()
	}
}

func (a *Adapter) dispatchField(hash, field, value string) {
	prev, changed := a.shadow.Observe(hash, field, value)
	if !changed {
		return
	}
	for _, s := range a.sources {
		if !contains(s.Hashes(), hash) {
			continue
		}
		a.emit(s.OnField(hash, field, value, prev))
	}
}

func (a *Adapter) dispatchMessage(channel, payload string) {
	for _, s := range a.sources {
		if !contains(s.Channels(), channel) {
			continue
		}
		a.emit(s.OnMessage(channel, payload))
	}
}

// emit publishes derived events. A failure to publish one event must not stop
// the others or kill the watcher goroutine, so errors are logged and skipped.
func (a *Adapter) emit(evs []eventbus.Event) {
	for _, e := range evs {
		if err := a.emitter.Emit(e); err != nil {
			log.Printf("emit %s: %v", e.Topic, err)
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
