package adapter

import (
	"fmt"
	"log"
	"sync"

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

	// seeding marks, per hash, whether the initial HGETALL replay done by
	// StartWithSync is still in flight. It has to be per-hash rather than
	// one flag for the whole adapter: watchers start one at a time, and a
	// single flag would suppress live events on an already-started hash
	// while a later hash in the loop is still replaying.
	seedingMu sync.Mutex
	seeding   map[string]bool
}

// New returns an Adapter. client and emitter may be nil in tests that only
// exercise Subscriptions.
func New(client *ipc.Client, em Emitter, sh *shadow.Store) *Adapter {
	return &Adapter{client: client, emitter: em, shadow: sh, seeding: make(map[string]bool)}
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

// Start subscribes to every hash and channel the registered sources need.
//
// For each hash, HashWatcher.StartWithSync subscribes gated, does an HGETALL,
// and replays every existing field synchronously through the same catch-all
// used for live updates before it ungates the subscription. Start marks the
// hash as seeding for the duration of that call: dispatchField still records
// the replayed values in the shadow store (that is the only way to have a
// correct "from" later), but does not turn them into events, since a value
// that was already there before the process started is not a transition.
// Only fields observed after StartWithSync returns are live and get
// dispatched to sources, with "from" taken from the seeded value.
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

	for name := range hashes {
		if name != "" && channels[name] {
			return fmt.Errorf("adapter: %q is registered as both a hash and a channel", name)
		}
	}

	for h := range hashes {
		hash := h
		w := a.client.NewHashWatcher(hash)
		w.OnAny(func(field, value string) error {
			a.dispatchField(hash, field, value)
			return nil
		})
		a.setSeeding(hash, true)
		err := w.StartWithSync()
		a.setSeeding(hash, false)
		if err != nil {
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

func (a *Adapter) setSeeding(hash string, seeding bool) {
	a.seedingMu.Lock()
	defer a.seedingMu.Unlock()
	if seeding {
		a.seeding[hash] = true
	} else {
		delete(a.seeding, hash)
	}
}

func (a *Adapter) isSeeding(hash string) bool {
	a.seedingMu.Lock()
	defer a.seedingMu.Unlock()
	return a.seeding[hash]
}

func (a *Adapter) dispatchField(hash, field, value string) {
	prev, changed := a.shadow.Observe(hash, field, value)
	if !changed {
		return
	}
	if a.isSeeding(hash) {
		return
	}
	for _, s := range a.sources {
		if !contains(s.Hashes(), hash) {
			continue
		}
		a.emit(a.callOnField(s, hash, field, value, prev))
	}
}

func (a *Adapter) dispatchMessage(channel, payload string) {
	for _, s := range a.sources {
		if !contains(s.Channels(), channel) {
			continue
		}
		a.emit(a.callOnMessage(s, channel, payload))
	}
}

// callOnField calls s.OnField and recovers a panic in it. A bug in one
// source's derivation must not kill the watcher goroutine, which would take
// down every other source and, once the goroutine's panic reaches the
// runtime, the process. It is logged rather than surfaced as an event error,
// since there is no eventbus.Event to attach it to.
func (a *Adapter) callOnField(s Source, hash, field, value, prev string) (evs []eventbus.Event) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("recovered panic in OnField(hash=%s field=%s): %v", hash, field, r)
		}
	}()
	return s.OnField(hash, field, value, prev)
}

// callOnMessage is callOnField's counterpart for OnMessage.
func (a *Adapter) callOnMessage(s Source, channel, payload string) (evs []eventbus.Event) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("recovered panic in OnMessage(channel=%s): %v", channel, r)
		}
	}()
	return s.OnMessage(channel, payload)
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
