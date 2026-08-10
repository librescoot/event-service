// Package stats surfaces the rule engine's counters into the datastore, so a
// CLI can report whether a rule set is dispatching, dropping or thrashing
// without the service having to say so out loud in the log.
package stats

import (
	"sync"
	"time"
)

// Hash is the datastore hash rule-engine counters are published to.
const Hash = "extensions"

// Logger is the small slice of logging this package needs.
type Logger interface {
	Printf(format string, v ...any)
}

// Hasher is the one datastore operation the publisher needs: a single field
// write. Depending on this instead of a whole client keeps the package
// testable and free of the datastore library.
type Hasher interface {
	HSet(key, field string, value any) error
}

// Publisher polls a snapshot on an interval and writes only the fields whose
// value changed since the last publish. Comparing in process, against a map
// this package keeps itself, rather than reading the datastore back to
// compare, is what keeps an idle scooter from writing anything at all: the
// ticker wakes, finds every field unchanged, and goes back to sleep without a
// round trip.
type Publisher struct {
	c        Hasher
	interval time.Duration
	log      Logger
	hash     string

	stop chan struct{}
	wg   sync.WaitGroup

	stopOnce sync.Once
}

// NewPublisher returns a Publisher over the standard extensions hash. Start
// launches it, separately, with the function that produces each snapshot.
func NewPublisher(c Hasher, interval time.Duration, log Logger) *Publisher {
	return newPublisherIn(c, interval, log, Hash)
}

// newPublisherIn is the same over a caller-chosen hash, which is how tests
// keep out of each other's way on a shared datastore.
func newPublisherIn(c Hasher, interval time.Duration, log Logger, hash string) *Publisher {
	return &Publisher{
		c:        c,
		interval: interval,
		log:      log,
		hash:     hash,
		stop:     make(chan struct{}),
	}
}

// Start launches the publisher's goroutine. It publishes once immediately,
// writing every field the snapshot returns, so a reader of the hash never
// finds it half populated or missing a field like version that will not
// change again for the life of the process. After that it wakes on the
// interval and writes only the fields whose value differs from what was last
// sent.
//
// snapshot is called from this goroutine alone, never from the caller of
// Start, so Start itself never blocks.
func (p *Publisher) Start(snapshot func() map[string]string) {
	p.wg.Add(1)
	go p.run(snapshot)
}

func (p *Publisher) run(snapshot func() map[string]string) {
	defer p.wg.Done()

	last := make(map[string]string)
	p.publish(snapshot(), last)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.publish(snapshot(), last)
		}
	}
}

// publish writes every field of current whose value is not what last
// recorded for it, and updates last to match once the write succeeds. A
// field whose write fails is left out of last, so the next tick tries it
// again instead of the failure going unnoticed until the value happens to
// change for some unrelated reason.
func (p *Publisher) publish(current, last map[string]string) {
	for field, value := range current {
		if prev, ok := last[field]; ok && prev == value {
			continue
		}
		if err := p.c.HSet(p.hash, field, value); err != nil {
			p.log.Printf("stats: %s: %v", field, err)
			continue
		}
		last[field] = value
	}
}

// Stop signals the publisher's goroutine to exit and waits for it to return,
// so a caller that has called Stop knows no further write is coming. It is
// safe to call more than once.
func (p *Publisher) Stop() {
	p.stopOnce.Do(func() { close(p.stop) })
	p.wg.Wait()
}
