// Package engine joins the bus to the rules: it matches each event, applies
// per-rule rate limiting, and hands the work to the pool.
package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/event-service/internal/seq"
	"github.com/librescoot/eventbus"
)

// Logger is the small slice of logging this package needs.
type Logger interface {
	Printf(format string, v ...any)
}

type bound struct {
	rule *rules.Rule
	seq  *seq.Sequence

	mu       sync.Mutex
	lastFire time.Time

	// cancelDebounce is the pending quiet-window timer, non-nil only while
	// one is running. pending is the event that timer will dispatch, the most
	// recent one seen, not the one that started the window.
	cancelDebounce func() bool
	pending        eventbus.Event
}

// Engine holds the compiled rules and their built sequences.
type Engine struct {
	bounds []*bound
	runner *seq.Runner
	sch    *sched.Scheduler
	log    Logger
}

// New builds a sequence for every rule. A rule whose steps cannot be built is
// reported and dropped; the others still run, for the same reason a bad file
// does not stop the good ones loading. sch parks the tail of any step that
// carries an after delay, and the pending timer behind any rule's debounce.
func New(rs []*rules.Rule, pool *action.Pool, sch *sched.Scheduler, c action.Pusher, log Logger) (*Engine, []error) {
	en := &Engine{runner: seq.NewRunner(pool, sch, log), sch: sch, log: log}
	var errs []error

	for _, r := range rs {
		s, err := seq.Build(r, c)
		if err != nil {
			errs = append(errs, fmt.Errorf("rule %q in %s: %w", r.Name, r.Source, err))
			continue
		}
		en.bounds = append(en.bounds, &bound{rule: r, seq: s})
	}
	return en, errs
}

// RuleCount is how many rules are live.
func (en *Engine) RuleCount() int { return len(en.bounds) }

// Patterns returns the PSUBSCRIBE patterns needed to see every topic any rule
// mentions. Subscribing to exactly what is needed keeps an idle rule set from
// waking the process on traffic nothing will match.
//
// A topic named only in cancel-on still needs a subscription. Leave it out and
// the cancelling event never reaches Handle, so the rule keeps its pending
// tail and the feature does nothing at all on a live scooter, while every test
// that calls Handle directly still passes.
func (en *Engine) Patterns() []string {
	seen := make(map[string]bool)
	var out []string
	add := func(topic string) {
		p := eventbus.ChannelPrefix + topic
		if seen[p] {
			return
		}
		seen[p] = true
		out = append(out, p)
	}

	for _, b := range en.bounds {
		for _, topic := range b.rule.On {
			add(topic)
		}
		for _, topic := range b.rule.CancelOn {
			add(topic)
		}
	}
	return out
}

// Handle matches e against every rule and dispatches those that fire. It runs
// on the subscriber goroutine, so it must not block: matching is cheap and
// Submit never waits.
//
// Cancelling comes first, and one event does both jobs: the disarm that stops
// a rule blinking the hazards is usually also what a second rule chirps the
// horn on. Doing it in this order also means a rule that lists a topic in both
// on and cancel-on is stopped before it is started again, rather than the
// other way round.
func (en *Engine) Handle(e eventbus.Event) {
	en.runner.CancelMatching(e.Topic)

	now := time.Now()
	for _, b := range en.bounds {
		ok, err := b.rule.Matches(e)
		if err != nil {
			en.log.Printf("rule %s: %v", b.rule.Name, err)
			continue
		}
		if !ok {
			continue
		}
		if b.rule.Debounce > 0 {
			en.debounce(b, e)
			continue
		}
		if !b.allow(now) {
			continue
		}
		en.runner.Fire(b.seq, e)
	}
}

// debounce holds e as the trigger's most recent event and restarts the quiet
// window. Cooldown is leading edge: it fires on the first event of a burst
// and ignores the rest. Debounce is the opposite, trailing edge: nothing
// dispatches while the source keeps re-firing, and once it has gone quiet for
// the full window the rule fires exactly once, carrying forward whichever
// event was most recent when the window ran out, not whichever one started
// it. If the rule also sets a cooldown, it is checked here, against the
// debounced dispatch itself, not against each event that only restarted the
// window; a suppressed event never reaches it.
func (en *Engine) debounce(b *bound, e eventbus.Event) {
	b.mu.Lock()
	b.pending = e
	if b.cancelDebounce != nil {
		b.cancelDebounce()
	}
	b.cancelDebounce = en.sch.At(b.rule.Debounce, func() {
		b.mu.Lock()
		ev := b.pending
		b.cancelDebounce = nil
		b.mu.Unlock()

		if !b.allow(time.Now()) {
			return
		}
		en.runner.Fire(b.seq, ev)
	})
	b.mu.Unlock()
}

// Stop abandons every in-flight sequence, refuses new ones, and drops any
// debounce timer still waiting out its quiet window: a pending debounce is
// exactly the kind of pending tail Stop exists to cut loose, the same as a
// pending step wait. Call it before stopping the pool, so a run cannot queue
// a step into a pool that is already shutting down.
func (en *Engine) Stop() {
	for _, b := range en.bounds {
		b.mu.Lock()
		if b.cancelDebounce != nil {
			b.cancelDebounce()
			b.cancelDebounce = nil
		}
		b.mu.Unlock()
	}
	en.runner.Stop()
}

// allow enforces the cooldown. A flapping source must not be able to fill the
// action queue, so this is checked before Submit rather than inside the action.
func (b *bound) allow(now time.Time) bool {
	if b.rule.Cooldown <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.lastFire.IsZero() && now.Sub(b.lastFire) < b.rule.Cooldown {
		return false
	}
	b.lastFire = now
	return true
}
