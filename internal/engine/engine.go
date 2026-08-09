// Package engine joins the bus to the rules: it matches each event, applies
// per-rule rate limiting, and hands the work to the pool.
package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/eventbus"
)

// Logger is the small slice of logging this package needs.
type Logger interface {
	Printf(format string, v ...any)
}

type bound struct {
	rule   *rules.Rule
	action action.Action

	mu       sync.Mutex
	lastFire time.Time
}

// Engine holds the compiled rules and their built actions.
type Engine struct {
	bounds []*bound
	pool   *action.Pool
	log    Logger
}

// New builds an action for every rule. A rule whose action cannot be built is
// reported and dropped; the others still run, for the same reason a bad file
// does not stop the good ones loading.
func New(rs []*rules.Rule, pool *action.Pool, c action.Pusher, log Logger) (*Engine, []error) {
	en := &Engine{pool: pool, log: log}
	var errs []error

	for _, r := range rs {
		if r.Debounce > 0 {
			errs = append(errs, fmt.Errorf("rule %q: debounce is not supported yet", r.Name))
			continue
		}
		a, err := action.Build(action.Spec{
			Do:      r.Step.Do,
			List:    r.Step.List,
			Push:    r.Step.Push,
			Command: r.Step.Command,
			Timeout: r.Step.Timeout,
		}, c)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		en.bounds = append(en.bounds, &bound{rule: r, action: a})
	}
	return en, errs
}

// RuleCount is how many rules are live.
func (en *Engine) RuleCount() int { return len(en.bounds) }

// Patterns returns the PSUBSCRIBE patterns needed to see every topic any rule
// mentions. Subscribing to exactly what is needed keeps an idle rule set from
// waking the process on traffic nothing will match.
func (en *Engine) Patterns() []string {
	seen := make(map[string]bool)
	var out []string
	for _, b := range en.bounds {
		for _, topic := range b.rule.On {
			p := eventbus.ChannelPrefix + topic
			if seen[p] {
				continue
			}
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

// Handle matches e against every rule and dispatches those that fire. It runs
// on the subscriber goroutine, so it must not block: matching is cheap and
// Submit never waits.
func (en *Engine) Handle(e eventbus.Event) {
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
		if !b.allow(now) {
			continue
		}
		en.pool.Submit(b.action, e, b.rule.Name)
	}
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
