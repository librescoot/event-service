// Package action runs what a rule decided should happen.
//
// Everything here executes on a bounded worker pool. The pool is the isolation
// boundary between user-supplied behaviour and the rest of the service: a
// script that hangs occupies a worker and nothing else.
package action

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/librescoot/eventbus"
)

// Action is one thing a rule can do.
type Action interface {
	Do(ctx context.Context, e eventbus.Event) error
	Kind() string
}

// actionFunc adapts a plain function to Action, for tests and trivial cases.
type actionFunc func(ctx context.Context, e eventbus.Event) error

func (f actionFunc) Do(ctx context.Context, e eventbus.Event) error { return f(ctx, e) }
func (actionFunc) Kind() string                                     { return "func" }

// Logger is the small slice of logging the pool needs, so this package does
// not care which logger the service uses.
type Logger interface {
	Printf(format string, v ...any)
}

// Stats are cumulative counters, surfaced so a stalled or thrashing rule set
// is visible rather than silent.
type Stats struct {
	Dispatched uint64
	Dropped    uint64
	Failed     uint64
}

type job struct {
	action Action
	event  eventbus.Event
	rule   string
}

// Pool runs actions on a fixed number of workers behind a bounded queue.
//
// jobs is never closed. Closing it would make a concurrent or post-Stop
// Submit panic: a send to a closed channel panics unconditionally in Go,
// and the select/default in Submit only guards against a full channel, not
// a closed one. Shutdown is signalled instead through done, which only
// ever transitions from open to closed and is safe to select on and read
// from any number of goroutines at once.
type Pool struct {
	jobs    chan job
	done    chan struct{}
	workers int
	log     Logger
	wg      sync.WaitGroup

	// runCtx is passed to every Action.Do call and is canceled by Stop. This
	// is what lets Stop reach into a running exec action's own
	// context.WithTimeout and cut it short instead of waiting the timeout
	// out: canceling a parent context cancels every context derived from it.
	runCtx    context.Context
	runCancel context.CancelFunc

	dispatched atomic.Uint64
	dropped    atomic.Uint64
	failed     atomic.Uint64

	stopOnce sync.Once
}

// NewPool returns a pool with the given worker count and queue depth. Two
// workers is the default on the MDB: the box has one core, and more workers
// would mostly add contention. Worker count and queue depth are independent:
// a small number of workers can sit behind a much deeper queue.
func NewPool(workers, queue int, log Logger) *Pool {
	if workers < 1 {
		workers = 1
	}
	if queue < 1 {
		queue = 1
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	return &Pool{
		jobs:      make(chan job, queue),
		done:      make(chan struct{}),
		workers:   workers,
		log:       log,
		runCtx:    runCtx,
		runCancel: runCancel,
	}
}

// Start launches the workers.
func (p *Pool) Start() {
	p.startWorkers()
}

func (p *Pool) startWorkers() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.run()
	}
}

func (p *Pool) run() {
	defer p.wg.Done()
	for {
		// A stopping pool must notice promptly rather than pseudo-randomly
		// picking up another queued job first: select does not prioritise
		// between two ready cases, so without this check a worker that
		// finishes a job just after Stop is called has even odds of pulling
		// the next one off jobs instead of returning.
		select {
		case <-p.done:
			return
		default:
		}

		select {
		case j := <-p.jobs:
			p.dispatched.Add(1)
			if err := j.action.Do(p.runCtx, j.event); err != nil {
				p.failed.Add(1)
				p.log.Printf("rule %s: %s action failed: %v", j.rule, j.action.Kind(), err)
			}
		case <-p.done:
			return
		}
	}
}

// Submit queues an action. It never blocks and never panics, including when
// called concurrently with or after Stop: if the queue is full, or the pool
// is stopping, the job is refused and counted rather than sent, because
// blocking here would stall event delivery for every other rule.
func (p *Pool) Submit(a Action, e eventbus.Event, rule string) bool {
	select {
	case <-p.done:
		p.dropped.Add(1)
		p.log.Printf("rule %s: pool stopped, dropped", rule)
		return false
	default:
	}

	select {
	case p.jobs <- job{action: a, event: e, rule: rule}:
		return true
	case <-p.done:
		p.dropped.Add(1)
		p.log.Printf("rule %s: pool stopped, dropped", rule)
		return false
	default:
		// Deviation from the design: the design calls for dropping the
		// oldest queued job on overflow, keeping the incoming one. This
		// drops the incoming job instead, keeping whatever is already
		// queued. Noted here rather than changed, because for a
		// state-change trigger the newest event is usually the
		// semantically interesting one, so changing this is a real
		// behaviour question, not a typo fix.
		p.dropped.Add(1)
		p.log.Printf("rule %s: action queue full, dropped", rule)
		return false
	}
}

// Stop signals the workers to exit, cancels the context passed to every
// running action, and waits for them to return. It never closes jobs, so a
// Submit racing with Stop always has a live channel to select on.
//
// Canceling runCtx is what keeps shutdown bounded: without it, an exec action
// mid-timeout would run to the end of its own timeout before Stop could
// return, and systemd would SIGKILL the process out from under it once
// TimeoutStopUSec expired.
//
// Whatever is still sitting in jobs once every worker has exited was never
// going to run; it is abandoned, not merely delayed, so it is counted into
// Dropped here rather than left invisible.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		close(p.done)
		p.runCancel()
	})
	p.wg.Wait()
	if remaining := len(p.jobs); remaining > 0 {
		p.dropped.Add(uint64(remaining))
	}
}

// Stats returns a snapshot of the counters.
func (p *Pool) Stats() Stats {
	return Stats{
		Dispatched: p.dispatched.Load(),
		Dropped:    p.dropped.Load(),
		Failed:     p.failed.Load(),
	}
}

// Spec is the action-shaped subset of a rule step. It exists so this package
// does not import the rules package, which would be circular.
type Spec struct {
	Do      string
	List    string
	Push    string
	Command string
	Timeout string
}

// Build turns a step spec into a runnable action.
func Build(s Spec, c Pusher) (Action, error) {
	switch s.Do {
	case "redis":
		return NewRedisAction(c, s.List, s.Push)
	case "exec":
		timeout, err := parseTimeout(s.Timeout)
		if err != nil {
			return nil, err
		}
		return NewExecAction(s.Command, timeout)
	case "can", "lua", "http":
		return nil, fmt.Errorf("action %q is not supported yet", s.Do)
	case "":
		return nil, fmt.Errorf("step is missing do")
	default:
		return nil, fmt.Errorf("unknown action %q", s.Do)
	}
}

func parseTimeout(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("timeout: %w", err)
	}
	return d, nil
}
