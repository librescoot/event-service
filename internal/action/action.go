// Package action runs what a rule decided should happen.
//
// Everything here executes on a bounded worker pool. The pool is the isolation
// boundary between user-supplied behaviour and the rest of the service: a
// script that hangs occupies a worker and nothing else.
package action

import (
	"context"
	"sync"
	"sync/atomic"

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
	return &Pool{jobs: make(chan job, queue), done: make(chan struct{}), workers: workers, log: log}
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
		select {
		case j := <-p.jobs:
			p.dispatched.Add(1)
			if err := j.action.Do(context.Background(), j.event); err != nil {
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
		p.dropped.Add(1)
		p.log.Printf("rule %s: action queue full, dropped", rule)
		return false
	}
}

// Stop signals the workers to exit and waits for in-flight work to finish.
// It never closes jobs, so a Submit racing with Stop always has a live
// channel to select on.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() { close(p.done) })
	p.wg.Wait()
}

// Stats returns a snapshot of the counters.
func (p *Pool) Stats() Stats {
	return Stats{
		Dispatched: p.dispatched.Load(),
		Dropped:    p.dropped.Load(),
		Failed:     p.failed.Load(),
	}
}
