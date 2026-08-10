package seq

import (
	"sync"

	"github.com/librescoot/eventbus"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/sched"
)

// Logger is the small slice of logging this package needs.
type Logger interface {
	Printf(format string, v ...any)
}

// run is one walk over a sequence's steps. step is the index of the next step
// to claim, and ended marks a run that must not move any further. cancelTail
// holds the scheduler cancel for a step currently parked on a timer; it is
// nil whenever the run is not waiting out an after.
//
// All three fields are guarded by the Runner's mutex: a run starts on the bus
// subscriber goroutine and then moves forward on whichever pool worker
// finished the previous step, or on the scheduler's own goroutine once a
// parked step's timer fires.
type run struct {
	seq        *Sequence
	event      eventbus.Event
	step       int
	ended      bool
	cancelTail func() bool
}

// Runner walks sequences. In-flight runs are registered under their rule
// name, which is the handle anything acting on a whole rule needs.
type Runner struct {
	pool *action.Pool
	sch  *sched.Scheduler
	log  Logger

	mu      sync.Mutex
	runs    map[string][]*run
	stopped bool
}

// NewRunner returns a Runner that submits steps to pool and parks a step's
// after delay on sch.
func NewRunner(pool *action.Pool, sch *sched.Scheduler, log Logger) *Runner {
	return &Runner{
		pool: pool,
		sch:  sch,
		log:  log,
		runs: make(map[string][]*run),
	}
}

// Fire starts a run of s triggered by e. It returns as soon as the first step
// is queued, so the caller, which is the goroutine reading the bus, is never
// held for the length of a sequence.
func (rn *Runner) Fire(s *Sequence, e eventbus.Event) {
	if len(s.Steps) == 0 {
		return
	}

	r := &run{seq: s, event: e}

	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return
	}
	name := s.Rule.Name
	rn.runs[name] = append(rn.runs[name], r)
	rn.mu.Unlock()

	rn.advance(r)
}

// advance claims the step the run is sitting on, or ends the run if none are
// left. It is the single place a run's step index moves forward: Fire calls
// it to start a run, a step's completion callback calls it for the step
// after, and a parked step's timer fire reaches it indirectly, through
// runStep rather than a second call to advance, once its delay has elapsed.
func (rn *Runner) advance(r *run) {
	rn.mu.Lock()
	if r.ended || rn.stopped {
		rn.mu.Unlock()
		return
	}
	// The index moves on in the same critical section that read it, so two
	// calls for the same run claim different steps instead of both claiming
	// this one. Nothing puts it back: a step that is not submitted ends the
	// run outright, so an index one past a step that never ran is only ever
	// read by a run that is finished with it.
	idx := r.step
	r.step = idx + 1
	rn.mu.Unlock()

	if idx >= len(r.seq.Steps) {
		rn.end(r)
		return
	}
	step := r.seq.Steps[idx]

	if step.After > 0 {
		rn.deferStep(r, idx, step)
		return
	}

	rn.runStep(r, idx, step)
}

// deferStep parks step on the scheduler and returns without submitting
// anything, so the run holds no worker and no goroutine of its own while it
// waits out the delay. The step's when, if it has one, is deliberately left
// unevaluated here: runStep checks it once the timer fires, against whatever
// the shadow store holds at that moment, not against what it held when the
// wait began.
func (rn *Runner) deferStep(r *run, idx int, step CompiledStep) {
	rn.mu.Lock()
	if r.ended || rn.stopped {
		rn.mu.Unlock()
		return
	}
	// The cancel is stored under the same lock that a concurrent end or Stop
	// needs, so either this line has not run yet, in which case the run is
	// already ended and the fire callback below finds that and stops there,
	// or it has, in which case end or Stop can reach it and cancel the timer
	// before it fires.
	r.cancelTail = rn.sch.At(step.After, func() {
		rn.mu.Lock()
		if r.ended || rn.stopped {
			rn.mu.Unlock()
			return
		}
		r.cancelTail = nil
		rn.mu.Unlock()
		rn.runStep(r, idx, step)
	})
	rn.mu.Unlock()
}

// runStep evaluates a step's when, if it has one, and submits it. advance
// calls it directly for a step with no delay; deferStep's timer callback
// calls it once a delayed step's wait is over.
func (rn *Runner) runStep(r *run, idx int, step CompiledStep) {
	name := r.seq.Rule.Name

	if step.When != nil {
		ok, err := r.seq.Rule.EvalWhen(step.When, r.event)
		if err != nil {
			rn.log.Printf("rule %s: step %d: %v", name, idx, err)
			rn.end(r)
			return
		}
		if !ok {
			// Not a failure. The author wrote a condition, it does not hold,
			// and there is nothing left of the recipe worth running.
			rn.log.Printf("debug: rule %s: step %d when is false, run ends", name, idx)
			rn.end(r)
			return
		}
	}

	// The step is queued with the lock held, after one more check that the run
	// is still live. Submit neither blocks nor calls done inline, so holding
	// the mutex across it costs nothing and leaves no window in which a
	// cancelled run gets one more step out of the door.
	rn.mu.Lock()
	if r.ended || rn.stopped {
		rn.mu.Unlock()
		return
	}
	submitted := rn.pool.Submit(step.Action, r.event, name, func(err error) {
		if err != nil {
			// A sequence is a recipe, so a failed step ends it. Carrying on
			// would run "turn the hazards off" against a state the earlier
			// step never established. The pool has already logged and counted
			// the action's own error; this line says what it cost.
			rn.log.Printf("rule %s: step %d failed, %d later step(s) skipped", name, idx, len(r.seq.Steps)-idx-1)
			rn.end(r)
			return
		}
		rn.advance(r)
	})
	rn.mu.Unlock()

	if !submitted {
		// A refused job never calls done, so no callback is coming to move
		// this run along: end it here. The pool counts the refusal. Retrying
		// would only push at a queue that is already full.
		rn.log.Printf("rule %s: step %d refused by the action pool, run ends", name, idx)
		rn.end(r)
	}
}

// end takes the run out of the registry. Marking it ended first means a
// completion arriving afterwards finds nothing left to advance. A pending
// tail, if there is one, is cancelled here too: a run that ends early,
// whether from a failed step, a false when, or a refused submit, must not
// leave a timer behind to fire into a run nothing is tracking any more.
func (rn *Runner) end(r *run) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if r.ended {
		return
	}
	r.ended = true
	if r.cancelTail != nil {
		r.cancelTail()
		r.cancelTail = nil
	}

	name := r.seq.Rule.Name
	list := rn.runs[name]
	for i, other := range list {
		if other == r {
			rn.runs[name] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(rn.runs[name]) == 0 {
		delete(rn.runs, name)
	}
}

// Active is how many runs are part-way through their steps, including a run
// currently parked on a timer waiting out an after.
func (rn *Runner) Active() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	n := 0
	for _, list := range rn.runs {
		n += len(list)
	}
	return n
}

// Stop refuses new runs and abandons the in-flight ones, cancelling any
// pending tail along the way: a run parked on a timer is still active, and
// letting that timer fire after the runner considers itself stopped would
// submit a step into a pool that may already be gone. A step already on a
// worker still runs to the end; its callback then finds the run ended and
// stops there rather than queueing more work into a pool that is going away.
func (rn *Runner) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.stopped = true
	for _, list := range rn.runs {
		for _, r := range list {
			r.ended = true
			if r.cancelTail != nil {
				r.cancelTail()
				r.cancelTail = nil
			}
		}
	}
	rn.runs = make(map[string][]*run)
}
