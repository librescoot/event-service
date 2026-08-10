package seq

import (
	"sync"

	"github.com/librescoot/eventbus"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
)

// maxQueued is how many triggers a queue-policy rule may hold behind its live
// run. A trigger that flaps, a sequence that takes a while, and no bound is a
// memory leak with no upper edge; eight is deep enough for a burst and small
// enough that a hundred rules doing it at once is still nothing on a box with
// 48 MB.
const maxQueued = 8

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

// queuedFire is a trigger a queue-policy rule is holding until the run in
// front of it has finished. It keeps the event, not just the sequence,
// because a step's when is evaluated against whatever triggered its own run.
type queuedFire struct {
	seq   *Sequence
	event eventbus.Event
}

// Runner walks sequences. In-flight runs are registered under their rule
// name, which is the handle anything acting on a whole rule needs.
type Runner struct {
	pool *action.Pool
	sch  *sched.Scheduler
	log  Logger

	mu      sync.Mutex
	runs    map[string][]*run
	queued  map[string][]queuedFire
	refused uint64
	stopped bool
}

// NewRunner returns a Runner that submits steps to pool and parks a step's
// after delay on sch.
func NewRunner(pool *action.Pool, sch *sched.Scheduler, log Logger) *Runner {
	return &Runner{
		pool:   pool,
		sch:    sch,
		log:    log,
		runs:   make(map[string][]*run),
		queued: make(map[string][]queuedFire),
	}
}

// Fire starts a run of s triggered by e, or does whatever the rule's
// concurrency policy says to do about a run that is already live. It returns
// as soon as the first step is queued, so the caller, which is the goroutine
// reading the bus, is never held for the length of a sequence.
func (rn *Runner) Fire(s *Sequence, e eventbus.Event) {
	if len(s.Steps) == 0 {
		return
	}
	name := s.Rule.Name

	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return
	}
	if len(rn.runs[name]) > 0 {
		switch s.Rule.Concurrency {
		case rules.PolicyDrop:
			rn.mu.Unlock()
			rn.log.Printf("debug: rule %s: a run is already in flight, trigger dropped", name)
			return

		case rules.PolicyQueue:
			if len(rn.queued[name]) >= maxQueued {
				rn.refused++
				total := rn.refused
				rn.mu.Unlock()
				rn.log.Printf("rule %s: %d trigger(s) already queued, refusing this one (%d refused since start)", name, maxQueued, total)
				return
			}
			rn.queued[name] = append(rn.queued[name], queuedFire{seq: s, event: e})
			rn.mu.Unlock()
			return

		default:
			// Restart. The live runs are copied out before they are walked,
			// because endLocked edits the slice they are in.
			for _, other := range append([]*run(nil), rn.runs[name]...) {
				rn.endLocked(other)
			}
		}
	}

	r := &run{seq: s, event: e}
	rn.runs[name] = append(rn.runs[name], r)
	rn.mu.Unlock()

	rn.advance(r)
}

// CancelMatching drops every live run whose rule names topic in cancel-on,
// together with anything that rule is holding queued behind them, and returns
// how many runs it cancelled. Queued triggers are not runs and are not in the
// count; keeping them would defeat the cancel, since the backlog is usually
// more of exactly what is being cancelled.
//
// What a cancel stops is everything the run has not handed over yet: a
// pending timer is dropped and no further step is submitted. A step already
// given to the pool still runs, whether a worker has picked it up or it is
// waiting its turn in the queue behind other work. The pool owns an accepted
// job either way and there is no way to take one back, so "cancel" here means
// the tail, not the step already on its way.
func (rn *Runner) CancelMatching(topic string) int {
	rn.mu.Lock()

	n := 0
	for name, list := range rn.runs {
		if len(list) == 0 || !list[0].seq.Rule.CancelledBy(topic) {
			continue
		}
		delete(rn.queued, name)
		// Copied for the same reason as in Fire: endLocked edits rn.runs[name]
		// underneath a walk over it.
		for _, r := range append([]*run(nil), list...) {
			if rn.endLocked(r) {
				n++
			}
		}
	}
	rn.mu.Unlock()

	if n > 0 {
		rn.log.Printf("debug: %s cancelled %d run(s)", topic, n)
	}
	return n
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

// end finishes a run and starts whatever its rule has queued behind it. The
// promoted run is advanced after the lock is dropped: advance takes the same
// mutex, and a completion callback reaching this point is already deep enough
// in the call stack without holding it further.
func (rn *Runner) end(r *run) {
	var next *run

	rn.mu.Lock()
	if rn.endLocked(r) {
		next = rn.startQueuedLocked(r.seq.Rule.Name)
	}
	rn.mu.Unlock()

	if next != nil {
		rn.advance(next)
	}
}

// endLocked takes the run out of the registry and reports whether it was this
// call that ended it. Marking it ended first means a completion arriving
// afterwards finds nothing left to advance. A pending tail, if there is one,
// is cancelled here too: a run that ends early, whether from a failed step, a
// false when, a refused submit, a cancel-on topic or a restart, must not leave
// a timer behind to fire into a run nothing is tracking any more.
//
// It does not touch the rule's queue. A caller that ends a run because the
// sequence finished wants the next queued trigger to start; one that ends it
// because the rule was cancelled wants the queue gone. Only the caller knows
// which, so promotion lives in end and the queue drop lives in CancelMatching.
func (rn *Runner) endLocked(r *run) bool {
	if r.ended {
		return false
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
	return true
}

// startQueuedLocked registers a run for the head of name's queue, if the rule
// has one and nothing of it is in flight. The run is handed back rather than
// advanced, because advance must not be called with the mutex held.
func (rn *Runner) startQueuedLocked(name string) *run {
	if rn.stopped || len(rn.runs[name]) > 0 {
		return nil
	}
	q := rn.queued[name]
	if len(q) == 0 {
		return nil
	}

	head := q[0]
	// Clearing the slot drops the runner's last reference to that trigger's
	// event, which the tail of the slice would otherwise keep alive.
	q[0] = queuedFire{}
	if len(q) == 1 {
		delete(rn.queued, name)
	} else {
		rn.queued[name] = q[1:]
	}

	r := &run{seq: head.seq, event: head.event}
	rn.runs[name] = append(rn.runs[name], r)
	return r
}

// Active is how many runs are part-way through their steps, including a run
// currently parked on a timer waiting out an after. A trigger sitting in a
// queue-policy rule's backlog is not active: it has not started.
func (rn *Runner) Active() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	n := 0
	for _, list := range rn.runs {
		n += len(list)
	}
	return n
}

// Refused counts the triggers turned away by a queue-policy rule's bound
// since start. It is cumulative, and a number that climbs is the visible end
// of a rule whose sequence cannot keep up with what triggers it.
func (rn *Runner) Refused() uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.refused
}

// Stop refuses new runs, throws away anything queued behind them, and
// abandons the in-flight ones, cancelling any pending tail along the way: a
// run parked on a timer is still active, and letting that timer fire after
// the runner considers itself stopped would submit a step into a pool that
// may already be gone. A step already on a worker still runs to the end; its
// callback then finds the run ended and stops there rather than queueing more
// work into a pool that is going away.
//
// Ending goes through endLocked like every other cancel, so there is one
// definition of what ending a run means rather than two that can drift.
func (rn *Runner) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.stopped = true
	for _, list := range rn.runs {
		// Copied because endLocked edits the slice it is walking, and deletes
		// the key once the last run leaves it.
		for _, r := range append([]*run(nil), list...) {
			rn.endLocked(r)
		}
	}
	rn.queued = make(map[string][]queuedFire)
}
