package seq

import (
	"fmt"
	"sync"
	"time"

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
// to claim within the current pass. pass is how many complete passes ran
// before this one; a rule with no repeat never moves it off zero. ended marks
// a run that must not move any further. cancelTail holds the scheduler cancel
// for whatever this run is currently parked on, a step's own after or the gap
// between one repeat pass and the next; it is nil whenever the run is not
// waiting on a timer. rec says the datastore holds a pending record under id,
// which exactly one of the fire path or whatever ends the run must remove.
//
// id is the run's field name in the pending hash. It stays the same for the
// whole run: a run waits on at most one step at a time, so one field is
// enough, and a replayed run keeps the id it was recorded under.
//
// All fields are guarded by the Runner's mutex: a run starts on the bus
// subscriber goroutine and then moves forward on whichever pool worker
// finished the previous step, or on the scheduler's own goroutine once a
// parked timer fires.
type run struct {
	seq        *Sequence
	event      eventbus.Event
	id         string
	step       int
	pass       int
	ended      bool
	rec        bool
	cancelTail func() bool
}

// queuedFire is a trigger a queue-policy rule is holding until the run in
// front of it has finished. It keeps the event, not just the sequence,
// because a step's when is evaluated against whatever triggered its own run.
//
// A backlog is memory only and is gone after a restart, deliberately. What
// durability exists for is a run that has already acted on the vehicle and
// still owes it the other half, the thirty seconds between "hazards on" and
// "hazards off". A queued trigger has done nothing yet, so losing it leaves
// nothing latched. Replaying one would be the opposite: a burst of triggers
// from before the restart, all firing at once, against a vehicle state that
// has moved on in the meantime and against step conditions evaluated from a
// snapshot of a world that no longer exists.
type queuedFire struct {
	seq   *Sequence
	event eventbus.Event
}

// Runner walks sequences. In-flight runs are registered under their rule
// name, which is the handle anything acting on a whole rule needs.
type Runner struct {
	pool  *action.Pool
	sch   *sched.Scheduler
	store *PendingStore
	log   Logger

	// epoch separates this process's run ids from those of the process
	// before it, so a run started now cannot land on the hash field of a
	// record left behind by an earlier boot and waiting to be replayed.
	epoch int64

	mu      sync.Mutex
	runs    map[string][]*run
	queued  map[string][]queuedFire
	nextRun uint64
	refused uint64
	stopped bool
}

// NewRunner returns a Runner that submits steps to pool and parks a step's
// after delay on sch. store records the steps that are waiting, so a restart
// mid-wait can pick them up again; a nil store turns that off and the runner
// keeps everything in memory.
func NewRunner(pool *action.Pool, sch *sched.Scheduler, store *PendingStore, log Logger) *Runner {
	return &Runner{
		pool:   pool,
		sch:    sch,
		store:  store,
		log:    log,
		epoch:  time.Now().UnixNano(),
		runs:   make(map[string][]*run),
		queued: make(map[string][]queuedFire),
	}
}

// newRunLocked registers a run of s for e and gives it the id its pending
// records go under.
func (rn *Runner) newRunLocked(s *Sequence, e eventbus.Event) *run {
	rn.nextRun++
	name := s.Rule.Name
	r := &run{
		seq:   s,
		event: e,
		id:    fmt.Sprintf("%s#%d-%d", name, rn.epoch, rn.nextRun),
	}
	rn.runs[name] = append(rn.runs[name], r)
	return r
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
	var records []string

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
				if _, id := rn.endLocked(other); id != "" {
					records = append(records, id)
				}
			}
		}
	}

	r := rn.newRunLocked(s, e)
	rn.mu.Unlock()

	// The replaced runs' records go once the lock is down: nothing will ever
	// fire their tails, so leaving them would replay a step the restart
	// abandoned.
	rn.dropRecords(records)
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
	var records []string

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
			ended, id := rn.endLocked(r)
			if ended {
				n++
			}
			if id != "" {
				records = append(records, id)
			}
		}
	}
	rn.mu.Unlock()

	// A cancelled run's record has to go, and go here: a record that outlives
	// the cancel is replayed on the next boot and fires hardware the rider
	// stopped on purpose.
	rn.dropRecords(records)

	if n > 0 {
		rn.log.Printf("debug: %s cancelled %d run(s)", topic, n)
	}
	return n
}

// advance claims the step the run is sitting on within its current pass, or
// hands off to finishPass once the pass is out of steps. It is the place a
// run's step index moves forward: Fire calls it to start a run, a step's
// completion callback calls it for the step after, a parked step's timer fire
// reaches it indirectly through runStep once its delay has elapsed, and a
// repeat gap's timer fire calls it directly to start the next pass from step
// zero.
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
	// The pass is read here rather than in deferStep so a record carries the
	// pass the step belongs to without reading a mutex-guarded field from
	// outside the lock.
	iter := r.pass
	rn.mu.Unlock()

	if idx >= len(r.seq.Steps) {
		rn.finishPass(r)
		return
	}
	step := r.seq.Steps[idx]

	if step.After > 0 {
		rn.deferStep(r, idx, iter, step)
		return
	}

	rn.runStep(r, idx, step)
}

// finishPass is reached once every step of one pass has been claimed. A rule
// with no repeat, or one whose passes are used up, ends here like any other
// run out of steps. A rule with passes left instead parks the gap before the
// next one on the scheduler, exactly the way a step's own after parks, so the
// wait holds no worker; the cancel goes into the same cancelTail field a
// step's after uses, so a cancel or a Stop reaching a run sitting in the gap
// finds and drops the timer through the same path endLocked already handles
// for every other pending tail. The next pass starts from the sequence's
// first step once the gap elapses.
//
// The gap itself is not recorded. Durability belongs to a step's own after,
// which is where a rule leaves the vehicle half-changed and owes it the other
// half; a run sitting between two complete passes owes it nothing, and a
// restart there simply ends the repeat early.
func (rn *Runner) finishPass(r *run) {
	rn.mu.Lock()
	if r.ended || rn.stopped {
		rn.mu.Unlock()
		return
	}
	rep := r.seq.Rule.Repeat
	if rep == nil || r.pass+1 >= rep.Count {
		rn.mu.Unlock()
		rn.end(r)
		return
	}
	r.pass++
	r.step = 0
	r.cancelTail = rn.sch.At(rep.Every, func() {
		rn.mu.Lock()
		if r.ended || rn.stopped {
			rn.mu.Unlock()
			return
		}
		r.cancelTail = nil
		rn.mu.Unlock()
		rn.advance(r)
	})
	rn.mu.Unlock()
}

// deferStep parks step on the scheduler and returns without submitting
// anything, so the run holds no worker and no goroutine of its own while it
// waits out the delay. The step's when, if it has one, is deliberately left
// unevaluated here: runStep checks it once the timer fires, against whatever
// the shadow store holds at that moment, not against what it held when the
// wait began.
//
// A durable step is written down before the timer is armed. The other order
// loses the race with a short delay: the fire would remove a record that had
// not been written yet, and the write would land behind it with nothing left
// to take it away again.
func (rn *Runner) deferStep(r *run, idx, iter int, step CompiledStep) {
	if step.Durable {
		rn.putRecord(r, idx, iter, step.After)
	}
	rn.park(r, idx, step, step.After)
}

// park suspends r on the scheduler until d has elapsed, then fires step idx.
// The run's record, if it has one, is already written by the time park is
// called, whether by deferStep or by a replay picking the step back up.
func (rn *Runner) park(r *run, idx int, step CompiledStep, d time.Duration) {
	rn.mu.Lock()
	if r.ended || rn.stopped {
		// A shutdown leaves the record where it is: that is the whole point,
		// since the next start is what will run the step. A run that ended
		// any other way was ended before its record existed, so nobody else
		// is going to remove it.
		var id string
		if !rn.stopped {
			id = claimRecordLocked(r)
		}
		rn.mu.Unlock()
		rn.dropRecord(id)
		return
	}
	// The cancel is stored under the same lock that a concurrent end or Stop
	// needs, so either this line has not run yet, in which case the run is
	// already ended and the fire callback below finds that and stops there,
	// or it has, in which case end or Stop can reach it and cancel the timer
	// before it fires.
	r.cancelTail = rn.sch.At(d, func() { rn.fireDeferred(r, idx, step) })
	rn.mu.Unlock()
}

// fireDeferred is what a parked step's timer runs. The record goes as the
// step is claimed, not once its action has finished: what is recorded is the
// wait, and the wait is over. A step already handed to the pool is the pool's
// to run or abandon, and re-running it on the next boot because a worker was
// cut short mid-action would fire hardware twice.
func (rn *Runner) fireDeferred(r *run, idx int, step CompiledStep) {
	rn.mu.Lock()
	if r.ended || rn.stopped {
		rn.mu.Unlock()
		return
	}
	r.cancelTail = nil
	id := claimRecordLocked(r)
	rn.mu.Unlock()

	rn.dropRecord(id)
	rn.runStep(r, idx, step)
}

// putRecord writes the run's pending record and marks the run as holding one.
// It runs outside the runner lock: endLocked is on the path of every
// finishing run, so a datastore round trip held under that mutex would sit in
// front of every other rule on a one-core box.
func (rn *Runner) putRecord(r *run, idx, iter int, d time.Duration) {
	if rn.store == nil {
		return
	}
	rule := r.seq.Rule
	err := rn.store.Put(Pending{
		ID:     r.id,
		Rule:   rule.Name,
		Source: rule.Source,
		Step:   idx,
		Iter:   iter,
		FireAt: time.Now().Add(d).UnixMilli(),
		Event:  r.event,
	})
	if err != nil {
		// The step still runs; it just will not survive a restart. Saying so
		// is better than silently downgrading a rule the author expects to be
		// replayed.
		rn.log.Printf("rule %s: step %d: %v", rule.Name, idx, err)
		return
	}

	rn.mu.Lock()
	r.rec = true
	rn.mu.Unlock()
}

// claimRecordLocked takes the run's record, so exactly one caller ends up
// responsible for removing it. It returns the empty string when there is
// nothing to remove, which is the common case: most steps do not wait.
func claimRecordLocked(r *run) string {
	if !r.rec {
		return ""
	}
	r.rec = false
	return r.id
}

// dropRecord removes a claimed record. Call it with the lock down.
func (rn *Runner) dropRecord(id string) {
	if id == "" || rn.store == nil {
		return
	}
	if err := rn.store.Drop(id); err != nil {
		rn.log.Printf("pending: %v", err)
	}
}

func (rn *Runner) dropRecords(ids []string) {
	for _, id := range ids {
		rn.dropRecord(id)
	}
}

// runStep evaluates a step's when, if it has one, and submits it. advance
// calls it directly for a step with no delay; fireDeferred calls it once a
// parked step's wait is over.
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
	ended, id := rn.endLocked(r)
	if ended {
		next = rn.startQueuedLocked(r.seq.Rule.Name)
	}
	rn.mu.Unlock()

	rn.dropRecord(id)

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
// The run's pending record is claimed but not removed: this is the tail-cancel
// point every finishing run goes through, and a datastore round trip under the
// runner mutex would be paid by every other rule on a one-core box. The caller
// removes what it is handed, once it has dropped the lock. Stop is the one
// caller that deliberately does not.
//
// It does not touch the rule's queue. A caller that ends a run because the
// sequence finished wants the next queued trigger to start; one that ends it
// because the rule was cancelled wants the queue gone. Only the caller knows
// which, so promotion lives in end and the queue drop lives in CancelMatching.
func (rn *Runner) endLocked(r *run) (ended bool, record string) {
	if r.ended {
		return false, ""
	}
	r.ended = true
	if r.cancelTail != nil {
		r.cancelTail()
		r.cancelTail = nil
	}
	record = claimRecordLocked(r)

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
	return true, record
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

	return rn.newRunLocked(head.seq, head.event)
}

// Replay picks up the steps that were waiting when the service last went
// down and returns how many it resumed. Call it once, after the rules are
// compiled and before the bus subscription starts: a replayed step must not
// race a live re-fire of the same rule.
//
// A record is dropped, with a line saying why, when the rule it names is no
// longer loaded, when the rule no longer has the step it indexes (its author
// edited the file while the service was down), or when it is more than window
// past due. That last one is the important limit: a scooter that was off for
// a week must not come back up and start acting on what it was in the middle
// of doing back then. Anything left is resumed on the run it belonged to,
// keeping its id, so the record still covers it until it fires.
func (rn *Runner) Replay(seqs []*Sequence, window time.Duration) int {
	if rn.store == nil {
		return 0
	}
	recs, err := rn.store.Load()
	if err != nil {
		rn.log.Printf("pending: %v", err)
		return 0
	}

	byName := make(map[string]*Sequence, len(seqs))
	for _, s := range seqs {
		byName[s.Rule.Name] = s
	}

	now := time.Now()
	n := 0
	for _, p := range recs {
		s, ok := byName[p.Rule]
		if !ok {
			rn.log.Printf("pending %s: rule %s is not loaded any more, dropped", p.ID, p.Rule)
			rn.dropRecord(p.ID)
			continue
		}
		if p.Step < 0 || p.Step >= len(s.Steps) {
			rn.log.Printf("pending %s: rule %s has no step %d any more, dropped", p.ID, p.Rule, p.Step)
			rn.dropRecord(p.ID)
			continue
		}
		due := time.UnixMilli(p.FireAt)
		if late := now.Sub(due); late > window {
			rn.log.Printf("pending %s: rule %s step %d was due %v ago, past the %v window, dropped", p.ID, p.Rule, p.Step, late.Round(time.Second), window)
			rn.dropRecord(p.ID)
			continue
		}

		if !rn.resume(s, p, due.Sub(now)) {
			continue
		}
		n++
	}
	return n
}

// resume registers a run for a record and either fires its step, if the delay
// has already run out, or parks what is left of it. The record stays where it
// is: the resumed run carries the same id, so it is still covered until the
// step actually fires.
func (rn *Runner) resume(s *Sequence, p Pending, remaining time.Duration) bool {
	r := &run{
		seq:   s,
		event: p.Event,
		id:    p.ID,
		// The step this record names has not run, so it is the next one to
		// claim; the index moves past it here the same way advance moves it
		// past a step it is about to hand on.
		step: p.Step + 1,
		pass: p.Iter,
		rec:  true,
	}

	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return false
	}
	rn.runs[s.Rule.Name] = append(rn.runs[s.Rule.Name], r)
	rn.mu.Unlock()

	step := s.Steps[p.Step]
	if remaining <= 0 {
		rn.log.Printf("rule %s: step %d was due while the service was down, running it now", p.Rule, p.Step)
		rn.fireDeferred(r, p.Step, step)
		return true
	}
	rn.log.Printf("rule %s: step %d resumed, %v left to wait", p.Rule, p.Step, remaining.Round(time.Millisecond))
	rn.park(r, p.Step, step, remaining)
	return true
}

// Active is how many runs are part-way through their steps, including a run
// currently parked on a timer waiting out an after or the gap between two
// repeat passes. A trigger sitting in a queue-policy rule's backlog is not
// active: it has not started.
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
//
// The pending records endLocked hands back are deliberately dropped on the
// floor rather than removed from the datastore. Every other caller ends a run
// because it is over; Stop ends one because this process is. A step still
// waiting out its delay has not run, and leaving its record is what lets the
// next start run it. Removing them here would break the one case durability
// exists for.
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
