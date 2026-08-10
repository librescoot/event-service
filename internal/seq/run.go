package seq

import (
	"sync"

	"github.com/librescoot/eventbus"

	"github.com/librescoot/event-service/internal/action"
)

// Logger is the small slice of logging this package needs.
type Logger interface {
	Printf(format string, v ...any)
}

// run is one walk over a sequence's steps. step is the index of the step to
// submit next, and ended marks a run that must not move any further.
//
// Both fields are guarded by the Runner's mutex: a run starts on the bus
// subscriber goroutine and then moves forward on whichever pool worker
// finished the previous step.
type run struct {
	seq   *Sequence
	event eventbus.Event
	step  int
	ended bool
}

// Runner walks sequences. In-flight runs are registered under their rule
// name, which is the handle anything acting on a whole rule needs.
type Runner struct {
	pool *action.Pool
	log  Logger

	mu      sync.Mutex
	runs    map[string][]*run
	stopped bool
}

// NewRunner returns a Runner that submits steps to pool.
func NewRunner(pool *action.Pool, log Logger) *Runner {
	return &Runner{
		pool: pool,
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

// advance submits the step the run is sitting on, or ends the run. It is the
// single place a run moves forward: Fire calls it to start one, and every
// step's completion callback calls it for the step after.
func (rn *Runner) advance(r *run) {
	rn.mu.Lock()
	if r.ended || rn.stopped {
		rn.mu.Unlock()
		return
	}
	idx := r.step
	rn.mu.Unlock()

	name := r.seq.Rule.Name
	if idx >= len(r.seq.Steps) {
		rn.end(r)
		return
	}
	step := r.seq.Steps[idx]

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
		rn.mu.Lock()
		r.step = idx + 1
		rn.mu.Unlock()
		rn.advance(r)
	})

	if !submitted {
		// A refused job never calls done, so no callback is coming to move
		// this run along: end it here. The pool counts the refusal. Retrying
		// would only push at a queue that is already full.
		rn.log.Printf("rule %s: step %d refused by the action pool, run ends", name, idx)
		rn.end(r)
	}
}

// end takes the run out of the registry. Marking it ended first means a
// completion arriving afterwards finds nothing left to advance.
func (rn *Runner) end(r *run) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if r.ended {
		return
	}
	r.ended = true

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

// Active is how many runs are part-way through their steps.
func (rn *Runner) Active() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	n := 0
	for _, list := range rn.runs {
		n += len(list)
	}
	return n
}

// Stop refuses new runs and abandons the in-flight ones. A step already on a
// worker still runs to the end; its callback then finds the run ended and
// stops there rather than queueing more work into a pool that is going away.
func (rn *Runner) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.stopped = true
	for _, list := range rn.runs {
		for _, r := range list {
			r.ended = true
		}
	}
	rn.runs = make(map[string][]*run)
}
