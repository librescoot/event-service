// Package seq turns a compiled rule into a sequence of steps and walks it.
//
// A rule is a recipe: its steps run in order, and a step is submitted only
// once the step before it has finished. Nothing here owns a goroutine. Step
// N+1 is submitted from step N's completion callback, which runs on the pool
// worker that just finished step N, so a scooter with a hundred half-finished
// sequences pays for a hundred small structs and nothing else.
package seq

import (
	"fmt"

	"github.com/expr-lang/expr/vm"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/rules"
)

// Sequence is a rule with every step built into a runnable action.
type Sequence struct {
	Rule  *rules.Rule
	Steps []CompiledStep
}

// CompiledStep is one step ready to submit. When is nil for a step with no
// condition of its own.
type CompiledStep struct {
	Action action.Action
	When   *vm.Program
}

// Build compiles every step of r. An error names the step index; the caller
// adds the rule and the file.
func Build(r *rules.Rule, c action.Pusher) (*Sequence, error) {
	s := &Sequence{Rule: r, Steps: make([]CompiledStep, 0, len(r.Steps))}

	for i, step := range r.Steps {
		a, err := action.Build(action.Spec{
			Do:      step.Config.Do,
			List:    step.Config.List,
			Push:    step.Config.Push,
			Command: step.Config.Command,
			Timeout: step.Config.Timeout,
		}, c)
		if err != nil {
			return nil, fmt.Errorf("step %d: %w", i, err)
		}
		s.Steps = append(s.Steps, CompiledStep{Action: a, When: step.When})
	}

	return s, nil
}
