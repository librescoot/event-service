package rules

import (
	"fmt"

	"github.com/librescoot/event-service/api"
	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/eventbus"
)

func (s StepConfig) ActionSpec() action.Spec {
	return action.Spec{Do: s.Do, List: s.List, Push: s.Push, Command: s.Command,
		Timeout: s.Timeout, Iface: s.Iface, ID: s.ID, Data: s.Data, RTR: s.RTR, DLC: s.DLC}
}

// ValidateDefinition includes disabled definitions: an administrator must be
// able to inspect their errors without enabling or executing them.
func ValidateDefinition(c RuleConfig, lookup StateFunc) (*Rule, error) {
	r, err := compileOne(c, lookup)
	if err != nil {
		return nil, err
	}
	for i, s := range c.Steps {
		if err := action.Validate(s.ActionSpec()); err != nil {
			return nil, fmt.Errorf("step %d: %w", i, err)
		}
	}
	return r, nil
}

// Preview only compiles and evaluates pure configuration. It never builds an
// action, publishes an event, registers a timer, or modifies runtime state.
// lookup must read an immutable snapshot supplied by the caller.
func Preview(c RuleConfig, e eventbus.Event, lookup StateFunc) (api.TestResponse, error) {
	r, err := ValidateDefinition(c, lookup)
	if err != nil {
		return api.TestResponse{}, err
	}
	out := api.TestResponse{
		Name: c.Name, Enabled: c.Enabled == nil || *c.Enabled, RepeatCount: 1,
		Steps:    make([]api.StepPreview, 0, len(r.Steps)),
		Cooldown: c.Cooldown, Debounce: r.Debounce.String(),
		Note: "Dry run only. Step conditions use current state, not future state. Cooldown, debounce and concurrency are not simulated; no actions are dispatched.",
	}
	out.Matched, err = r.Matches(e)
	if err != nil {
		out.Error = err.Error()
	}
	if r.Repeat != nil {
		out.RepeatCount = r.Repeat.Count
		out.RepeatEvery = r.Repeat.Every.String()
	}
	for i, s := range r.Steps {
		p := api.StepPreview{Index: i, Kind: s.Config.Do, Condition: true, After: s.Config.After, Durable: s.Durable}
		if s.When != nil {
			p.Condition, err = r.EvalWhen(s.When, e)
			if err != nil {
				p.Error = err.Error()
			}
		}
		switch s.Config.Do {
		case "redis":
			p.Description = fmt.Sprintf("LPUSH %q %q", s.Config.List, s.Config.Push)
		case "exec":
			p.Description = fmt.Sprintf("execute %q", s.Config.Command)
		case "can":
			p.Description = fmt.Sprintf("CAN interface=%q id=%q data=%q rtr=%t", s.Config.Iface, s.Config.ID, s.Config.Data, s.Config.RTR)
			if s.Config.DLC != nil {
				p.Description += fmt.Sprintf(" dlc=%d", *s.Config.DLC)
			}
		}
		out.Steps = append(out.Steps, p)
	}
	return out, nil
}
