package main

import (
	"fmt"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/adapter"
	"github.com/librescoot/event-service/internal/inputs"
	"github.com/librescoot/event-service/internal/rules"
)

type inputRuleError struct {
	name string
	err  error
}

func (e *inputRuleError) Error() string { return e.err.Error() }
func (e *inputRuleError) Unwrap() error { return e.err }

// configureInputs reserves state dependencies for pending cleanup before new
// triggers. A conflicting/over-budget rule is rejected individually, without
// taking unrelated rules down or leaving its inputs subscribed.
func configureInputs(ad *adapter.Adapter, compiled []*rules.Rule) ([]*rules.Rule, adapter.Source, []error) {
	ordered := make([]*rules.Rule, 0, len(compiled))
	for _, r := range compiled {
		if r.ReplayOnly {
			ordered = append(ordered, r)
		}
	}
	for _, r := range compiled {
		if !r.ReplayOnly {
			ordered = append(ordered, r)
		}
	}
	kept := make(map[*rules.Rule]bool)
	var selected []inputs.Config
	var errs []error
	for _, r := range ordered {
		var invalid error
		for _, step := range r.Steps {
			if err := action.Validate(step.Config.ActionSpec()); err != nil {
				invalid = err
				break
			}
		}
		candidate := append([]inputs.Config(nil), selected...)
		for _, input := range r.Inputs {
			if r.ReplayOnly {
				if input.Hash == "" {
					continue
				}
				input.Topic = ""
			}
			candidate = append(candidate, input)
		}
		if invalid == nil {
			invalid = inputs.ValidateSet(candidate)
		}
		if invalid == nil {
			source, err := adapter.NewConfiguredSource(candidate)
			invalid = err
			if err == nil {
				invalid = ad.CanRegister(source)
			}
		}
		if invalid != nil {
			errs = append(errs, &inputRuleError{name: r.Name, err: fmt.Errorf("rule %q in %s: inputs/actions: %w", r.Name, r.Source, invalid)})
			continue
		}
		selected = candidate
		kept[r] = true
	}
	accepted := make([]*rules.Rule, 0, len(kept))
	for _, r := range compiled {
		if kept[r] {
			accepted = append(accepted, r)
		}
	}
	// Each accepted candidate was validated above; the empty set is valid.
	source, err := adapter.NewConfiguredSource(selected)
	if err != nil {
		panic(fmt.Sprintf("validated input set rejected: %v", err))
	}
	return accepted, source, errs
}
