package rules

import (
	"fmt"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/librescoot/eventbus"
)

// StateFunc reads a current value out of the shadow store. It is exposed to
// rule expressions as state("hash", "field") so a condition can consult
// something the event itself does not carry, without any datastore round trip.
type StateFunc func(hash, field string) string

// Rule is a compiled rule, ready to match. The expression is compiled once at
// load, so matching is a VM run over a small map with no allocation-heavy
// reflection and no I/O.
type Rule struct {
	Name     string
	Source   string
	On       []string
	Cooldown time.Duration
	Steps    []Step

	program *vm.Program
	state   StateFunc
}

// Step is one step of a rule. Config holds the fields the action is built
// from; When, when not nil, is a condition checked immediately before the
// step runs, as opposed to the rule-level condition checked when the event
// arrived.
type Step struct {
	Config StepConfig
	When   *vm.Program
}

// Compile turns parsed config into runnable rules. Errors are per-rule, so one
// bad rule does not disable the others; each error names the rule and the file
// it came from.
func Compile(cfgs []RuleConfig, lookup StateFunc) ([]*Rule, []error) {
	var out []*Rule
	var errs []error

	for _, c := range cfgs {
		if c.Enabled != nil && !*c.Enabled {
			continue
		}
		r, err := compileOne(c, lookup)
		if err != nil {
			errs = append(errs, fmt.Errorf("rule %q in %s: %w", c.Name, c.Source, err))
			continue
		}
		out = append(out, r)
	}
	return out, errs
}

func compileOne(c RuleConfig, lookup StateFunc) (*Rule, error) {
	if c.Name == "" {
		return nil, fmt.Errorf("missing name")
	}
	if len(c.On) == 0 {
		return nil, fmt.Errorf("missing on")
	}
	if len(c.Steps) == 0 {
		return nil, fmt.Errorf("missing step")
	}
	if c.Concurrency != "" {
		return nil, fmt.Errorf("concurrency is not supported yet")
	}
	// Presence, not truthiness: cancel-on = [] and repeat = {} both decode to
	// a non-nil, zero-length value, distinguishable from an omitted key by
	// nil-ness alone. A rule author who wrote either meant to use the
	// feature and must see the same rejection as the non-empty form.
	if c.CancelOn != nil {
		return nil, fmt.Errorf("cancel-on is not supported yet")
	}
	if c.Repeat != nil {
		return nil, fmt.Errorf("repeat is not supported yet")
	}
	if c.Debounce != nil {
		return nil, fmt.Errorf("debounce is not supported yet")
	}

	r := &Rule{
		Name:   c.Name,
		Source: c.Source,
		On:     c.On,
		Steps:  make([]Step, 0, len(c.Steps)),
		state:  lookup,
	}

	var err error
	if r.Cooldown, err = parseDuration(c.Cooldown); err != nil {
		return nil, fmt.Errorf("cooldown: %w", err)
	}

	if c.When != "" {
		p, err := compileWhen(c.When)
		if err != nil {
			return nil, fmt.Errorf("when: %w", err)
		}
		r.program = p
	}

	// The index goes into every step error: with a rule of five steps, an
	// error that names only the rule leaves the user reading all five.
	for i, sc := range c.Steps {
		if sc.After != "" {
			return nil, fmt.Errorf("step %d: after is not supported yet", i)
		}
		s := Step{Config: sc}
		if sc.When != "" {
			p, err := compileWhen(sc.When)
			if err != nil {
				return nil, fmt.Errorf("step %d: when: %w", i, err)
			}
			s.When = p
		}
		r.Steps = append(r.Steps, s)
	}

	return r, nil
}

// compileWhen turns one condition into a program. Rule-level and step-level
// conditions both come through here, so both see the same variables and both
// turn a typo into a load error rather than a rule that never fires.
func compileWhen(src string) (*vm.Program, error) {
	return expr.Compile(src, expr.Env(exprEnv{}), expr.AsBool())
}

func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// exprEnv is the shape a rule expression sees. Declaring it as a type lets
// expr type-check the expression at compile time, so a typo in a field name is
// a load error rather than a rule that silently never fires.
type exprEnv struct {
	Topic string                      `expr:"topic"`
	Src   string                      `expr:"src"`
	From  string                      `expr:"from"`
	To    string                      `expr:"to"`
	Data  map[string]any              `expr:"data"`
	State func(string, string) string `expr:"state"`
}

// Matches reports whether e should fire this rule. Topic is checked first
// because it is far cheaper than running the VM.
func (r *Rule) Matches(e eventbus.Event) (bool, error) {
	matched := false
	for _, pattern := range r.On {
		if MatchTopic(pattern, e.Topic) {
			matched = true
			break
		}
	}
	if !matched {
		return false, nil
	}
	return r.EvalWhen(r.program, e)
}

// EvalWhen runs a condition compiled from this rule against e. A nil program
// is the absent condition and evaluates true.
//
// A step condition is passed the event that triggered the run, so a step
// three deep in a sequence reads the same to, from and data the rule matched
// on, and state() reads whatever the shadow store holds at the moment the
// step is reached.
func (r *Rule) EvalWhen(p *vm.Program, e eventbus.Event) (bool, error) {
	if p == nil {
		return true, nil
	}

	lookup := r.state
	if lookup == nil {
		lookup = func(string, string) string { return "" }
	}

	env := exprEnv{
		Topic: e.Topic,
		Src:   e.Src,
		From:  e.From,
		To:    e.To,
		Data:  e.Data,
		State: lookup,
	}
	if env.Data == nil {
		env.Data = map[string]any{}
	}

	out, err := expr.Run(p, env)
	if err != nil {
		return false, fmt.Errorf("evaluate when: %w", err)
	}
	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("when did not evaluate to a bool, got %T", out)
	}
	return b, nil
}
