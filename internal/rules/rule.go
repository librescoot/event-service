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

// Policy decides what a trigger does to a run of the same rule that has not
// finished yet.
type Policy string

const (
	// PolicyRestart drops the pending tail of the live run and starts the
	// sequence over. It is what an omitted concurrency means, because a rule
	// that keeps triggering usually wants its timer pushed back rather than a
	// second copy of itself.
	PolicyRestart Policy = "restart"
	// PolicyDrop ignores the trigger while a run is live.
	PolicyDrop Policy = "drop"
	// PolicyQueue holds the trigger until the live run has finished, then
	// runs the sequence again.
	PolicyQueue Policy = "queue"
)

// Rule is a compiled rule, ready to match. The expression is compiled once at
// load, so matching is a VM run over a small map with no allocation-heavy
// reflection and no I/O.
type Rule struct {
	Name        string
	Source      string
	On          []string
	CancelOn    []string
	Concurrency Policy
	Cooldown    time.Duration
	Steps       []Step

	program *vm.Program
	state   StateFunc
}

// Step is one step of a rule. Config holds the fields the action is built
// from; When, when not nil, is a condition checked immediately before the
// step runs, as opposed to the rule-level condition checked when the event
// arrived. After, when greater than zero, delays the step: it is checked
// against the clock, not against When, so a step can wait and still carry
// its own condition.
type Step struct {
	Config StepConfig
	When   *vm.Program
	After  time.Duration
}

// Compile turns parsed config into runnable rules. Errors are per-rule, so one
// bad rule does not disable the others; each error names the rule and the file
// it came from.
func Compile(cfgs []RuleConfig, lookup StateFunc) ([]*Rule, []error) {
	var out []*Rule
	var errs []error

	// A name is the handle everything acting on a whole rule uses: the runner
	// groups runs under it, so two rules sharing one would share a concurrency
	// policy, a cancel-on list and a queue, and each would cancel and restart
	// the other's runs. Copying a rule file to try a variant and leaving the
	// name as it was is an easy mistake to make and a near-invisible one to
	// debug, so the second definition is a load error naming both files.
	source := make(map[string]string, len(cfgs))

	for _, c := range cfgs {
		if c.Enabled != nil && !*c.Enabled {
			continue
		}
		if first, taken := source[c.Name]; taken {
			errs = append(errs, fmt.Errorf("rule %q in %s: duplicate name, already defined in %s", c.Name, c.Source, first))
			continue
		}
		r, err := compileOne(c, lookup)
		if err != nil {
			errs = append(errs, fmt.Errorf("rule %q in %s: %w", c.Name, c.Source, err))
			continue
		}
		// Claimed only once the rule really loaded, so a name a broken rule
		// mentioned is still free for a working one.
		source[c.Name] = c.Source
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
	// Presence, not truthiness: repeat = {} decodes to a non-nil, zero-length
	// value, distinguishable from an omitted key by nil-ness alone. A rule
	// author who wrote it meant to use the feature and must see the same
	// rejection as the non-empty form.
	if c.Repeat != nil {
		return nil, fmt.Errorf("repeat is not supported yet")
	}
	if c.Debounce != nil {
		return nil, fmt.Errorf("debounce is not supported yet")
	}

	policy, err := parsePolicy(c.Concurrency)
	if err != nil {
		return nil, err
	}

	r := &Rule{
		Name:        c.Name,
		Source:      c.Source,
		On:          c.On,
		CancelOn:    c.CancelOn,
		Concurrency: policy,
		Steps:       make([]Step, 0, len(c.Steps)),
		state:       lookup,
	}

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
		after, err := parseDuration(sc.After)
		if err != nil {
			return nil, fmt.Errorf("step %d: after: %w", i, err)
		}
		if after < 0 {
			return nil, fmt.Errorf("step %d: after must not be negative", i)
		}
		s := Step{Config: sc, After: after}
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

// parsePolicy turns the concurrency key into a Policy. A misspelling is a
// load error naming every accepted spelling: the alternative is a rule that
// silently gets the default and behaves in a way its author never asked for.
func parsePolicy(s string) (Policy, error) {
	switch Policy(s) {
	case "":
		return PolicyRestart, nil
	case PolicyRestart, PolicyDrop, PolicyQueue:
		return Policy(s), nil
	default:
		return "", fmt.Errorf("concurrency %q is not one of restart, drop, queue", s)
	}
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

// CancelledBy reports whether an event on topic drops this rule's runs. The
// patterns are the same shape as on, so cancel-on takes an exact topic or a
// prefix glob.
func (r *Rule) CancelledBy(topic string) bool {
	for _, pattern := range r.CancelOn {
		if MatchTopic(pattern, topic) {
			return true
		}
	}
	return false
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
